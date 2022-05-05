// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import "fmt"


// 在probe状态下，leader每次心跳间隔只能向这个节点发送一条app信息，在应答这条app信息之前，不能向这个节点继续发送其他app信息。
// 在网络不可用，或者follower拒绝了某条app信息之后，都会切换到这个状态
// 这个状态也是节点的初始状态

const (
	ProgressStateProbe ProgressStateType = iota
	ProgressStateReplicate
	ProgressStateSnapshot
)

type ProgressStateType uint64

var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}

func (st ProgressStateType) String() string { return prstmap[uint64(st)] }


// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// 正常情况下，Next = Match + 1
	// 以下情况下不是上面这种情况：
	// 1. 切换到Probe状态时，如果上一个状态是Snapshot状态，即正在接收快照，那么Next = max(pr.Match+1, pendingSnapshot+1)
	// 2. 当该follower不在Replicate状态时，说明不是正常的接收副本状态。
	//    此时当leader与follower同步leader上的日志时，可能出现覆盖的情况，即此时follower上面假设Match为3，但是索引为3的数据会被
	//    leader覆盖，此时Next指针可能会一直回溯到与leader上日志匹配的位置，再开始正常同步日志，此时也会出现Next != Match + 1的情况出现
	Match, Next uint64

	// State defines how the leader should interact with the follower.
	//
	// When in ProgressStateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in ProgressStateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in ProgressStateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	//
	State ProgressStateType

	// Paused is used in ProgressStateProbe.
	// When Paused is true, raft should pause sending replication message to this peer.
	Paused bool

	// PendingSnapshot is used in ProgressStateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	// 如果向该节点发送快照消息，PendingSnapshot用于保存快照消息的lastIndex，
	// 当PendingSnapshot不为0时，该节点也被标记为暂停状态（即IsPaused返回true，此时不能进行 entries 的同步）。
	// raft只有在这个正在进行中的快照同步失败以后，才会重传快照消息
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	// RecentActive 最近收到该节点的响应msg，则设置为true
	RecentActive bool

	// inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.freeTo with the index of the last
	// received entry.
	ins *inflights
}

// resetState 重置状态
func (pr *Progress) resetState(state ProgressStateType) {
	pr.Paused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.ins.reset()
}

// becomeProbe 修改为probe状态
func (pr *Progress) becomeProbe() {
	// If the original state is ProgressStateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == ProgressStateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.resetState(ProgressStateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

// becomeReplicate 修改为 Replicate 状态
func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

// becomeSnapshot 修改为 Snapshot 状态，并设置 PendingSnapshot
func (pr *Progress) becomeSnapshot(snapshoti uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
// maybeUpdate 用于更新 Match 和 Next，如果n和n+1分别大于 Process 中的 Match 和 Next，则更新；
// 如果 Match 被更新，则返回True，并将 Pause 设置为 false; 如果返回fasle，表明传入的 n(m.Index) 是过期的
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.resume()
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}

func (pr *Progress) optimisticUpdate(n uint64) { pr.Next = n + 1 }

// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
// maybeDecrTo 用于缩小Process的Next 值，如果收到的 rejected 是过期的，直接返回 false；
// rejected 表示被拒绝 msgApp 的Index，last 表示拒绝 msgApp 的节点的 lastIndex(this is a hint);
func (pr *Progress) maybeDecrTo(rejected, last uint64) bool {

	// 1. 如果该节点的Process处于ProgressStateReplicate状态，特点：pr.Next = pr.Match + 1
	if pr.State == ProgressStateReplicate {
		// the rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {// pr.Next = pr.Match + 1，所以可以用 pr.Match 来判断
			// 1-1. 收到的 rejected 已经过期，因为 match 的增加只有在对方成功接收后才增加
			return false
		}
		// 1-2. directly set next to match + 1，感觉略显多余
		pr.Next = pr.Match + 1
		return true
	}

	// 2. 如果节点的Process不处于ProgressStateReplicate状态

	// the rejection must be stale if "rejected" does not match next - 1
	// 2-1. 收到的 rejected 已经过期，return false
	if pr.Next-1 != rejected { // 注意：与 rejected 判断的是 pr.Next-1，而不是 pr.Match
		return false
	}

	// 2-2. pr.Next = min(pr.Next-1, last+1)
	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}
	// 2-3. Pause set false
	pr.resume()
	return true
}

func (pr *Progress) pause()  { pr.Paused = true }
func (pr *Progress) resume() { pr.Paused = false }

// IsPaused returns whether sending log entries to this node has been
// paused. A node may be paused because it has rejected recent
// MsgApps, is currently waiting for a snapshot, or has reached the
// MaxInflightMsgs limit.
// IsPaused 判断是否需要对该节点停止发送 entries，如果需要返回 true
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:
		// 1. 处于 ProgressStateProbe 状态，并且 pr.Paused 为 true
		return pr.Paused
	case ProgressStateReplicate:
		// 2. 滑动窗口已满：处于 ProgressStateReplicate 状态，并且 pr.ins.full() 为true
		return pr.ins.full()
	case ProgressStateSnapshot:
		// 3. 正在发送snapshot：处于ProgressStateSnapshot 状态
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) snapshotFailure() { pr.PendingSnapshot = 0 }

// needSnapshotAbort returns true if snapshot progress's Match
// is equal or higher than the pendingSnapshot.
// 可以中断快照的情况：当前为ProgressStateSnapshot状态，同时match已经大于等于快照索引
// 因为match已经大于快照索引了，所以这部分快照数据可以不接收了，也就是可以被中断的快照操作
// 因为在节点落后leader数据很多的情况下，可能leader会多次通过snapshot同步数据给节点，
// 而当 pr.Match >= pr.PendingSnapshot的时候，说明通过快照来同步数据的流程完成了，这时可以进入正常的接收同步数据状态了。
func (pr *Progress) needSnapshotAbort() bool {
	return pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot
}

func (pr *Progress) String() string {
	return fmt.Sprintf("next = %d, match = %d, state = %s, waiting = %v, pendingSnapshot = %d", pr.Next, pr.Match, pr.State, pr.IsPaused(), pr.PendingSnapshot)
}

// inflights 用于表示 commitIndex 和 applyIndex 之间的滑动窗口

type inflights struct {
	// the starting index in the buffer
	start int

	// number of inflights in the buffer
	count int

	// the size of the buffer
	size int

	// buffer contains the index of the last entry
	// inside one message.
	buffer []uint64
}

func newInflights(size int) *inflights {
	return &inflights{
		size: size,
	}
}

// add adds an inflight into inflights
func (in *inflights) add(inflight uint64) {
	if in.full() {
		panic("cannot add into a full inflights")
	}
	next := in.start + in.count
	size := in.size

	// 1. 索引如果超过size了，要回绕回来
	if next >= size {
		next -= size
	}

	// 2. 如果空间不足，需要扩容
	if next >= len(in.buffer) {
		in.growBuf()
	}

	// 3. 将 inflight 加入 inflights
	in.buffer[next] = inflight
	in.count++
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
func (in *inflights) growBuf() {
	// 注：并不是一开始就分配 size 个单位的空间，而是逐渐扩大分配的内存空间
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// freeTo frees the inflights smaller or equal to the given `to` flight.
func (in *inflights) freeTo(to uint64) {

	// 1. 窗口为空，或者传入的参数小于窗口第一个元素的值，直接return
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}
	i, idx := 0, in.start

	// 2. 寻找比to大的最小的值，找到了就退出循环
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // found the first large inflight
			break
		}
		// increase index and maybe rotate，处理需要回绕的情况
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	// 3. 数量减少 小于to的个数
	in.count -= i
	// 4. 设置start为第一个大于to的元素开始
	in.start = idx
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

func (in *inflights) freeFirstOne() { in.freeTo(in.buffer[in.start]) }

// full returns true if the inflights is full.
func (in *inflights) full() bool {
	return in.count == in.size
}

// resets frees all inflights.
func (in *inflights) reset() {
	in.count = 0
	in.start = 0
}
