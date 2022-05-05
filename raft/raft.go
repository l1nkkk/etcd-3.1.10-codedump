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

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/coreos/etcd/raft/raftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	// 由于leader转让发起的竞选
	campaignTransfer CampaignType = "CampaignTransfer"
)

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization. Only the methods needed by the code are exposed
// (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex // l1nkkk: 为什么需要锁
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// 保存集群所有节点ID的数组（包括自身）
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// 选举超时tick
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// 心跳超时tick
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	// MaxSizePerMsg limits the max size of each append message. Smaller value
	// lowers the raft recovery cost(initial probing and message lost during normal
	// operation). On the other side, it might affect the throughput during normal
	// replication. Note: math.MaxUint64 for unlimited, 0 for at most one entry per
	// message.
	MaxSizePerMsg uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	// 标记leader是否需要检查集群中超过半数节点的活跃性，如果在选举超时内没有满足该条件，leader切换到follower状态
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	return nil
}

type raft struct {
	id          uint64
	Term        uint64
	Vote        uint64      // voteFor
	readStates  []ReadState // relate to read-only
	raftLog     *raftLog    // log data manager
	maxInflight int         // 滑动窗口阈值
	maxMsgSize  uint64
	prs         map[uint64]*Progress // 节点同步进度
	state       StateType       // 当前节点在集群中的角色
	votes       map[uint64]bool // Candidate 时有用。存储当前收到的投票情况

	msgs []pb.Message
	lead uint64 // the leader id
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	leadTransferee uint64 // 禅让对象id

	// New configuration is ignored if there exists unapplied configuration.
	pendingConf bool // 标识当前还有未applied的 config entrie

	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	checkQuorum bool
	preVote     bool

	heartbeatTimeout int
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	// tick函数，在到期的时候调用，不同的角色该函数不同
	tick func()
	step stepFunc

	logger Logger
}

func newRaft(c *Config) *raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage, c.Logger)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers) and ConfState.Nodes)")
		}
		peers = cs.Nodes
	}
	r := &raft{
		id:               c.ID,
		lead:             None,
		raftLog:          raftlog,
		maxMsgSize:       c.MaxSizePerMsg,
		maxInflight:      c.MaxInflightMsgs,
		prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		logger:           c.Logger,
		checkQuorum:      c.CheckQuorum,
		preVote:          c.PreVote,
		readOnly:         newReadOnly(c.ReadOnlyOption),
	}
	for _, p := range peers {
		r.prs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight)}
	}
	// 如果不是第一次启动而是从之前的数据进行恢复
	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	// 启动都是follower状态
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

// hasLeader 当前 raft 状态中是否存在 leader，存在返回 true
func (r *raft) hasLeader() bool { return r.lead != None }

// softState 返回当前 raft 的 SoftState，内容包括：leaderID，RaftState
func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

// hardState 返回当前 raft 的 HardState，内容包括：Term，VoteFor，commitIndex
func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// quorum 返回当前集群的 quorum 值，即多数派需达到多少
func (r *raft) quorum() int { return len(r.prs)/2 + 1 }

// nodes 返回排序之后的节点ID数组
func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.prs))
	for id := range r.prs {
		nodes = append(nodes, id)
	}
	// 注意这里进行了排序
	sort.Sort(uint64Slice(nodes))
	return nodes
}

// send persists state to stable storage and then sends to its mailbox.
// send 将 msg 进行信息完善(填入from id，term)和check之后，放入 msg slice r.msgs 中
func (r *raft) send(m pb.Message) {
	m.From = r.id
	if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
		// 投票时term不能为空
		if m.Term == 0 {
			// PreVote RPCs are sent at a term other than our actual term, so the code
			// that sends these messages is responsible for setting the term.
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		// 其他的消息类型，term必须为空
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		// prop消息和readindex消息不需要带上term参数，因为此类 msg 将转发给 leader
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	// 注意这里只是添加到msgs中
	r.msgs = append(r.msgs, m)
}

/// 以下sendxxx方法，都是在构造 msg 之后，调用 send 方法

// sendAppend sends RPC, with entries to the given peer.
// sendAppend 向to节点发送 日志数据，可能是 shapshot（MsgSnap），也可能是 entries（MsgApp）
func (r *raft) sendAppend(to uint64) {
	pr := r.prs[to]

	// 1. 判断该节点是否处于Pause状态，如果是，则直接return
	if pr.IsPaused() {
		r.logger.Infof("node %d paused", to)
		return
	}
	m := pb.Message{}
	m.To = to

	// 2. get prevLogTerm and entries which want to send
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	// 3. send snapshot if we failed to get term or entries
	if errt != nil || erre != nil {

		// 3-1. 如果该节点当前不可用，直接return
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return
		}

		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return
			}
			panic(err) // TODO(bdarnell)
		}
		// 3-2 检查待发送的 snapshot 是否为空，为空直接 panic
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}

		// 3-3 构造 MsgSnap
		m.Type = pb.MsgSnap
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)

		// 3-4. 更新该节点的 progress，将其设置为 ProgressStateSnapshot 状态
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		// 4. send entries if successful to get term and entries
		// 4-1. 构造 pb.MsgApp
		m.Type = pb.MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.raftLog.committed

		if n := len(m.Entries); n != 0 {
			switch pr.State {
			// optimistically increase the next when in ProgressStateReplicate
			case ProgressStateReplicate:
				// 4-2. 如果是ProgressStateReplicate状态，更新 process
				last := m.Entries[n-1].Index
				pr.optimisticUpdate(last)
				pr.ins.add(last)	// l1nkkk: 滑动窗口update
			case ProgressStateProbe:
				// 4-3 如果是ProgressStateProbe状态，pr.pause()
				pr.pause()
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	r.send(m)
}

// sendHeartbeat sends an empty MsgApp
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	// l1nkkk: 这里发送的 leaderCommitIndex 有讲究，与上面的 sendAppend 中的不一样，
	// 不能直接是 r.raftLog.committed
	commit := min(r.prs[to].Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
// bcastAppend 向所有follower调用 sendAppend
func (r *raft) bcastAppend() {
	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// bcastHeartbeat sends RPC, without entries to all the peers.
// bcastHeartbeat 向所有follower发送心跳
func (r *raft) bcastHeartbeat() {
	// l1nkkk: read-only
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id, ctx)
	}
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
// maybeCommit 尝试commit当前的日志，如果commitIndex发生变化了就返回true
func (r *raft) maybeCommit() bool {
	// TODO(bmizerany): optimize.. Currently naive
	mis := make(uint64Slice, 0, len(r.prs))
	for id := range r.prs {
		mis = append(mis, r.prs[id].Match)
	}

	// 1. 获取 matchIndex 中位数
	sort.Sort(sort.Reverse(mis)) // 逆序排列
	mci := mis[r.quorum()-1]

	// 2. update raftLog
	return r.raftLog.maybeCommit(mci, r.Term)
}

// reset 重置raft的状态，这些状态包括 当前term、当前leader、选举超时、心跳超时、选举超时阈值、中断leader禅让
// 集群中每个节点的 process、pendingConf、readOnly
func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}

	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	// 重置选举超时阈值
	r.resetRandomizedElectionTimeout()
	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	for id := range r.prs {
		r.prs[id] = &Progress{Next: r.raftLog.lastIndex() + 1, ins: newInflights(r.maxInflight)}
		if id == r.id {
			r.prs[id].Match = r.raftLog.lastIndex()
		}
	}
	r.pendingConf = false
	r.readOnly = newReadOnly(r.readOnly.option)
}

// appendEntry 用于leader，往 raftLog 中添加 entries
func (r *raft) appendEntry(es ...pb.Entry) {
	// 1. append entries to raftLog
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.raftLog.append(es...)

	// 2. update raftLog
	r.prs[r.id].maybeUpdate(r.raftLog.lastIndex())

	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	// 3. try to update commitIndex
	r.maybeCommit()
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *raft) tickElection() {
	// 计时器++
	r.electionElapsed++

	// 如果节点在当前配置中，同时选举时间也到了，则发送 MsgHup 消息，触发选举
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	// 1. 心跳和超时计数器++
	r.heartbeatElapsed++
	r.electionElapsed++

	// 2. 如果选举超时，通过向本节点发送MsgCheckQuorum，检查集群中节点的可用性
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0	// reset
		if r.checkQuorum {
			r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum})
		}
		// 2-1 如果当前处于leader禅让阶段，则终止leader禅让
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader {
		// 不是leader的就不用往下走了
		return
	}

	// 3. 如果心跳超时，向本节点发送 MsgBeat
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0 // reset
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
	}
}

// becomeFollower 将 raft 的身份状态设置为 Follower
func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate 将 raft 的身份状态设置为 StateCandidate
func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id // 给自己投票
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomePreCandidate 将 raft 的身份状态设置为 StatePreCandidate
func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	// l1nkkk: prevote不会递增term，也不会先进行投票，而是等prevote结果出来再进行决定
	r.step = stepCandidate
	r.tick = r.tickElection
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

// becomeLeader 将 raft 的身份状态设置为 StateLeader
func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	ents, err := r.raftLog.entries(r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}

	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {
		r.pendingConf = true
	}

	// l1nkkk: 读一致性要求，以及让前面的可以提交
	r.appendEntry(pb.Entry{Data: nil})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *raft) campaign(t CampaignType) {
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}
	// 1. 调用poll函数给自己投票，同时返回当前投票给本节点的节点数量
	if r.quorum() == r.poll(r.id, voteRespMsgType(voteMsg), true) {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		// l1nkkk: 单节点情况
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}

	// 2. 向集群里的其他节点发送投票消息
	for id := range r.prs {
		if id == r.id {
			// 过滤掉自己
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
	}
}

// poll 将 <id,v> 记录到 raft 中，并返回投票给该节点的数量
func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	// 1. 如果id没有投票过，那么更新id的投票情况
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	// 2. 计算下都有多少节点已经投票给自己了
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

// Step raft 的状态机
func (r *raft) Step(m pb.Message) error {
	// l1nkkk: 该函数处理各个状态通用的一些处理，另外一些特定的处理交个 r.step()
	r.logger.Infof("from:%d, to:%d, type:%s, term:%d, state:%v", m.From, m.To, m.Type, r.Term, r.state)

	// Handle the message term, which may result in our stepping down to a follower.
	// 1. 根据 msg's term 的情况，分别做处理
	switch {
	case m.Term == 0:
		// 1-1. local message
	case m.Term > r.Term:
		// 1-2.
		lead := m.From
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			// 1-2-1. 如果收到的是投票类消息，判断是否将当前 leader 设置为 None

			// a. 判断是否是领导者禅让
			force := bytes.Equal(m.Context, []byte(campaignTransfer))

			// l1nkkk: important
			// b. 判断是否在租约期间
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// 如果非节点禅让，而且又在租约期以内，就不做任何处理,
				// 租约相关内容，见论文的4.2.3，这是为了阻止已经离开集群的节点再次发起投票请求
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}
			lead = None // l1nkkk: MsgPreVote 也要将 leader 设置为None,个人觉得其实没必要
		}

		switch {

		// 1-2-2. 是否更新当前的 term，并设置身份状态为 follower
		case m.Type == pb.MsgPreVote:
			// a. 在应答一个 MsgPreVote msg 时不对其修改
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// b. 在应答一个 MsgPreVoteResp msg 以及 其没被拒绝 时不对其修改
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			// c. 其他情况修改 term
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			// 变成follower状态
			r.becomeFollower(m.Term, lead)
		}

	case m.Term < r.Term:
		// 1-3. m.Term < r.Term 处理完直接 return
			// 1-3-1. 当 msg 是 MsgHeartbeat 或者 MsgApp 时，直接响应 MsgAppResp 消息
		if r.checkQuorum && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases
			// 收到了一个节点发送过来的更小的term消息。这种情况可能是因为消息的网络延时导致，
			// 但是也可能因为该节点由于网络分区导致了它递增了term到一个新的任期。
			// 这种情况下该节点不能赢得一次选举，也不能使用旧的任期号重新再加入集群中。
			// 如果checkQurom为false，这种情况可以使用递增任期号应答来处理。但是如果checkQurom为True，
			// 此时收到了一个更小的term的节点发出的HB或者APP消息，于是应答一个appresp消息，试图纠正它的状态
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else {
			// ignore other cases
			// 1-3-2. 除了上面的情况以外，忽略任何term小于当前节点所在任期号的消息
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		// 注意：直接 return
		return nil
	}

	// 2. 根据 msg Type 类型( MsgHup/MsgVote/MsgPreVote )分别做处理
	switch m.Type {
	case pb.MsgHup:
		// 2-1 收到 MsgHup，说明准备进行选举
		if r.state != StateLeader {
			// 取出[applied+1,committed+1]之间的消息，即得到还未进行applied的日志列表
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			// 核心：如果有 config entry 没有被 apply，则不能进行选举
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)

			if r.preVote {
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}
		} else {
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MsgVote, pb.MsgPreVote:
		// 2-2 收到 MsgVote 和 MsgPreVote
		// The m.Term > r.Term clause is for MsgPreVote. For MsgVote m.Term should
		// always equal r.Term.
		if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			// 如果当前没有给任何节点投票（r.Vote == None）
			// 或者投票的节点term大于本节点的（m.Term > r.Term），注：经过前面的处理，不可能出现这种情况
			// 或者是之前已经投票的节点（r.Vote == m.From）
			// 同时 candidate 的最新entry比当前节点新或者一样，
			// 那么就投票给该节点
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				// 如果是 MsgVote 则更新状态
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			// 否则拒绝投票
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		// 2-3 其他类型的msg处理，进入各种身份状态下定制的状态机函数
		r.step(r, m)
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message)

// leader的状态机
func stepLeader(r *raft, m pb.Message) {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		// 1. 广播心跳包
		r.bcastHeartbeat()
		return
	case pb.MsgCheckQuorum:
		// 1. 检查集群可用性，判断集群中Process处于active状态的节点是否大于
		if !r.checkQuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		return
	case pb.MsgProp:
		// 1. check
		// 1-1. entries 不能为空
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		// 1-2. (important)检查是否在集群中，这种情况出现在当前leader被移出配置，但是leader仍临时管理该集群的情况
		if _, ok := r.prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return
		}
		// 1-3 当前是否在进行 leader 禅让，如果是，不能处理 propose
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return
		}

		// 2. 遍历 entries，处理其中的 config entry
		for i, e := range m.Entries {
			if e.Type == pb.EntryConfChange {
				if r.pendingConf {
					// 2-1. 如果当前有未 apply 的 config entry，则忽略该 entry
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration", e.String())
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				}
				r.pendingConf = true
			}
		}
		// 3. 将 entries app 到 raftLog
		r.appendEntry(m.Entries...)
		// 4. 向集群其他节点广播 MsgApp
		r.bcastAppend()
		return
	case pb.MsgReadIndex:
		if r.quorum() > 1 {
			// 这个表达式用于判断是否commttied log索引对应的term是否与当前term不相等，
			// 如果不相等说明这是一个新的leader，而在它当选之后还没有提交任何数据
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				// Reject read only request when this leader has not committed any log entry at its term.
				return
			}

			// thinking: use an interally defined context instead of the user given context.
			// We can express this in terms of the term and index instead of a user-supplied value.
			// This would allow multiple reads to piggyback on the same message.
			switch r.readOnly.option {
			case ReadOnlySafe:
				// 把读请求到来时的committed索引保存下来
				r.readOnly.addRequest(r.raftLog.committed, m)
				// 广播消息出去，其中消息的CTX是该读请求的唯一标识
				// 在应答是Context要原样返回，将使用这个ctx操作readOnly相关数据
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				var ri uint64
				if r.checkQuorum {
					ri = r.raftLog.committed
				}
				if m.From == None || m.From == r.id { // from local member
					r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}
			}
		} else {
			r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
		}

		return
	}

	// All other message types require a progress for m.From (pr).
	// 检查消息发送者当前是否在集群中
	pr, prOk := r.prs[m.From]
	if !prOk {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return
	}
	switch m.Type {
	case pb.MsgAppResp:
		// 1. set active for this node
		pr.RecentActive = true

		// 2. Reject 为true表明日志匹配不通过
		if m.Reject {
			r.logger.Debugf("%x received msgApp rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)

			// rejecthint 为拒绝该 msgApp 的节点的 lastindex，Index 用于日志匹配，其也可以用于标识发送给该节点的 msg
			// 2-1. 尝试下调该节点对应 Process 的 Next
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == ProgressStateReplicate {
					// 2-2 becomeProbe
					pr.becomeProbe()
				}
				// 2-3. 再次发送 msgApp
				r.sendAppend(m.From)
			}
		} else {
			// 3. msgApp 被正常处理
			// 此时的 m.Index 可能是对应follower的commitIndex(当msgApp携带的entries中存在follower
			// 已commit的数据)或者其lastIndex
			oldPaused := pr.IsPaused()
			if pr.maybeUpdate(m.Index) {
				// 3-1 如果收到的 msg resp 不是过期的
				switch {
				case pr.State == ProgressStateProbe:
					// a. 当前为 ProgressStateProbe，则切换为 Replicate 状态
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					// b. 当前为 ProgressStateSnapshot，则切换为 Probe 状态
					r.logger.Debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					pr.becomeProbe()
				case pr.State == ProgressStateReplicate:
					// c. 当前为 ProgressStateReplicate 状态，则释放窗口
					pr.ins.freeTo(m.Index)
				}
				// 3-2 尝试 commit entry，如果成功，则广播发送 msgApp
				if r.maybeCommit() {
					r.bcastAppend()
				} else if oldPaused {
					// update() reset the wait state on this node. If we had delayed sending
					// an update before, send it now.
					// l1nkkk: 如果该节点之前状态是暂停，继续发送append消息给它，感觉多余了
					r.sendAppend(m.From)
				}
				// Transfer leadership is in progress.
				// 3-3 如果在leader禅让的过程中，并且被禅让节点的日志已经和当前leader一样新，则发送 MsgTimeoutNow
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		// 1. set active for this node
		pr.RecentActive = true
		// 这里调用resume是因为当前可能处于probe状态，而这个状态在两个heartbeat消息的间隔期只能收一条同步日志消息，因此在收到HB消息时就停止pause标记
		pr.resume()

		// free one slot for the full inflights window to allow progress.
		if pr.State == ProgressStateReplicate && pr.ins.full() {
			pr.ins.freeFirstOne()
		}
		// 该节点的match节点小于当前最大日志索引，可能已经过期了，尝试添加日志
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		// 只有readonly safe方案，才会继续往下走
		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return
		}

		// 收到应答调用recvAck函数返回当前针对该消息已经应答的节点数量
		ackCount := r.readOnly.recvAck(m)
		if ackCount < r.quorum() {
			// 小于集群半数以上就返回不往下走了
			return
		}

		// 调用advance函数尝试丢弃已经被确认的read index状态
		rss := r.readOnly.advance(m)
		for _, rs := range rss { // 遍历准备被丢弃的readindex状态
			req := rs.req
			if req.From == None || req.From == r.id { // from local member
				// 如果来自本地
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				// 否则就是来自外部，需要应答
				r.send(pb.Message{To: req.From, Type: pb.MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case pb.MsgSnapStatus:
		// 当前该节点状态已经不是在接受快照的状态了，直接返回
		if pr.State != ProgressStateSnapshot {
			return
		}
		if !m.Reject {
			// 接收快照成功
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// 接收快照失败
			pr.snapshotFailure()
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// If snapshot finish, wait for the msgAppResp from the remote node before sending
		// out the next msgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		// 先暂停等待下一次被唤醒
		pr.pause()
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.State == ProgressStateReplicate {
			pr.becomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			// 判断是否已经有相同节点的leader转让流程在进行中
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				// 如果是，直接返回
				return
			}
			// 否则中断之前的转让流程
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		// 判断是否转让过来的leader是否本节点，如果是也直接返回，因为本节点已经是leader了
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			// 如果日志已经匹配了，那么就发送timeoutnow协议过去
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			// 否则继续追加日志
			r.sendAppend(leadTransferee)
		}
	}
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
func stepCandidate(r *raft, m pb.Message) {
	// 进入到该函数，已经在前面 Step 中把保证 m.term >= r.term

	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}


	switch m.Type {
	case pb.MsgProp: // client propose
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return
	case pb.MsgApp:
		// 收到append消息，说明集群中已经有leader，转换为follower
		r.becomeFollower(r.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		// 收到HB消息，说明集群已经有leader，转换为follower
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		// 收到快照消息，说明集群已经有leader，转换为follower
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteRespType:
		// l1nkkk: 核心
		// 1. 计算当前集群中有多少节点给自己投了赞成票
		gr := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.Type, len(r.votes)-gr)
		switch r.quorum() {
		case gr: // a.赞成票数
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				// 变成leader，并广播心跳
				r.becomeLeader()
				r.bcastAppend()
			}
		case len(r.votes) - gr: // b.反对票数
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow: // 用于 leader 禅让
		// 收到 MsgTimeoutNow 是不处理
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
}


func stepFollower(r *raft, m pb.Message) {
	switch m.Type {
	case pb.MsgProp: // client propose
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		}
		// 重定向
		m.To = r.lead
		r.send(m)
	case pb.MsgApp:
		// 收到leader的 MsgApp，重置超时计时
		r.electionElapsed = 0
		r.lead = m.From		// 似乎略显多余
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		// 收到leader的 MsgApp，重置超时计时
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		// 收到leader的 MsgSnap，重置超时计时
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		// 收到 client 要求的领导权禅让，转发给 leader
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return
		}
		// 重定向
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		// l1nkkk: 领导权禅让核心
		// 收到 leader 发来的 MsgTimeoutNow，表明 leader 已经将 entries 同步完毕，该节点可以接任了
		if r.promotable() {
			// 如果本节点可以提升为leader，那么就发起新一轮的竞选
			r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			// Leadership transfers never use pre-vote even if r.preVote is true; we
			// know we are not recovering from a partition so there is no need for the
			// extra round trip.
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
		}
	case pb.MsgReadIndex:
		// read-only: 收到 MsgReadIndex，转发给 leader
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return
		}
		// 向leader转发此类型消息
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp:
		// read-only:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return
		}
		// 更新 readstates 数组
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
}

// handleAppendEntries 处理MsgApp，添加日志
func (r *raft) handleAppendEntries(m pb.Message) {
	// 1. 传入的消息索引是已经commit过，通过 msg 的 Index 字段告知当前 commitIndex
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}
	r.logger.Infof("%x -> %x index %d", m.From, r.id, m.Index)

	// 2. 尝试添加到 raftLog 中
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		// 2-1 添加成功，通过 msg 的 Index 字段告知当前的 raftLog lastIndex
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		// 2-2 添加失败，将 msg 的 Index 字段设置为 m.Index，
		// Reject 字段设置为true，RejectHint 字段设置为 lastIndex
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

// handleHeartbeat 处理 Heartbeat msg
func (r *raft) handleHeartbeat(m pb.Message) {
	r.raftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

// handleSnapshot 处理 msgSnap
func (r *raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	// 注意这里成功与失败，只是返回的Index参数不同
	if r.restore(m.Snapshot) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine.
// restore 使用快照数据进行恢复
func (r *raft) restore(s pb.Snapshot) bool {
	// 1. check：判断快照数据是否在当前节点已经commit
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}

	// 2. 如果当前的raftLog中的entries有部分条目被覆盖，提交到快照所在的lastIndex
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.raftLog.commitTo(s.Metadata.Index)
		// 为什么这里返回false？
		return false
	}

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)

	// 3. 将此快照
	r.raftLog.restore(s)
	r.prs = make(map[uint64]*Progress)
	// 包括集群中其他节点的状态也使用快照中的状态数据进行恢复
	for _, n := range s.Metadata.ConfState.Nodes {
		match, next := uint64(0), r.raftLog.lastIndex()+1
		if n == r.id {
			match = next - 1
		}
		r.setProgress(n, match, next)
		r.logger.Infof("%x restored progress of %x [%s]", r.id, n, r.prs[n])
	}
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
// promotable 返回是否可以被提升为leader，实际上是简单的判断该节点是否在raft记录的 processes 中
func (r *raft) promotable() bool {
	// 在prs数组中找到本节点，说明该节点在集群中，这就具备了可以被选为leader的条件
	_, ok := r.prs[r.id]
	return ok
}

// addNode 增加一个节点
func (r *raft) addNode(id uint64) {
	// 重置pengdingConf标志位
	r.pendingConf = false
	// 检查是否已经存在节点列表中
	if _, ok := r.prs[id]; ok {
		// Ignore any redundant addNode calls (which can happen because the
		// initial bootstrapping entries are applied twice).
		return
	}

	// 这里才真的添加进来
	r.setProgress(id, 0, r.raftLog.lastIndex()+1)
}

// removeNode 删除一个节点
func (r *raft) removeNode(id uint64) {
	r.delProgress(id)
	// 重置 pengding Conf 标志位
	r.pendingConf = false

	// do not try to commit or abort transferring if there is no nodes in the cluster.
	if len(r.prs) == 0 {
		return
	}

	// The quorum size is now smaller, so see if any pending entries can
	// be committed.
	// 由于删除了节点，所以半数节点的数量变少了，于是去查看是否有可以认为提交成功的数据
	if r.maybeCommit() {
		r.bcastAppend()
	}
	// If the removed node is the leadTransferee, then abort the leadership transferring.
	if r.state == StateLeader && r.leadTransferee == id {
		// 如果在leader迁移过程中发生了删除节点的操作，那么中断迁移leader流程
		r.abortLeaderTransfer()
	}
}

// resetPendingConf r.pendingConf = false
func (r *raft) resetPendingConf() { r.pendingConf = false }

// setProgress 往 raft 中添加节点progress
func (r *raft) setProgress(id, match, next uint64) {
	r.prs[id] = &Progress{Next: next, Match: match, ins: newInflights(r.maxInflight)}
}

// delProgress 往 raft 中删除节点progress
func (r *raft) delProgress(id uint64) {
	delete(r.prs, id)
}

// loadState 从 state 中加载 当前任期、投票对象、commitIndex
func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
// pastElectionTimeout 判断是否选举超时
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

// resetRandomizedElectionTimeout 重置选举超时阈值
func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// checkQuorumActive returns true if the quorum is active from
// the view of the local raft state machine. Otherwise, it returns
// false.
// checkQuorumActive also resets all RecentActive to false.
// checkQuorumActive 检查集群的可用性，当小于半数的节点不可用时返回false
func (r *raft) checkQuorumActive() bool {
	var act int

	for id := range r.prs {
		if id == r.id { // self is always active
			act++
			continue
		}

		if r.prs[id].RecentActive {
			act++
		}
		// l1nkkk: 为什么设置为 false，因为下次再调用 checkQuorumActive，已经是leader又一次选举超时了
		// 这个时候还是 false 表明在该超时间隔中，没有收到任何该节点的响应
		r.prs[id].RecentActive = false
	}

	return act >= r.quorum()
}

// sendTimeoutNow 向 to 节点发送 MsgTimeoutNow
func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

// abortLeaderTransfer 中断leader禅让，简单将 r.leadTransferee 设置为None
func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}


// numOfPendingConf 返回 ents 中，conf entry 的数量
func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange {
			n++
		}
	}
	return n
}
