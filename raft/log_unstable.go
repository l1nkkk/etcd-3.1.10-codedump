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

import pb "github.com/coreos/etcd/raft/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.

// unstable用来保存还未持久化的数据，其可能保存在两个地方：
// 快照数据snapshot或者entry数组，两者中同时只可能存在其中之一；
// 1. 快照数据用于开始启动时需要恢复的数据较多，所以一次性的使用快照数据来恢复；
// 2. entry数组则是用于逐条数据进行接收时使用；其中offset与entry数组配合着使用，
// 可能会出现小于持久化最大索引偏移量的数据，所以需要做截断处理
type unstable struct {
	// the incoming unstable snapshot, if any.
	// 未持久化的 snapshot
	snapshot *pb.Snapshot

	// all entries that have not yet been written to storage.
	// 未持久化的log entries
	entries []pb.Entry

	// 保存entries数组中的数据的起始index
	offset uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
// 当 unstable 存储 snapshot 时，返回期待的下一条 log entries 的 index (即lastIndex+1)。
// 否则返回 {0,false}
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
// 返回 unstable 中，最后日志数据的index(先找entries，若没有再找snapshot)
// unstable 没有数据返回 {0,false}
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
// 返回 index=i 对应的term，如果不存在则返回{0,false}
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot == nil {
			return 0, false
		}
		if u.snapshot.Metadata.Index == i {
			// 刚好 snapshot metadata 中记录了该 index 对应的 term
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	// else, i >= u.offset
	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	// 如果比lastindex还大，那也查不到
	if i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

// （entries）通知 unstable 缩容，告知已经将index=i，及其之前的log entries持久化
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= u.offset {
		u.entries = u.entries[i+1-u.offset:]
		u.logger.Infof("stable to %d, entries size: %d", i, len(u.entries))
		u.offset = i + 1
	}
}

// （snapshot）通知 unstable 缩容，告知已经将 lastindex=i 的 snapshot 持久化
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

// （write snapshot）往 unstable 中写入 snapshot
func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

// (write entries) 往 unstable 中写入entries，可能覆盖原有的数据
func (u *unstable) truncateAndAppend(ents []pb.Entry) {

	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// 1. 如果正好是紧接着当前数据的，就直接append
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		// 2. 如果比当前偏移量小，全部覆盖
		u.offset = after
		u.entries = ents
	default:
		// truncate to after and copy to u.entries
		// then append
		// 3.部分覆盖， u.offset < after < u.offset+uint64(len(u.entries))
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

// 返回索引范围在[lo-u.offset : hi-u.offset]之间的数据
func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.offset)
// 检查传入的索引范围是否合法，不合法直接panic
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
