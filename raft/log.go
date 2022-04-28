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
	"fmt"
	"log"

	pb "github.com/coreos/etcd/raft/raftpb"
)

type raftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// unstable contains all unstable entries and snapshot.
	// they will be saved into storage.
	unstable unstable

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64	// commitIndex

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64	// applyIndex

	logger Logger
}

// newLog returns log using the given storage. It recovers the log to the state
// that it just commits and applies the latest snapshot.
func newLog(storage Storage, logger Logger) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &raftLog{
		storage: storage,
		logger:  logger,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	// init unstable state
	log.unstable.offset = lastIndex + 1
	log.unstable.logger = logger
	// Initialize our committed and applied pointers to the time of the last compaction.
	// init commitIndex & applyIndex
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
// index、logTerm：分别为论文中的prevIndex，prevTerm，用于日志匹配；committed：leader上的commitIndex
// ents：需要存储到 raftLog 中的 log entries
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		// 1. 先进行 日志匹配 检查

		lastnewi = index + uint64(len(ents))
		// 2. 获取从 ents 的哪个位置开始写到 raftlog
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
			// 2-1. 没有冲突，忽略
		case ci <= l.committed:
			// 2-2. 需覆盖的位置在 commitedIndex 之前，panic
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			// 2-3. ci > 0, 且合法，进行切片
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		// 3. update commitIndex
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

// 将 log entries 写入 unstable 中
func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	// 如果索引小于committed，则说明该数据是非法的
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	// 放入unstable中存储
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
// 返回 raftlog 中与 ents 日志匹配不通过的index，如果没有则返回 ents 中与 raftLog 没有覆盖的第一条 entry 的 index
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		// 找到第一个 日志匹配 不通过的 entry
		if !l.matchTerm(ne.Index, ne.Term) {
			// matchTerm 为false 有两种情况，一种是日志匹配不通过，另一种是已经到了 raftLog 中 entries 的尽头
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

// unstableEntries 返回unstable中存储的 entries
func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
// nextEnts 返回commit但是还没有apply的 entries
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, noLimit)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
// hasNextEnts 判断是否有 commit 但是还没有 apply 的 entries
func (l *raftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

// snapshot 返回快照数据
func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		// 如果没有保存的数据有快照，就返回
		return *l.unstable.snapshot, nil
	}
	// 否则返回持久化存储的快照数据
	return l.storage.Snapshot()
}

// firstIndex 返回 most recent snapshot 的下一条 log entry 的 index
func (l *raftLog) firstIndex() uint64 {
	// 首先尝试在未持久化数据中看有没有快照数据
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	// 否则才返回持久化数据的firstIndex
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

// lastIndex 返回 raftLog 中存储的entries中的lastIndex
func (l *raftLog) lastIndex() uint64 {
	// 如果有未持久化的日志，返回未持久化日志的最后索引
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	// 否则返回持久化日志的最后索引
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

// commitTo 将raftlog的commitIndex，修改为tocommit
func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			// 传入的值如果比lastIndex大则是非法的
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
		l.logger.Infof("commit to %d", tocommit)
	}
}

// appliedTo 修改 appliedIndex 为 i
func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	// 判断合法性
	// 新的applied ID既不能比committed大，也不能比当前的applied索引小
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

// stableTo 传入 index、term 将 unstable 的entries 缩容到相应位置
func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

// stableSnapTo 传入 index，将 unstable 中对应的 snapshot 丢弃
func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

// lastTerm 返回最后一个索引的term
func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// term 返回 index=i 的 log entry 对应term
func (l *raftLog) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.firstIndex() - 1
	// 1. 先判断 i 范围是否正确，越界和已在snapshot中将检索不到
	if i < dummyIndex || i > l.lastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}

	// 2. 尝试从unstable中查询term
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	// 3. 尝试从storage中查询term
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err) // TODO(bdarnell)
}

// entries 返回从i开始的entries，大小不超过maxsize
func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	// TODO (xiangli): handle error?
	panic(err)
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
// 判断是否比当前节点的日志更新：1）term是否更大 2）term相同的情况下，索引是否更大
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

// matchTerm 判断索引 preIndex 和 preTeam 是否和 raftLog 一致，实现 raft 论文中的 Log Matching Property
func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	// 只有在传入的index大于当前commit索引，以及maxIndex对应的term与传入的term匹配时，才使用这些数据进行commit
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// restore 往 unstable 写入 snapshot
func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
// 返回[lo,hi-1]之间的数据，这些数据的条目数不超过maxSize
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	// 1. check
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}

	// 2. 读取 entries
	var ents []pb.Entry
	if lo < l.unstable.offset {
		// 2-1. lo 小于unstable的offset，说明前半部分在持久化的storage中

		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}

	if hi > l.unstable.offset {
		// 2-2. hi大于unstable offset，说明后半部分在unstable中取得
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			ents = append([]pb.Entry{}, ents...)
			ents = append(ents, unstable...)
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// 判断传入的lo，hi是否超过范围了
// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

// 如果传入的err是nil，则返回t；如果是ErrCompacted则返回0，其他情况都panic
func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
