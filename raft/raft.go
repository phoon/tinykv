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
	"errors"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
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

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// self-defining for randomized election timeout
	rand *rand.Rand
	// this can reduce conflicts down to <= 0.3,
	// an old implementation which defined the isElectionTimeout() func instead
	// of randomizedElectionTimeout, is <= 0.4, which is not satisfy our tests
	randomizedElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	r := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		rand:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	for _, p := range c.peers {
		r.Prs[p] = &Progress{Next: 1}
	}

	// starts as follower
	r.becomeFollower(r.Term, None)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + r.rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	r.reset(r.Term + 1)
	r.State = StateCandidate
	// vote for self
	r.Vote = r.id
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + r.rand.Intn(r.electionTimeout)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// no-op
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// trigger r itself to start a new election
	if m.MsgType == pb.MessageType_MsgHup {
		if r.State != StateLeader {
			r.compaign()
			return nil
		}
	}

	switch {
	case m.Term == 0:
		// local message
	case m.Term < r.Term:
		// ignore
		return nil
	case m.Term > r.Term:
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}

		r.becomeFollower(m.Term, lead)
	}

	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// self-defining functions
func (r *Raft) send(m pb.Message) {
	m.From = r.id

	// not local message
	if m.MsgType != pb.MessageType_MsgPropose {
		m.Term = r.Term
	}
	// push to mailbox
	r.msgs = append(r.msgs, m)
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.sendAppend(id)
	}
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.sendHeartbeat(id)
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.pastElectionTimeout() {
		r.electionElapsed = 0

		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
		})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0

		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
		})
	}

}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		// since r.Vote is stored for current term
		r.Vote = None
	}

	r.Lead = None
	r.votes = make(map[uint64]bool)
}

func (r *Raft) compaign() {
	r.becomeCandidate()
	// update votemap, votes for self
	r.poll(r.id, true)
	if r.granted() == r.quorum() {
		r.becomeLeader()
		return
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
		})
	}
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) poll(id uint64, v bool) {
	// vote only once
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
}

func (r *Raft) granted() (granted int) {
	for _, v := range r.votes {
		if v {
			granted++
		}
	}

	return granted
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		// redirect to leader
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.becomeFollower(r.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		// since we already vote for self
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			Reject:  true,
		})
	case pb.MessageType_MsgRequestVoteResponse:
		// push to poll(update votesmap)
		r.poll(m.From, !m.Reject)
		granted := r.granted()

		// once we get quorum votes, we become a leader
		switch r.quorum() {
		case granted:
			r.becomeLeader()
			r.bcastHeartbeat()
			// or we get quorum rejections first
		case len(r.votes) - granted:
			r.becomeFollower(r.Term, None)
		}
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgRequestVote:
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			Reject:  true,
		})
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.Vote == None || r.Vote == m.From {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
		})
		r.Vote = m.From
	} else {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			Reject:  true,
		})
	}
}
