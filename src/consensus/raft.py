"""
Implementasi Raft Consensus Algorithm.

Raft adalah consensus algorithm yang lebih mudah dipahami dibanding Paxos.
Raft membagi consensus problem menjadi 3 subproblem:
1. Leader Election
2. Log Replication  
3. Safety

Reference: https://raft.github.io/
"""

import asyncio
import random
import time
from enum import Enum
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import logging

from ..communication.message_passing import MessagePassing, Message, MessageType

logger = logging.getLogger(__name__)


class NodeState(Enum):
    """
    3 possible states dalam Raft:
    - FOLLOWER: Default state, menerima updates dari leader
    - CANDIDATE: Trying to become leader
    - LEADER: Handle client requests dan replicate logs
    """
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """
    Satu entry dalam Raft log.
    Berisi command yang akan di-apply ke state machine.
    """
    term: int  
    index: int  
    command: Dict[str, Any]  
    
    def __repr__(self):
        return f"LogEntry(term={self.term}, index={self.index})"


@dataclass
class RaftState:
    """
    Persistent state yang harus di-save ke disk.
    Dalam implementasi ini kita simplify dengan in-memory storage.
    """
    # Current term number 
    current_term: int = 0
    
    # Candidate yang di-vote pada current term 
    voted_for: Optional[int] = None
    
    # Log entries
    log: List[LogEntry] = field(default_factory=list)
    
    def get_last_log_index(self) -> int:
        """Get index of last log entry"""
        return len(self.log)
    
    def get_last_log_term(self) -> int:
        """Get term of last log entry"""
        if not self.log:
            return 0
        return self.log[-1].term


class RaftNode:
    """
    Implementasi Raft node.
    
    Raft Protocol Overview:
    1. Semua nodes mulai sebagai FOLLOWER
    2. Jika FOLLOWER tidak receive heartbeat -> jadi CANDIDATE
    3. CANDIDATE request votes dari nodes lain
    4. Jika dapat majority votes -> jadi LEADER
    5. LEADER send heartbeats untuk maintain authority
    """
    
    def __init__(self,
                 node_id: int,
                 peers: List[str],  
                 message_passing: MessagePassing,
                 election_timeout_range: tuple = (150, 300),  
                 heartbeat_interval: int = 50):  
        """
        Args:
            node_id: Unique ID untuk node ini
            peers: List of peer addresses
            message_passing: MessagePassing instance
            election_timeout_range: Random range untuk election timeout
            heartbeat_interval: Interval untuk leader heartbeats
        """
        self.node_id = node_id
        self.peers = peers
        self.mp = message_passing
        
        # Timing configuration (convert ms to seconds)
        self.election_timeout_min = election_timeout_range[0] / 1000
        self.election_timeout_max = election_timeout_range[1] / 1000
        self.heartbeat_interval = heartbeat_interval / 1000
        
        # Current state
        self.state = NodeState.FOLLOWER
        self.raft_state = RaftState()
        
        # Volatile state (reset after restart)
        self.commit_index = 0  
        self.last_applied = 0  
        
        # Leader-only volatile state
        self.next_index: Dict[str, int] = {}  
        self.match_index: Dict[str, int] = {}  
        
        # Current leader (None if unknown)
        self.current_leader: Optional[int] = None
        
        # Election timer
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._get_random_election_timeout()
        
        # Background tasks
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Statistics
        self.elections_started = 0
        self.elections_won = 0
        self.heartbeats_sent = 0
        
        logger.info(f"RaftNode {node_id} initialized with {len(peers)} peers")
    
    def _get_random_election_timeout(self) -> float:
        """
        Get random election timeout.
        Randomization mencegah split votes.
        """
        return random.uniform(self.election_timeout_min, self.election_timeout_max)
    
    async def start(self):
        """Start Raft node"""
        self._running = True
        self._election_task = asyncio.create_task(self._election_timer_loop())
        logger.info(f"RaftNode {self.node_id} started as {self.state.value}")
    
    async def stop(self):
        """Stop Raft node"""
        self._running = False
        
        if self._election_task:
            self._election_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        
        logger.info(f"RaftNode {self.node_id} stopped")
    
    def _become_follower(self, term: int):
        """
        Transition ke FOLLOWER state.
        Dipanggil saat:
        - Node baru start
        - Discover higher term
        - Lost election
        """
        logger.info(f"Node {self.node_id}: {self.state.value} -> FOLLOWER (term {term})")
        
        self.state = NodeState.FOLLOWER
        self.raft_state.current_term = term
        self.raft_state.voted_for = None
        self.current_leader = None
        
        # Cancel heartbeat task jika ada
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        
        # Reset election timer
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._get_random_election_timeout()
    
    def _become_candidate(self):
        """
        Transition ke CANDIDATE state.
        Start election untuk become leader.
        """
        logger.info(f"Node {self.node_id}: {self.state.value} -> CANDIDATE")
        
        self.state = NodeState.CANDIDATE
        self.raft_state.current_term += 1
        self.raft_state.voted_for = self.node_id  
        self.current_leader = None
        self.elections_started += 1
        
        # Reset election timer
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._get_random_election_timeout()
        
        # Start election
        asyncio.create_task(self._start_election())
    
    def _become_leader(self):
        """
        Transition ke LEADER state.
        Start sending heartbeats.
        """
        logger.info(f"Node {self.node_id}: {self.state.value} -> LEADER (term {self.raft_state.current_term})")
        
        self.state = NodeState.LEADER
        self.current_leader = self.node_id
        self.elections_won += 1
        
        # Initialize leader state
        last_log_index = self.raft_state.get_last_log_index()
        for peer in self.peers:
            self.next_index[peer] = last_log_index + 1
            self.match_index[peer] = 0
        
        # Start heartbeat task
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    
    async def _election_timer_loop(self):
        """
        Background loop untuk check election timeout.
        Jika timeout expired -> start election.
        """
        while self._running:
            try:
                await asyncio.sleep(0.01)  
                
                # Only followers and candidates have election timeout
                if self.state in [NodeState.FOLLOWER, NodeState.CANDIDATE]:
                    time_since_heartbeat = time.time() - self.last_heartbeat_time
                    
                    if time_since_heartbeat >= self.election_timeout:
                        logger.debug(f"Node {self.node_id}: Election timeout "
                                   f"({time_since_heartbeat:.3f}s >= {self.election_timeout:.3f}s)")
                        self._become_candidate()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in election timer loop: {e}")
    
    async def _start_election(self):
        """
        Start election process.
        
        Steps:
        1. Increment current term
        2. Vote for self
        3. Send RequestVote RPCs ke semua peers
        4. Jika dapat majority votes -> become leader
        """
        logger.info(f"Node {self.node_id}: Starting election for term {self.raft_state.current_term}")
        
        # Create RequestVote message
        vote_request = Message(
            msg_type=MessageType.REQUEST_VOTE,
            sender_id=self.node_id,
            term=self.raft_state.current_term,
            data={
                'candidate_id': self.node_id,
                'last_log_index': self.raft_state.get_last_log_index(),
                'last_log_term': self.raft_state.get_last_log_term()
            }
        )
        
        # Broadcast ke all peers
        responses = await self.mp.broadcast_message(self.peers, vote_request)
        
        # Count votes 
        votes_received = 1  
        
        for response in responses:
            if response and response.get('vote_granted'):
                votes_received += 1
        
        # Check if won election (majority)
        total_nodes = len(self.peers) + 1  
        majority = (total_nodes // 2) + 1
        
        logger.info(f"Node {self.node_id}: Received {votes_received}/{total_nodes} votes "
                   f"(need {majority} for majority)")
        
        # Check if still candidate (state might have changed)
        if self.state == NodeState.CANDIDATE and votes_received >= majority:
            self._become_leader()
        elif self.state == NodeState.CANDIDATE:
            logger.info(f"Node {self.node_id}: Lost election (not enough votes)")
    
    async def handle_request_vote(self, message: Message) -> Dict[str, Any]:
        """
        Handle RequestVote RPC.
        
        Voter logic:
        1. Reply false jika term < currentTerm
        2. Grant vote jika:
           - Haven't voted yet or already voted for this candidate
           - Candidate's log is at least as up-to-date as receiver's log
        """
        candidate_id = message.data['candidate_id']
        candidate_term = message.term
        last_log_index = message.data['last_log_index']
        last_log_term = message.data['last_log_term']
        
        logger.debug(f"Node {self.node_id}: Received vote request from {candidate_id} "
                    f"(term {candidate_term})")
        
        # If candidate term is higher, update our term
        if candidate_term > self.raft_state.current_term:
            self._become_follower(candidate_term)
        
        vote_granted = False
        
        # Grant vote conditions
        if candidate_term >= self.raft_state.current_term:
            # Haven't voted or already voted for this candidate
            if self.raft_state.voted_for in [None, candidate_id]:
                # Check if candidate's log is up-to-date
                our_last_term = self.raft_state.get_last_log_term()
                our_last_index = self.raft_state.get_last_log_index()
                
                log_is_updated = (
                    last_log_term > our_last_term or
                    (last_log_term == our_last_term and last_log_index >= our_last_index)
                )
                
                if log_is_updated:
                    vote_granted = True
                    self.raft_state.voted_for = candidate_id
                    self.last_heartbeat_time = time.time()  
                    logger.info(f"Node {self.node_id}: Granted vote to {candidate_id}")
        
        return {
            'term': self.raft_state.current_term,
            'vote_granted': vote_granted
        }
    
    async def _heartbeat_loop(self):
        """
        Leader heartbeat loop.
        Sends periodic AppendEntries (empty) untuk maintain authority.
        """
        while self._running and self.state == NodeState.LEADER:
            try:
                await self._send_heartbeats()
                await asyncio.sleep(self.heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
    
    async def _send_heartbeats(self):
        """Send heartbeat (empty AppendEntries) ke all peers"""
        heartbeat = Message(
            msg_type=MessageType.APPEND_ENTRIES,
            sender_id=self.node_id,
            term=self.raft_state.current_term,
            data={
                'leader_id': self.node_id,
                'prev_log_index': self.raft_state.get_last_log_index(),
                'prev_log_term': self.raft_state.get_last_log_term(),
                'entries': [],  # Empty untuk heartbeat
                'leader_commit': self.commit_index
            }
        )
        
        await self.mp.broadcast_message(self.peers, heartbeat)
        self.heartbeats_sent += 1
        
        logger.debug(f"Node {self.node_id}: Sent heartbeat (term {self.raft_state.current_term})")
    
    async def handle_append_entries(self, message: Message) -> Dict[str, Any]:
        """
        Handle AppendEntries RPC (heartbeat or log replication).
        
        Follower logic:
        1. Reply false jika term < currentTerm
        2. Reply false jika log doesn't contain entry at prevLogIndex
        3. If existing entry conflicts, delete it and all following
        4. Append new entries
        5. Update commitIndex
        """
        leader_id = message.data['leader_id']
        leader_term = message.term
        entries = message.data.get('entries', [])
        
        # Update term jika leader term lebih tinggi
        if leader_term > self.raft_state.current_term:
            self._become_follower(leader_term)
        
        success = False
        
        if leader_term >= self.raft_state.current_term:
            # Valid leader, reset election timer
            self.last_heartbeat_time = time.time()
            self.current_leader = leader_id
            
            # Step down if we were candidate/leader
            if self.state != NodeState.FOLLOWER:
                self._become_follower(leader_term)
            
            success = True
            
            # Log replication logic 
            if entries:
                logger.info(f"Node {self.node_id}: Received {len(entries)} log entries from leader")
        
        return {
            'term': self.raft_state.current_term,
            'success': success
        }
    
    def is_leader(self) -> bool:
        """Check if this node is the leader"""
        return self.state == NodeState.LEADER
    
    def get_leader_id(self) -> Optional[int]:
        """Get current leader ID"""
        return self.current_leader
    
    def get_state(self) -> Dict[str, Any]:
        """Get current Raft state untuk debugging/monitoring"""
        return {
            'node_id': self.node_id,
            'state': self.state.value,
            'term': self.raft_state.current_term,
            'leader_id': self.current_leader,
            'log_size': len(self.raft_state.log),
            'commit_index': self.commit_index,
            'elections_started': self.elections_started,
            'elections_won': self.elections_won,
            'heartbeats_sent': self.heartbeats_sent
        }


# Test code
async def test_raft():
    """Test Raft implementation dengan 3 nodes"""
    from ..utils.config import Config
    
    # Setup message passing untuk 3 nodes
    nodes = []
    
    for i in range(1, 4):
        peers = [f"localhost:{5000+j}" for j in range(1, 4) if j != i]
        mp = MessagePassing(node_id=i, host='localhost', port=5000+i)
        await mp.initialize()
        
        raft = RaftNode(
            node_id=i,
            peers=peers,
            message_passing=mp,
            election_timeout_range=(150, 300),
            heartbeat_interval=50
        )
        
        nodes.append(raft)
    
    # Start all nodes
    for node in nodes:
        await node.start()
    
    # Run for 10 seconds
    for i in range(20):
        await asyncio.sleep(0.5)
        print(f"\n--- Second {i*0.5:.1f} ---")
        for node in nodes:
            state = node.get_state()
            print(f"Node {state['node_id']}: {state['state']} (term {state['term']}, "
                  f"leader={state['leader_id']})")
    
    # Stop all nodes
    for node in nodes:
        await node.stop()
        await node.mp.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_raft())
