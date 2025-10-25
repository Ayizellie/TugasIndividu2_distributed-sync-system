"""
Unit tests untuk Raft consensus algorithm.
"""

import pytest
import asyncio
from src.consensus.raft import RaftNode, NodeState, LogEntry, RaftState
from src.communication.message_passing import MessagePassing


@pytest.mark.asyncio
async def test_raft_initialization():
    """Test Raft node initialization"""
    mp = MessagePassing(node_id=1, host='localhost', port=5000)
    await mp.initialize()
    
    raft = RaftNode(
        node_id=1,
        peers=['localhost:5001', 'localhost:5002'],
        message_passing=mp
    )
    
    assert raft.node_id == 1
    assert raft.state == NodeState.FOLLOWER
    assert raft.raft_state.current_term == 0
    assert raft.raft_state.voted_for is None
    
    await mp.close()


@pytest.mark.asyncio
async def test_raft_state_transitions():
    """Test Raft state transitions"""
    mp = MessagePassing(node_id=1, host='localhost', port=5000)
    await mp.initialize()
    
    raft = RaftNode(
        node_id=1,
        peers=['localhost:5001'],
        message_passing=mp
    )
    
    # Test FOLLOWER -> CANDIDATE
    initial_term = raft.raft_state.current_term
    raft._become_candidate()
    
    assert raft.state == NodeState.CANDIDATE
    assert raft.raft_state.current_term == initial_term + 1
    assert raft.raft_state.voted_for == 1
    
    # Test CANDIDATE -> LEADER
    raft._become_leader()
    
    assert raft.state == NodeState.LEADER
    assert raft.current_leader == 1
    
    # Test any -> FOLLOWER
    raft._become_follower(10)
    
    assert raft.state == NodeState.FOLLOWER
    assert raft.raft_state.current_term == 10
    assert raft.raft_state.voted_for is None
    
    await mp.close()


@pytest.mark.asyncio
async def test_log_entry():
    """Test LogEntry creation"""
    entry = LogEntry(
        term=1,
        index=1,
        command={'op': 'set', 'key': 'x', 'value': 10}
    )
    
    assert entry.term == 1
    assert entry.index == 1
    assert entry.command['op'] == 'set'


def test_raft_state():
    """Test RaftState"""
    state = RaftState()
    
    assert state.current_term == 0
    assert state.voted_for is None
    assert len(state.log) == 0
    
    # Add log entries
    state.log.append(LogEntry(term=1, index=1, command={}))
    state.log.append(LogEntry(term=1, index=2, command={}))
    
    assert state.get_last_log_index() == 2
    assert state.get_last_log_term() == 1


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
