"""
Integration tests untuk Distributed Lock Manager.
"""

import pytest
import asyncio
from src.nodes.lock_manager import LockManager, LockType


@pytest.mark.asyncio
async def test_distributed_lock_basic():
    """Test basic distributed lock operations"""
    # Create 3 lock managers
    nodes = []
    
    for i in range(1, 4):
        peers = [f"localhost:{9000+j}" for j in range(1, 4) if j != i]
        node = LockManager(
            node_id=i,
            host='localhost',
            port=9000 + i,
            peers=peers
        )
        nodes.append(node)
    
    # Start all nodes
    for node in nodes:
        await node.start()
    
    # Wait for leader election
    await asyncio.sleep(2)
    
    # Find leader
    leader = None
    for node in nodes:
        if node.is_leader():
            leader = node
            break
    
    assert leader is not None, "No leader elected"
    
    # Test exclusive lock
    result = await leader.acquire_lock('resource1', LockType.EXCLUSIVE, 1)
    assert result['status'] == 'acquired'
    
    # Try to acquire same lock (should wait)
    result = await leader.acquire_lock('resource1', LockType.EXCLUSIVE, 2)
    assert result['status'] == 'waiting'
    
    # Release lock
    result = await leader.release_lock('resource1', 1)
    assert result['status'] == 'released'
    
    # Stop all nodes
    for node in nodes:
        await node.stop()


@pytest.mark.asyncio
async def test_shared_locks():
    """Test shared locks (multiple readers)"""
    nodes = []
    
    for i in range(1, 4):
        peers = [f"localhost:{9100+j}" for j in range(1, 4) if j != i]
        node = LockManager(
            node_id=i,
            host='localhost',
            port=9100 + i,
            peers=peers
        )
        nodes.append(node)
    
    for node in nodes:
        await node.start()
    
    await asyncio.sleep(2)
    
    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None
    
    # Multiple shared locks should succeed
    result1 = await leader.acquire_lock('resource2', LockType.SHARED, 1)
    result2 = await leader.acquire_lock('resource2', LockType.SHARED, 2)
    
    assert result1['status'] == 'acquired'
    assert result2['status'] == 'acquired'
    
    # Exclusive lock should wait
    result3 = await leader.acquire_lock('resource2', LockType.EXCLUSIVE, 3)
    assert result3['status'] == 'waiting'
    
    for node in nodes:
        await node.stop()


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
