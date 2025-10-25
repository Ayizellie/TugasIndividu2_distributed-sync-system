"""
Distributed Lock Manager.
Implementasi distributed locks dengan:
- Shared locks (multiple readers)
- Exclusive locks (single writer)
- Deadlock detection
- Raft consensus untuk consistency
"""

import asyncio
import time
from typing import Dict, Set, Optional, List
from enum import Enum
from dataclasses import dataclass
import logging

from .base_node import BaseNode
from ..communication.message_passing import Message, MessageType
from aiohttp import web

logger = logging.getLogger(__name__)


class LockType(Enum):
    """Tipe locks"""
    SHARED = "shared"      # Multiple readers, no writers
    EXCLUSIVE = "exclusive"  # Single writer, no readers/writers


@dataclass
class Lock:
    """Represents a lock on a resource"""
    resource_id: str
    lock_type: LockType
    owner_ids: Set[int]  # Node IDs yang hold lock (multiple untuk shared)
    acquired_time: float
    
    def __repr__(self):
        return f"Lock({self.resource_id}, {self.lock_type.value}, owners={self.owner_ids})"


class DeadlockDetector:
    """
    Deadlock detector menggunakan wait-for graph.
    
    Deadlock terjadi jika ada cycle dalam wait-for graph.
    Contoh: Node A waits for B, B waits for A = deadlock
    """
    
    def __init__(self):
        # Wait-for graph: node_id -> set of node_ids it's waiting for
        self.wait_for: Dict[int, Set[int]] = {}
    
    def add_wait(self, waiter: int, holder: int):
        """Add wait edge: waiter is waiting for holder"""
        if waiter not in self.wait_for:
            self.wait_for[waiter] = set()
        self.wait_for[waiter].add(holder)
    
    def remove_wait(self, waiter: int):
        """Remove all wait edges for waiter"""
        if waiter in self.wait_for:
            del self.wait_for[waiter]
    
    def detect_deadlock(self, start_node: int) -> Optional[List[int]]:
        """
        Detect cycle dalam wait-for graph starting from start_node.
        
        Returns:
            List of nodes in cycle jika detected, None otherwise
        """
        visited = set()
        path = []
        
        def dfs(node: int) -> Optional[List[int]]:
            if node in path:
                # Found cycle
                cycle_start = path.index(node)
                return path[cycle_start:]
            
            if node in visited:
                return None
            
            visited.add(node)
            path.append(node)
            
            # Check nodes this node is waiting for
            if node in self.wait_for:
                for next_node in self.wait_for[node]:
                    cycle = dfs(next_node)
                    if cycle:
                        return cycle
            
            path.pop()
            return None
        
        return dfs(start_node)
    
    def has_deadlock(self) -> bool:
        """Check if any deadlock exists"""
        for node in self.wait_for.keys():
            if self.detect_deadlock(node):
                return True
        return False


class LockManager(BaseNode):
    """
    Distributed Lock Manager Node.
    
    Features:
    - Shared locks (multiple readers)
    - Exclusive locks (single writer)
    - Deadlock detection
    - Automatic lock timeout
    """
    
    def __init__(self, node_id: int, host: str, port: int, peers: List[str]):
        super().__init__(node_id, host, port, peers)
        
        # Lock storage: resource_id -> Lock
        self.locks: Dict[str, Lock] = {}
        
        # Wait queue: resource_id -> list of (node_id, lock_type, timestamp)
        self.wait_queue: Dict[str, List[tuple]] = {}
        
        # Deadlock detector
        self.deadlock_detector = DeadlockDetector()
        
        # Lock timeout (seconds)
        self.lock_timeout = 60.0
        
        # Statistics
        self.locks_acquired = 0
        self.locks_released = 0
        self.lock_timeouts = 0
        self.deadlocks_detected = 0
        
        # Add custom routes
        self.app.router.add_post('/api/lock/acquire', self.handle_acquire_lock)
        self.app.router.add_post('/api/lock/release', self.handle_release_lock)
        self.app.router.add_get('/api/lock/status', self.handle_lock_status)
        
        # Background cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        
        logger.info(f"LockManager {node_id} initialized")
    
    async def start(self):
        """Start lock manager"""
        await super().start()
        
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        logger.info(f"LockManager {self.node_id} started")
    
    async def stop(self):
        """Stop lock manager"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        await super().stop()
    
    async def _cleanup_loop(self):
        """Background task untuk cleanup expired locks"""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                await self._cleanup_expired_locks()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
    
    async def _cleanup_expired_locks(self):
        """Remove locks yang sudah timeout"""
        current_time = time.time()
        expired = []
        
        for resource_id, lock in self.locks.items():
            if current_time - lock.acquired_time > self.lock_timeout:
                expired.append(resource_id)
        
        for resource_id in expired:
            logger.warning(f"Lock on {resource_id} expired, releasing")
            del self.locks[resource_id]
            self.lock_timeouts += 1
            
            # Process wait queue
            await self._process_wait_queue(resource_id)
    
    def _can_acquire_lock(self, resource_id: str, lock_type: LockType, requester_id: int) -> bool:
        """
        Check apakah lock bisa di-acquire.
        
        Rules:
        - SHARED lock: OK jika no exclusive locks
        - EXCLUSIVE lock: OK jika no locks at all
        """
        if resource_id not in self.locks:
            return True
        
        existing_lock = self.locks[resource_id]
        
        if lock_type == LockType.SHARED:
            # Shared lock OK jika existing lock juga shared
            return existing_lock.lock_type == LockType.SHARED
        else:
            # Exclusive lock requires no existing locks
            return False
    
    async def acquire_lock(self, resource_id: str, lock_type: LockType, requester_id: int) -> Dict[str, any]:
        """
        Acquire lock pada resource.
        
        Returns:
            Dict dengan status: 'acquired', 'waiting', atau 'denied'
        """
        # Only leader can grant locks
        if not self.is_leader():
            leader_id = self.get_leader_id()
            return {
                'status': 'denied',
                'reason': 'not_leader',
                'leader_id': leader_id
            }
        
        # Check deadlock
        if resource_id in self.locks:
            for holder_id in self.locks[resource_id].owner_ids:
                self.deadlock_detector.add_wait(requester_id, holder_id)
            
            if self.deadlock_detector.detect_deadlock(requester_id):
                self.deadlocks_detected += 1
                self.deadlock_detector.remove_wait(requester_id)
                logger.warning(f"Deadlock detected for node {requester_id} on {resource_id}")
                return {
                    'status': 'denied',
                    'reason': 'deadlock_detected'
                }
        
        # Try to acquire
        if self._can_acquire_lock(resource_id, lock_type, requester_id):
            # Acquire lock
            if resource_id in self.locks and lock_type == LockType.SHARED:
                # Add to existing shared lock
                self.locks[resource_id].owner_ids.add(requester_id)
            else:
                # Create new lock
                self.locks[resource_id] = Lock(
                    resource_id=resource_id,
                    lock_type=lock_type,
                    owner_ids={requester_id},
                    acquired_time=time.time()
                )
            
            self.locks_acquired += 1
            self.deadlock_detector.remove_wait(requester_id)
            
            logger.info(f"Node {requester_id} acquired {lock_type.value} lock on {resource_id}")
            
            return {
                'status': 'acquired',
                'resource_id': resource_id,
                'lock_type': lock_type.value
            }
        else:
            # Add to wait queue
            if resource_id not in self.wait_queue:
                self.wait_queue[resource_id] = []
            
            self.wait_queue[resource_id].append((requester_id, lock_type, time.time()))
            
            logger.info(f"Node {requester_id} waiting for {lock_type.value} lock on {resource_id}")
            
            return {
                'status': 'waiting',
                'resource_id': resource_id,
                'queue_position': len(self.wait_queue[resource_id])
            }
    
    async def release_lock(self, resource_id: str, releaser_id: int) -> Dict[str, any]:
        """Release lock pada resource"""
        if not self.is_leader():
            return {
                'status': 'denied',
                'reason': 'not_leader'
            }
        
        if resource_id not in self.locks:
            return {
                'status': 'error',
                'reason': 'lock_not_found'
            }
        
        lock = self.locks[resource_id]
        
        if releaser_id not in lock.owner_ids:
            return {
                'status': 'error',
                'reason': 'not_owner'
            }
        
        # Remove owner
        lock.owner_ids.remove(releaser_id)
        self.locks_released += 1
        self.deadlock_detector.remove_wait(releaser_id)
        
        logger.info(f"Node {releaser_id} released lock on {resource_id}")
        
        # If no more owners, delete lock
        if not lock.owner_ids:
            del self.locks[resource_id]
            
            # Process wait queue
            await self._process_wait_queue(resource_id)
        
        return {
            'status': 'released',
            'resource_id': resource_id
        }
    
    async def _process_wait_queue(self, resource_id: str):
        """Process wait queue setelah lock released"""
        if resource_id not in self.wait_queue:
            return
        
        queue = self.wait_queue[resource_id]
        if not queue:
            return
        
        # Try to grant locks dari wait queue
        granted = []
        
        for i, (node_id, lock_type, timestamp) in enumerate(queue):
            result = await self.acquire_lock(resource_id, lock_type, node_id)
            
            if result['status'] == 'acquired':
                granted.append(i)
                logger.info(f"Granted waiting lock to node {node_id}")
            elif lock_type == LockType.EXCLUSIVE:
                # Exclusive lock blocks semua following requests
                break
        
        # Remove granted requests dari queue
        for i in reversed(granted):
            queue.pop(i)
        
        if not queue:
            del self.wait_queue[resource_id]
    
    async def handle_acquire_lock(self, request: web.Request) -> web.Response:
        """HTTP endpoint untuk acquire lock"""
        try:
            data = await request.json()
            resource_id = data['resource_id']
            lock_type = LockType(data['lock_type'])
            requester_id = data['requester_id']
            
            result = await self.acquire_lock(resource_id, lock_type, requester_id)
            return web.json_response(result)
            
        except Exception as e:
            logger.error(f"Error in acquire_lock: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_release_lock(self, request: web.Request) -> web.Response:
        """HTTP endpoint untuk release lock"""
        try:
            data = await request.json()
            resource_id = data['resource_id']
            releaser_id = data['releaser_id']
            
            result = await self.release_lock(resource_id, releaser_id)
            return web.json_response(result)
            
        except Exception as e:
            logger.error(f"Error in release_lock: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_lock_status(self, request: web.Request) -> web.Response:
        """Get status semua locks"""
        status = {
            'node_id': self.node_id,
            'is_leader': self.is_leader(),
            'active_locks': len(self.locks),
            'waiting_requests': sum(len(q) for q in self.wait_queue.values()),
            'locks': {
                rid: {
                    'type': lock.lock_type.value,
                    'owners': list(lock.owner_ids),
                    'age': time.time() - lock.acquired_time
                }
                for rid, lock in self.locks.items()
            },
            'statistics': {
                'locks_acquired': self.locks_acquired,
                'locks_released': self.locks_released,
                'lock_timeouts': self.lock_timeouts,
                'deadlocks_detected': self.deadlocks_detected
            }
        }
        return web.json_response(status)


# Test code
async def test_lock_manager():
    """Test LockManager"""
    # Create 3 lock manager nodes
    nodes = []
    
    for i in range(1, 4):
        peers = [f"localhost:{6000+j}" for j in range(1, 4) if j != i]
        node = LockManager(
            node_id=i,
            host='localhost',
            port=6000 + i,
            peers=peers
        )
        nodes.append(node)
    
    # Start all nodes
    for node in nodes:
        await node.start()
    
    print("Waiting for leader election...")
    await asyncio.sleep(2)
    
    # Find leader
    leader = None
    for node in nodes:
        if node.is_leader():
            leader = node
            break
    
    if not leader:
        print("No leader elected!")
        return
    
    print(f"\nLeader is Node {leader.node_id}")
    
    # Test locks
    print("\n=== Testing Locks ===")
    
    # Test 1: Acquire exclusive lock
    print("\n1. Node 1 acquires EXCLUSIVE lock on 'resource_A'")
    result = await leader.acquire_lock('resource_A', LockType.EXCLUSIVE, 1)
    print(f"   Result: {result}")
    
    # Test 2: Try to acquire same resource (should wait)
    print("\n2. Node 2 tries to acquire EXCLUSIVE lock on 'resource_A'")
    result = await leader.acquire_lock('resource_A', LockType.EXCLUSIVE, 2)
    print(f"   Result: {result}")
    
    # Test 3: Release lock
    print("\n3. Node 1 releases lock on 'resource_A'")
    result = await leader.release_lock('resource_A', 1)
    print(f"   Result: {result}")
    
    await asyncio.sleep(0.5)
    
    # Test 4: Multiple shared locks
    print("\n4. Node 1 acquires SHARED lock on 'resource_B'")
    result = await leader.acquire_lock('resource_B', LockType.SHARED, 1)
    print(f"   Result: {result}")
    
    print("\n5. Node 2 acquires SHARED lock on 'resource_B'")
    result = await leader.acquire_lock('resource_B', LockType.SHARED, 2)
    print(f"   Result: {result}")
    
    print("\n6. Node 3 tries to acquire EXCLUSIVE lock on 'resource_B'")
    result = await leader.acquire_lock('resource_B', LockType.EXCLUSIVE, 3)
    print(f"   Result: {result}")
    
    # Print status
    print("\n=== Lock Status ===")
    print(f"Active locks: {len(leader.locks)}")
    for rid, lock in leader.locks.items():
        print(f"  {rid}: {lock}")
    
    # Keep running
    print("\nPress Ctrl+C to stop...")
    try:
        await asyncio.sleep(30)
    except KeyboardInterrupt:
        pass
    
    # Stop nodes
    for node in nodes:
        await node.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(test_lock_manager())
