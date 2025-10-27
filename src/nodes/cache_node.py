"""
Distributed Cache dengan MESI Coherence Protocol.

MESI States:
- Modified (M): Cache line valid, dirty, exclusive
- Exclusive (E): Cache line valid, clean, exclusive
- Shared (S): Cache line valid, clean, may exist in other caches
- Invalid (I): Cache line tidak valid

Features:
- Cache coherence dengan MESI protocol
- LRU cache replacement policy
- Snooping mechanism untuk invalidation
"""

import asyncio
import time
from typing import Dict, Optional, Any, List
from enum import Enum
from collections import OrderedDict
from dataclasses import dataclass
import logging

from .base_node import BaseNode
from ..communication.message_passing import Message, MessageType
from aiohttp import web

logger = logging.getLogger(__name__)


class MESIState(Enum):
    """MESI cache states"""
    MODIFIED = "M"    
    EXCLUSIVE = "E"   
    SHARED = "S"      
    INVALID = "I"     


@dataclass
class CacheLine:
    """Represents a cache line"""
    key: str
    value: Any
    state: MESIState
    last_access: float
    
    def __repr__(self):
        return f"CacheLine({self.key}, state={self.state.value})"


class LRUCache:
    """
    LRU (Least Recently Used) Cache implementation.
    Menggunakan OrderedDict untuk O(1) operations.
    """
    
    def __init__(self, capacity: int):
        """
        Args:
            capacity: Maximum number of entries
        """
        self.capacity = capacity
        self.cache: OrderedDict[str, CacheLine] = OrderedDict()
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
    
    def get(self, key: str) -> Optional[CacheLine]:
        """
        Get cache line.
        Returns None jika miss.
        """
        if key not in self.cache:
            self.misses += 1
            return None
        
        # Move to end (most recently used)
        self.cache.move_to_end(key)
        line = self.cache[key]
        line.last_access = time.time()
        self.hits += 1
        
        return line
    
    def put(self, key: str, value: Any, state: MESIState) -> Optional[str]:
        """
        Put cache line.
        Returns evicted key jika ada eviction.
        """
        evicted_key = None
        
        # Check capacity
        if key not in self.cache and len(self.cache) >= self.capacity:
            # Evict LRU (first item)
            evicted_key, evicted_line = self.cache.popitem(last=False)
            self.evictions += 1
            logger.debug(f"Evicted {evicted_key} (state={evicted_line.state.value})")
        
        # Add/update entry
        if key in self.cache:
            self.cache.move_to_end(key)
        
        self.cache[key] = CacheLine(
            key=key,
            value=value,
            state=state,
            last_access=time.time()
        )
        
        return evicted_key
    
    def update_state(self, key: str, new_state: MESIState) -> bool:
        """
        Update cache line state.
        Returns True jika sukses, False jika key tidak ada.
        """
        if key not in self.cache:
            return False
        
        self.cache[key].state = new_state
        return True
    
    def remove(self, key: str) -> bool:
        """Remove cache line"""
        if key not in self.cache:
            return False
        
        del self.cache[key]
        return True
    
    def get_hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return self.hits / total
    
    def clear(self):
        """Clear all cache entries"""
        self.cache.clear()
    
    def __len__(self):
        return len(self.cache)
    
    def __contains__(self, key):
        return key in self.cache


class CacheNode(BaseNode):
    """
    Distributed Cache Node dengan MESI protocol.
    
    MESI Protocol State Transitions:
    
    Read Miss:
      I -> E (jika exclusive) atau I -> S (jika shared)
    
    Read Hit:
      State tetap sama
    
    Write Hit:
      E -> M, S -> M (broadcast invalidate), M -> M
    
    Write Miss:
      I -> M (broadcast invalidate)
    
    Invalidate:
      M/E/S -> I
    """
    
    def __init__(self, node_id: int, host: str, port: int, peers: List[str], cache_size: int = 1000):
        super().__init__(node_id, host, port, peers)
        
        # LRU Cache
        self.cache = LRUCache(capacity=cache_size)
        
        # Shared cache state tracking (key -> set of node_ids yang have copy)
        self.sharers: Dict[str, set] = {}
        
        # Statistics
        self.reads = 0
        self.writes = 0
        self.invalidations_sent = 0
        self.invalidations_received = 0
        
        # Add custom routes
        self.app.router.add_get('/api/cache/get', self.handle_cache_get)
        self.app.router.add_post('/api/cache/put', self.handle_cache_put)
        self.app.router.add_get('/api/cache/status', self.handle_cache_status)
        
        logger.info(f"CacheNode {node_id} initialized with capacity {cache_size}")
    
    async def _dispatch_message(self, message: Message) -> Dict[str, Any]:
        """Override untuk handle cache messages"""
        
        # Handle cache-specific messages
        if message.msg_type == MessageType.CACHE_INVALIDATE:
            return await self._handle_invalidate(message)
        
        elif message.msg_type == MessageType.CACHE_GET:
            return await self._handle_remote_get(message)
        
        # Fallback ke base class
        return await super()._dispatch_message(message)
    
    async def cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get value dari cache (local atau remote).
        
        MESI Protocol:
        - Local hit: Return value
        - Local miss: Fetch dari peer, transition I -> S atau I -> E
        """
        self.reads += 1
        
        # Try local cache
        line = self.cache.get(key)
        
        if line and line.state != MESIState.INVALID:
            # Cache hit
            logger.debug(f"Cache HIT: {key} (state={line.state.value})")
            return {
                'status': 'hit',
                'key': key,
                'value': line.value,
                'node_id': self.node_id
            }
        
        # Cache miss - try to fetch dari peers
        logger.debug(f"Cache MISS: {key}")
        
        # Broadcast get request ke peers
        get_msg = Message(
            msg_type=MessageType.CACHE_GET,
            sender_id=self.node_id,
            data={'key': key}
        )
        
        responses = await self.mp.broadcast_message(self.peers, get_msg)
        
        # Find peer yang have the data
        for response in responses:
            if response and response.get('found'):
                value = response['value']
                state = MESIState(response['state'])
                
                # Determine local state
                if state == MESIState.MODIFIED or state == MESIState.SHARED:
                    # Other caches have it -> transition to SHARED
                    local_state = MESIState.SHARED
                else:
                    # Exclusive at source -> transition to SHARED (both caches)
                    local_state = MESIState.SHARED
                
                # Add to local cache
                self.cache.put(key, value, local_state)
                
                logger.info(f"Fetched {key} from peer (state={local_state.value})")
                
                return {
                    'status': 'hit',
                    'key': key,
                    'value': value,
                    'node_id': self.node_id,
                    'source': 'remote'
                }
        
        # Not found anywhere
        return {
            'status': 'miss',
            'key': key
        }
    
    async def cache_put(self, key: str, value: Any) -> Dict[str, Any]:
        """
        Put value ke cache.
        
        MESI Protocol:
        - Broadcast invalidate ke all peers
        - Transition local state to MODIFIED
        """
        self.writes += 1
        
        # Check current state
        line = self.cache.get(key)
        
        if line:
            # Cache hit - update
            if line.state == MESIState.SHARED:
                # Need to invalidate other copies
                await self._broadcast_invalidate(key)
            
            # Update to MODIFIED
            line.value = value
            line.state = MESIState.MODIFIED
            line.last_access = time.time()
            
            logger.info(f"Updated {key} (state=MODIFIED)")
        else:
            await self._broadcast_invalidate(key)
            
            # Insert as MODIFIED
            evicted = self.cache.put(key, value, MESIState.MODIFIED)
            
            if evicted:
                logger.debug(f"Evicted {evicted} to make room for {key}")
            
            logger.info(f"Inserted {key} (state=MODIFIED)")
        
        return {
            'status': 'success',
            'key': key,
            'node_id': self.node_id
        }
    
    async def _broadcast_invalidate(self, key: str):
        """
        Broadcast invalidate message ke all peers.
        """
        invalidate_msg = Message(
            msg_type=MessageType.CACHE_INVALIDATE,
            sender_id=self.node_id,
            data={'key': key}
        )
        
        await self.mp.broadcast_message(self.peers, invalidate_msg)
        self.invalidations_sent += 1
        
        logger.debug(f"Broadcast invalidate for {key}")
    
    async def _handle_invalidate(self, message: Message) -> Dict[str, Any]:
        """
        Handle invalidate message dari peer.
        
        Transition: M/E/S -> I
        """
        key = message.data['key']
        
        if key in self.cache:
            # Invalidate cache line
            self.cache.update_state(key, MESIState.INVALID)
            self.invalidations_received += 1
            
            logger.info(f"Invalidated {key} (from node {message.sender_id})")
        
        return {'status': 'ok'}
    
    async def _handle_remote_get(self, message: Message) -> Dict[str, Any]:
        """
        Handle get request dari peer.
        
        MESI Protocol:
        - Jika state = EXCLUSIVE -> transition to SHARED
        - Jika state = MODIFIED -> write back (simplified: just share)
        """
        key = message.data['key']
        
        line = self.cache.get(key)
        
        if not line or line.state == MESIState.INVALID:
            return {'found': False}
        
        # Transition state untuk sharing
        if line.state == MESIState.EXCLUSIVE:
            line.state = MESIState.SHARED
            logger.debug(f"{key}: E -> S (shared with node {message.sender_id})")
        
        elif line.state == MESIState.MODIFIED:
            line.state = MESIState.SHARED
            logger.debug(f"{key}: M -> S (shared with node {message.sender_id})")
        
        return {
            'found': True,
            'key': key,
            'value': line.value,
            'state': line.state.value
        }
    
    async def handle_cache_get(self, request: web.Request) -> web.Response:
        """HTTP endpoint untuk cache get"""
        try:
            key = request.query.get('key')
            if not key:
                return web.json_response({'error': 'key required'}, status=400)
            
            result = await self.cache_get(key)
            return web.json_response(result)
            
        except Exception as e:
            logger.error(f"Error in cache_get: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_cache_put(self, request: web.Request) -> web.Response:
        """HTTP endpoint untuk cache put"""
        try:
            data = await request.json()
            key = data['key']
            value = data['value']
            
            result = await self.cache_put(key, value)
            return web.json_response(result)
            
        except Exception as e:
            logger.error(f"Error in cache_put: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_cache_status(self, request: web.Request) -> web.Response:
        """Get cache status dan statistics"""
        # Collect cache entries
        entries = []
        for key, line in self.cache.cache.items():
            entries.append({
                'key': key,
                'state': line.state.value,
                'age': time.time() - line.last_access
            })
        
        status = {
            'node_id': self.node_id,
            'cache_size': len(self.cache),
            'capacity': self.cache.capacity,
            'hit_rate': self.cache.get_hit_rate(),
            'statistics': {
                'reads': self.reads,
                'writes': self.writes,
                'hits': self.cache.hits,
                'misses': self.cache.misses,
                'evictions': self.cache.evictions,
                'invalidations_sent': self.invalidations_sent,
                'invalidations_received': self.invalidations_received
            },
            'entries': entries[:10] 
        }
        return web.json_response(status)


# Test code
async def test_cache_node():
    """Test CacheNode dengan MESI protocol"""
    # Create cache nodes
    nodes = []
    
    for i in range(1, 4):
        peers = [f"localhost:{8000+j}" for j in range(1, 4) if j != i]
        node = CacheNode(
            node_id=i,
            host='localhost',
            port=8000 + i,
            peers=peers,
            cache_size=100
        )
        nodes.append(node)
    
    # Start all nodes
    for node in nodes:
        await node.start()
    
    print("Cache nodes started")
    await asyncio.sleep(1)
    
    # Test MESI protocol
    print("\n=== Testing MESI Protocol ===")
    
    # Test 1: Node 1 writes (will be MODIFIED)
    print("\n1. Node 1 writes 'key1' = 'value1'")
    result = await nodes[0].cache_put('key1', 'value1')
    print(f"   Result: {result}")
    print(f"   Node 1 cache state: {nodes[0].cache.cache.get('key1').state.value if 'key1' in nodes[0].cache else 'N/A'}")
    
    await asyncio.sleep(0.5)
    
    # Test 2: Node 2 reads (
    print("\n2. Node 2 reads 'key1'")
    result = await nodes[1].cache_get('key1')
    print(f"   Result: {result}")
    print(f"   Node 1 state: {nodes[0].cache.cache.get('key1').state.value if 'key1' in nodes[0].cache else 'N/A'}")
    print(f"   Node 2 state: {nodes[1].cache.cache.get('key1').state.value if 'key1' in nodes[1].cache else 'N/A'}")
    
    await asyncio.sleep(0.5)
    
    # Test 3: Node 3 writes (will invalidate Node 1 and Node 2)
    print("\n3. Node 3 writes 'key1' = 'value1_updated'")
    result = await nodes[2].cache_put('key1', 'value1_updated')
    print(f"   Result: {result}")
    
    await asyncio.sleep(0.5)
    
    print(f"   Node 1 state: {nodes[0].cache.cache.get('key1').state.value if 'key1' in nodes[0].cache else 'N/A'}")
    print(f"   Node 2 state: {nodes[1].cache.cache.get('key1').state.value if 'key1' in nodes[1].cache else 'N/A'}")
    print(f"   Node 3 state: {nodes[2].cache.cache.get('key1').state.value if 'key1' in nodes[2].cache else 'N/A'}")
    
    # Test 4: Multiple operations
    print("\n4. Multiple operations")
    await nodes[0].cache_put('key2', 'value2')
    await nodes[0].cache_put('key3', 'value3')
    await nodes[1].cache_get('key2')
    await nodes[2].cache_get('key3')
    
    # Print statistics
    print("\n=== Cache Statistics ===")
    for node in nodes:
        stats = {
            'node_id': node.node_id,
            'cache_size': len(node.cache),
            'hit_rate': f"{node.cache.get_hit_rate()*100:.1f}%",
            'reads': node.reads,
            'writes': node.writes,
            'invalidations': f"sent={node.invalidations_sent}, recv={node.invalidations_received}"
        }
        print(f"\nNode {stats['node_id']}:")
        for k, v in stats.items():
            if k != 'node_id':
                print(f"  {k}: {v}")
    
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
    asyncio.run(test_cache_node())
