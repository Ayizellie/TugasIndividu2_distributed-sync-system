"""
Distributed Queue System.
Features:
- Consistent hashing untuk distribusi
- Multiple producers dan consumers
- Message persistence dengan Redis
- At-least-once delivery guarantee
- Node failure recovery
"""

import asyncio
import hashlib
import json
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging

import redis.asyncio as aioredis

from .base_node import BaseNode
from ..communication.message_passing import Message, MessageType
from aiohttp import web

logger = logging.getLogger(__name__)


@dataclass
class QueueMessage:
    """Message dalam queue"""
    message_id: str
    data: Dict[str, Any]
    timestamp: float
    retry_count: int = 0
    status: str = 'pending'  # pending, processing, completed, failed
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'QueueMessage':
        return cls(**data)


class ConsistentHashRing:
    """
    Consistent Hash Ring implementation.
    
    Consistent hashing memastikan:
    - Data terdistribusi merata
    - Minimal data movement saat node ditambah/dihapus
    """
    
    def __init__(self, nodes: List[str] = None, virtual_nodes: int = 150):
        """
        Args:
            nodes: List of node identifiers
            virtual_nodes: Jumlah virtual nodes per physical node
        """
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}  # hash -> node_id
        self.sorted_keys: List[int] = []
        self.nodes: set = set()
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def _hash(self, key: str) -> int:
        """Hash function menggunakan MD5"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: str):
        """Add node ke ring"""
        if node in self.nodes:
            return
        
        self.nodes.add(node)
        
        # Add virtual nodes
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}:{i}"
            hash_value = self._hash(virtual_key)
            self.ring[hash_value] = node
        
        # Re-sort keys
        self.sorted_keys = sorted(self.ring.keys())
        
        logger.info(f"Added node {node} to hash ring ({self.virtual_nodes} virtual nodes)")
    
    def remove_node(self, node: str):
        """Remove node dari ring"""
        if node not in self.nodes:
            return
        
        self.nodes.remove(node)
        
        # Remove virtual nodes
        keys_to_remove = [k for k, v in self.ring.items() if v == node]
        for key in keys_to_remove:
            del self.ring[key]
        
        # Re-sort keys
        self.sorted_keys = sorted(self.ring.keys())
        
        logger.info(f"Removed node {node} from hash ring")
    
    def get_node(self, key: str) -> Optional[str]:
        """
        Get node yang responsible untuk key.
        
        Algorithm:
        1. Hash the key
        2. Find first node dengan hash >= key hash
        3. Jika tidak ada, wrap around ke first node
        """
        if not self.ring:
            return None
        
        hash_value = self._hash(key)
        
        # Find first node with hash >= hash_value
        for ring_hash in self.sorted_keys:
            if ring_hash >= hash_value:
                return self.ring[ring_hash]
        
        # Wrap around ke first node
        return self.ring[self.sorted_keys[0]]
    
    def get_nodes_for_replication(self, key: str, n: int = 3) -> List[str]:
        """
        Get n nodes untuk replication.
        Returns nodes in clockwise order dari key position.
        """
        if not self.ring or n <= 0:
            return []
        
        hash_value = self._hash(key)
        nodes = []
        seen = set()
        
        # Start dari position pada ring
        start_idx = 0
        for i, ring_hash in enumerate(self.sorted_keys):
            if ring_hash >= hash_value:
                start_idx = i
                break
        
        # Collect n unique nodes
        idx = start_idx
        while len(nodes) < n and len(seen) < len(self.nodes):
            node = self.ring[self.sorted_keys[idx % len(self.sorted_keys)]]
            if node not in seen:
                nodes.append(node)
                seen.add(node)
            idx += 1
        
        return nodes


class QueueNode(BaseNode):
    """
    Distributed Queue Node.
    
    Features:
    - Consistent hashing untuk partition messages
    - Redis untuk persistence
    - At-least-once delivery
    - Automatic retry
    """
    
    def __init__(self, node_id: int, host: str, port: int, peers: List[str]):
        super().__init__(node_id, host, port, peers)
        
        # Consistent hash ring
        all_nodes = [f"{host}:{port}"] + peers
        self.hash_ring = ConsistentHashRing(all_nodes)
        
        # Redis connection
        self.redis: Optional[aioredis.Redis] = None
        
        # Local queue storage (in-memory, backed by Redis)
        self.queue: Dict[str, QueueMessage] = {}
        
        # Processing tracking
        self.processing: Dict[str, float] = {}  # message_id -> start_time
        self.processing_timeout = 30.0  # seconds
        
        # Statistics
        self.messages_enqueued = 0
        self.messages_dequeued = 0
        self.messages_failed = 0
        
        # Add custom routes
        self.app.router.add_post('/api/queue/enqueue', self.handle_enqueue)
        self.app.router.add_post('/api/queue/dequeue', self.handle_dequeue)
        self.app.router.add_post('/api/queue/ack', self.handle_ack)
        self.app.router.add_get('/api/queue/status', self.handle_queue_status)
        
        # Background tasks
        self._retry_task: Optional[asyncio.Task] = None
        
        logger.info(f"QueueNode {node_id} initialized")
    
    async def start(self):
        """Start queue node"""
        await super().start()
        
        # Connect to Redis
        from ..utils.config import Config
        self.redis = aioredis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB,
            decode_responses=False
        )
        
        # Test connection
        try:
            await self.redis.ping()
            logger.info("Connected to Redis successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
        
        # Load persisted messages
        await self._load_from_redis()
        
        # Start retry task
        self._retry_task = asyncio.create_task(self._retry_loop())
        
        logger.info(f"QueueNode {self.node_id} started")
    
    async def stop(self):
        """Stop queue node"""
        if self._retry_task:
            self._retry_task.cancel()
        
        # Close Redis
        # Close Redis
        if self.redis:
            await self.redis.close()

        
        await super().stop()
    
    async def _load_from_redis(self):
        """Load messages dari Redis"""
        try:
            # Get all messages untuk this node
            pattern = f"queue:{self.node_id}:*"
            cursor = 0

            async for key in self.redis.scan_iter(match=pattern):
                data = await self.redis.get(key)
                if data:
                    msg = QueueMessage.from_dict(json.loads(data))
                    self.queue[msg.message_id] = msg

                
                for key in keys:
                    data = await self.redis.get(key)
                    if data:
                        msg = QueueMessage.from_dict(json.loads(data))
                        self.queue[msg.message_id] = msg
            
            logger.info(f"Loaded {len(self.queue)} messages from Redis")
            
        except Exception as e:
            logger.error(f"Error loading from Redis: {e}")
    
    async def _save_to_redis(self, message: QueueMessage):
        """Save message ke Redis"""
        try:
            key = f"queue:{self.node_id}:{message.message_id}"
            await self.redis.set(key, json.dumps(message.to_dict()))
        except Exception as e:
            logger.error(f"Error saving to Redis: {e}")
    
    async def _delete_from_redis(self, message_id: str):
        """Delete message dari Redis"""
        try:
            key = f"queue:{self.node_id}:{message_id}"
            await self.redis.delete(key)
        except Exception as e:
            logger.error(f"Error deleting from Redis: {e}")
    
    def _get_responsible_node(self, message_id: str) -> str:
        """Get node yang responsible untuk message"""
        return self.hash_ring.get_node(message_id)
    
    def _is_responsible_for(self, message_id: str) -> bool:
        """Check apakah node ini responsible untuk message"""
        responsible_node = self._get_responsible_node(message_id)
        my_address = f"{self.host}:{self.port}"
        return responsible_node == my_address
    
    async def enqueue(self, message_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enqueue message.
        
        Message akan di-route ke responsible node berdasarkan consistent hashing.
        """
        # Check jika kita responsible untuk message ini
        if not self._is_responsible_for(message_id):
            responsible_node = self._get_responsible_node(message_id)
            return {
                'status': 'redirected',
                'responsible_node': responsible_node,
                'message': 'Message routed to responsible node'
            }
        
        # Create message
        msg = QueueMessage(
            message_id=message_id,
            data=data,
            timestamp=time.time(),
            status='pending'
        )
        
        # Store in memory dan Redis
        self.queue[message_id] = msg
        await self._save_to_redis(msg)
        
        self.messages_enqueued += 1
        
        logger.info(f"Enqueued message {message_id}")
        
        return {
            'status': 'enqueued',
            'message_id': message_id,
            'node_id': self.node_id
        }
    
    async def dequeue(self) -> Optional[Dict[str, Any]]:
        """
        Dequeue message (at-least-once delivery).
        
        Returns:
            Message jika ada, None jika queue kosong
        """
        # Find pending message
        for message_id, msg in self.queue.items():
            if msg.status == 'pending':
                # Mark as processing
                msg.status = 'processing'
                self.processing[message_id] = time.time()
                await self._save_to_redis(msg)
                
                self.messages_dequeued += 1
                
                logger.info(f"Dequeued message {message_id}")
                
                return {
                    'message_id': message_id,
                    'data': msg.data,
                    'retry_count': msg.retry_count
                }
        
        return None
    
    async def ack(self, message_id: str) -> Dict[str, Any]:
        """
        Acknowledge message completion.
        Message akan dihapus dari queue.
        """
        if message_id not in self.queue:
            return {
                'status': 'error',
                'message': 'Message not found'
            }
        
        # Remove from queue
        del self.queue[message_id]
        if message_id in self.processing:
            del self.processing[message_id]
        
        await self._delete_from_redis(message_id)
        
        logger.info(f"Acknowledged message {message_id}")
        
        return {
            'status': 'acknowledged',
            'message_id': message_id
        }
    
    async def _retry_loop(self):
        """Background task untuk retry failed/timeout messages"""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                current_time = time.time()
                
                # Check for timeout processing messages
                for message_id, start_time in list(self.processing.items()):
                    if current_time - start_time > self.processing_timeout:
                        # Timeout, mark as pending untuk retry
                        if message_id in self.queue:
                            msg = self.queue[message_id]
                            msg.status = 'pending'
                            msg.retry_count += 1
                            
                            if msg.retry_count > 3:
                                # Too many retries, mark as failed
                                msg.status = 'failed'
                                self.messages_failed += 1
                                logger.warning(f"Message {message_id} failed after {msg.retry_count} retries")
                            else:
                                logger.warning(f"Message {message_id} timeout, retrying (attempt {msg.retry_count})")
                            
                            await self._save_to_redis(msg)
                            del self.processing[message_id]
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in retry loop: {e}")
    
    async def handle_enqueue(self, request: web.Request) -> web.Response:
        """HTTP endpoint untuk enqueue"""
        try:
            data = await request.json()
            message_id = data['message_id']
            message_data = data['data']
            
            result = await self.enqueue(message_id, message_data)
            return web.json_response(result)
            
        except Exception as e:
            logger.error(f"Error in enqueue: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_dequeue(self, request: web.Request) -> web.Response:
        """HTTP endpoint untuk dequeue"""
        try:
            result = await self.dequeue()
            
            if result:
                return web.json_response(result)
            else:
                return web.json_response({'message': 'Queue empty'}, status=404)
            
        except Exception as e:
            logger.error(f"Error in dequeue: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_ack(self, request: web.Request) -> web.Response:
        """HTTP endpoint untuk acknowledge"""
        try:
            data = await request.json()
            message_id = data['message_id']
            
            result = await self.ack(message_id)
            return web.json_response(result)
            
        except Exception as e:
            logger.error(f"Error in ack: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def handle_queue_status(self, request: web.Request) -> web.Response:
        """Get queue status"""
        status = {
            'node_id': self.node_id,
            'queue_size': len(self.queue),
            'pending': sum(1 for m in self.queue.values() if m.status == 'pending'),
            'processing': len(self.processing),
            'failed': sum(1 for m in self.queue.values() if m.status == 'failed'),
            'statistics': {
                'enqueued': self.messages_enqueued,
                'dequeued': self.messages_dequeued,
                'failed': self.messages_failed
            },
            'hash_ring_nodes': list(self.hash_ring.nodes)
        }
        return web.json_response(status)


# Test code
async def test_queue_node():
    """Test QueueNode"""
    # Start Redis terlebih dahulu dengan Docker:
    # docker run -d -p 6379:6379 redis:7-alpine
    
    # Create queue nodes
    nodes = []
    
    for i in range(1, 4):
        peers = [f"localhost:{7000+j}" for j in range(1, 4) if j != i]
        node = QueueNode(
            node_id=i,
            host='localhost',
            port=7000 + i,
            peers=peers
        )
        nodes.append(node)
    
    # Start all nodes
    for node in nodes:
        await node.start()
    
    print("Queue nodes started")
    await asyncio.sleep(1)
    
    # Test enqueue
    print("\n=== Testing Queue ===")
    
    # Enqueue messages
    for i in range(5):
        message_id = f"msg_{i}"
        data = {'content': f'Test message {i}', 'value': i}
        
        # Find responsible node
        responsible_node = nodes[0].hash_ring.get_node(message_id)
        print(f"\nMessage '{message_id}' -> Node {responsible_node}")
        
        # Enqueue ke node pertama (akan di-redirect jika perlu)
        result = await nodes[0].enqueue(message_id, data)
        print(f"  Enqueue result: {result}")
    
    await asyncio.sleep(1)
    
    # Check status
    print("\n=== Queue Status ===")
    for node in nodes:
        print(f"\nNode {node.node_id}:")
        print(f"  Queue size: {len(node.queue)}")
        print(f"  Messages: {list(node.queue.keys())}")
    
    # Test dequeue
    print("\n=== Testing Dequeue ===")
    for node in nodes:
        msg = await node.dequeue()
        if msg:
            print(f"\nNode {node.node_id} dequeued: {msg['message_id']}")
            # Acknowledge
            await node.ack(msg['message_id'])
            print(f"  Acknowledged {msg['message_id']}")
    
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
    asyncio.run(test_queue_node())
