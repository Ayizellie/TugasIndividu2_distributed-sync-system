"""
Base Node class untuk semua distributed nodes.
Mengintegrasikan Raft consensus, message passing, dan failure detection.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from aiohttp import web

from ..consensus.raft import RaftNode
from ..communication.message_passing import MessagePassing, Message, MessageType
from ..communication.failure_detector import FailureDetector
from ..utils.config import Config
from ..utils.metrics import metrics

logger = logging.getLogger(__name__)


class BaseNode:
    """
    Base class untuk distributed nodes.
    Menyediakan common functionality:
    - HTTP API server
    - Raft consensus integration
    - Message passing
    - Failure detection
    """
    
    def __init__(self, node_id: int, host: str, port: int, peers: List[str]):
        """
        Args:
            node_id: Unique ID untuk node
            host: Host address
            port: Port number
            peers: List of peer addresses ("host:port")
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        
        # Initialize components
        self.mp = MessagePassing(node_id, host, port)
        self.raft = RaftNode(
            node_id=node_id,
            peers=peers,
            message_passing=self.mp,
            election_timeout_range=(Config.ELECTION_TIMEOUT_MIN, Config.ELECTION_TIMEOUT_MAX),
            heartbeat_interval=Config.HEARTBEAT_INTERVAL
        )
        self.failure_detector = FailureDetector(
            node_id=node_id,
            heartbeat_interval=1.0,
            timeout_threshold=3.0
        )
        
        # HTTP server
        self.app = web.Application()
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        
        # Setup routes
        self._setup_routes()
        
        # Running state
        self._running = False
        
        logger.info(f"BaseNode {node_id} initialized at {host}:{port}")
    
    def _setup_routes(self):
        """Setup HTTP API routes"""
        self.app.router.add_post('/api/message', self.handle_message)
        self.app.router.add_get('/api/status', self.handle_status)
        self.app.router.add_get('/api/metrics', self.handle_metrics)
        self.app.router.add_get('/health', self.handle_health)
    
    async def start(self):
        """Start node dan semua components"""
        logger.info(f"Starting node {self.node_id}...")
        
        # Initialize message passing
        await self.mp.initialize()
        
        # Start Raft consensus
        await self.raft.start()
        
        # Start failure detector
        await self.failure_detector.start_monitoring()
        
        # Register peers untuk failure detection
        for i, peer in enumerate(self.peers, start=1):
            peer_id = i if i < self.node_id else i + 1
            self.failure_detector.register_node(peer_id)
        
        # Start HTTP server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        
        self._running = True
        logger.info(f"Node {self.node_id} started successfully at http://{self.host}:{self.port}")
    
    async def stop(self):
        """Stop node dan cleanup"""
        logger.info(f"Stopping node {self.node_id}...")
        
        self._running = False
        
        # Stop components
        await self.failure_detector.stop_monitoring()
        await self.raft.stop()
        await self.mp.close()
        
        # Stop HTTP server
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        
        logger.info(f"Node {self.node_id} stopped")
    
    async def handle_message(self, request: web.Request) -> web.Response:
        """
        Handle incoming messages dari nodes lain.
        Dispatch ke appropriate handler berdasarkan message type.
        """
        try:
            data = await request.json()
            message = Message.from_dict(data)
            
            logger.debug(f"Node {self.node_id}: Received {message.msg_type.value} from {message.sender_id}")
            
            # Update failure detector
            self.failure_detector.record_heartbeat(message.sender_id)
            
            # Dispatch berdasarkan message type
            response_data = await self._dispatch_message(message)
            
            return web.json_response(response_data)
            
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def _dispatch_message(self, message: Message) -> Dict[str, Any]:
        """
        Dispatch message ke appropriate handler.
        Override method ini untuk add custom message handlers.
        """
        # Raft messages
        if message.msg_type == MessageType.REQUEST_VOTE:
            return await self.raft.handle_request_vote(message)
        
        elif message.msg_type == MessageType.APPEND_ENTRIES:
            return await self.raft.handle_append_entries(message)
        
        elif message.msg_type == MessageType.HEARTBEAT:
            return {'status': 'ok', 'term': self.raft.raft_state.current_term}
        
        else:
            logger.warning(f"Unknown message type: {message.msg_type}")
            return {'error': 'unknown_message_type'}
    
    async def handle_status(self, request: web.Request) -> web.Response:
        """Get node status"""
        status = {
            'node_id': self.node_id,
            'address': f"{self.host}:{self.port}",
            'running': self._running,
            'raft_state': self.raft.get_state(),
            'failure_detector': self.failure_detector.get_stats(),
            'message_passing': self.mp.get_stats()
        }
        return web.json_response(status)
    
    async def handle_metrics(self, request: web.Request) -> web.Response:
        """Export Prometheus metrics"""
        metrics_data = metrics.get_metrics()
        return web.Response(body=metrics_data, content_type='text/plain')
    
    async def handle_health(self, request: web.Request) -> web.Response:
        """Health check endpoint"""
        if self._running:
            return web.json_response({'status': 'healthy'})
        else:
            return web.json_response({'status': 'unhealthy'}, status=503)
    
    def is_leader(self) -> bool:
        """Check if this node is the Raft leader"""
        return self.raft.is_leader()
    
    def get_leader_id(self) -> Optional[int]:
        """Get current Raft leader ID"""
        return self.raft.get_leader_id()
    
    async def wait_for_leader(self, timeout: float = 10.0) -> bool:
        """
        Wait sampai ada leader elected.
        
        Args:
            timeout: Maximum time to wait (seconds)
            
        Returns:
            True jika leader found, False jika timeout
        """
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            if self.get_leader_id() is not None:
                return True
            await asyncio.sleep(0.1)
        
        return False


# Test code
async def test_base_node():
    """Test BaseNode dengan 3 nodes"""
    nodes = []
    
    # Create 3 nodes
    for i in range(1, 4):
        peers = [f"localhost:{5000+j}" for j in range(1, 4) if j != i]
        node = BaseNode(
            node_id=i,
            host='localhost',
            port=5000 + i,
            peers=peers
        )
        nodes.append(node)
    
    # Start all nodes
    for node in nodes:
        await node.start()
    
    print("All nodes started. Waiting for leader election...")
    
    # Wait for leader
    await asyncio.sleep(2)
    
    # Check status
    for node in nodes:
        state = node.raft.get_state()
        print(f"\nNode {state['node_id']}:")
        print(f"  State: {state['state']}")
        print(f"  Term: {state['term']}")
        print(f"  Leader: {state['leader_id']}")
    
    # Keep running
    print("\nNodes running. Press Ctrl+C to stop...")
    try:
        await asyncio.sleep(30)
    except KeyboardInterrupt:
        pass
    
    # Stop all nodes
    print("\nStopping nodes...")
    for node in nodes:
        await node.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(test_base_node())
