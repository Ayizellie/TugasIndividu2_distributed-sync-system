"""
Failure detector untuk mendeteksi node failures.
Menggunakan heartbeat mechanism untuk monitor node health.
"""

import asyncio
import time
from typing import Dict, Set, Callable, Optional
import logging

logger = logging.getLogger(__name__)


class FailureDetector:
    """
    Failure detector menggunakan heartbeat protocol.
    Mendeteksi node failures dengan timeout mechanism.
    """
    
    def __init__(self, 
                 node_id: int,
                 heartbeat_interval: float = 1.0,
                 timeout_threshold: float = 3.0):
        """
        Args:
            node_id: ID node ini
            heartbeat_interval: Interval untuk send heartbeat (seconds)
            timeout_threshold: Threshold untuk declare node as failed (seconds)
        """
        self.node_id = node_id
        self.heartbeat_interval = heartbeat_interval
        self.timeout_threshold = timeout_threshold
        
        # Track last heartbeat time dari setiap node
        self.last_heartbeat: Dict[int, float] = {}
        
        # Set of failed nodes
        self.failed_nodes: Set[int] = set()
        
        # Callback untuk notify saat node failure detected
        self.on_failure_callback: Optional[Callable[[int], None]] = None
        self.on_recovery_callback: Optional[Callable[[int], None]] = None
        
        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._running = False
    
    def register_node(self, node_id: int):
        """Register node untuk monitoring"""
        if node_id not in self.last_heartbeat:
            self.last_heartbeat[node_id] = time.time()
            logger.info(f"Registered node {node_id} for monitoring")
    
    def record_heartbeat(self, node_id: int):
        """
        Record heartbeat dari node.
        Dipanggil saat menerima heartbeat message.
        """
        current_time = time.time()
        self.last_heartbeat[node_id] = current_time
        
        # Check if node was previously failed
        if node_id in self.failed_nodes:
            self.failed_nodes.remove(node_id)
            logger.info(f"Node {node_id} recovered")
            
            if self.on_recovery_callback:
                self.on_recovery_callback(node_id)
    
    def is_node_alive(self, node_id: int) -> bool:
        """
        Check apakah node masih alive.
        
        Returns:
            True jika node alive, False jika failed
        """
        if node_id not in self.last_heartbeat:
            return False
        
        time_since_heartbeat = time.time() - self.last_heartbeat[node_id]
        return time_since_heartbeat < self.timeout_threshold
    
    async def start_monitoring(self):
        """Start background monitoring task"""
        self._running = True
        self._monitoring_task = asyncio.create_task(self._monitor_loop())
        logger.info(f"Failure detector started (interval={self.heartbeat_interval}s, "
                   f"timeout={self.timeout_threshold}s)")
    
    async def stop_monitoring(self):
        """Stop background monitoring"""
        self._running = False
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("Failure detector stopped")
    
    async def _monitor_loop(self):
        """
        Background loop untuk check node health.
        Runs every heartbeat_interval.
        """
        while self._running:
            try:
                current_time = time.time()
                
                # Check semua registered nodes
                for node_id, last_time in list(self.last_heartbeat.items()):
                    time_since_heartbeat = current_time - last_time
                    
                    # Check timeout
                    if time_since_heartbeat >= self.timeout_threshold:
                        if node_id not in self.failed_nodes:
                            # Node baru detected as failed
                            self.failed_nodes.add(node_id)
                            logger.warning(f"Node {node_id} declared as FAILED "
                                         f"(no heartbeat for {time_since_heartbeat:.1f}s)")
                            
                            if self.on_failure_callback:
                                self.on_failure_callback(node_id)
                
                # Sleep until next check
                await asyncio.sleep(self.heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.heartbeat_interval)
    
    def get_alive_nodes(self) -> Set[int]:
        """
        Get set of alive nodes.
        
        Returns:
            Set of node IDs yang masih alive
        """
        return {
            node_id for node_id in self.last_heartbeat.keys()
            if self.is_node_alive(node_id)
        }
    
    def get_failed_nodes(self) -> Set[int]:
        """Get set of failed nodes"""
        return self.failed_nodes.copy()
    
    def set_failure_callback(self, callback: Callable[[int], None]):
        """Set callback untuk notify saat node failure"""
        self.on_failure_callback = callback
    
    def set_recovery_callback(self, callback: Callable[[int], None]):
        """Set callback untuk notify saat node recovery"""
        self.on_recovery_callback = callback
    
    def get_stats(self) -> Dict:
        """Get failure detector statistics"""
        return {
            'total_nodes': len(self.last_heartbeat),
            'alive_nodes': len(self.get_alive_nodes()),
            'failed_nodes': len(self.failed_nodes),
            'failed_node_ids': list(self.failed_nodes)
        }


# Test code
async def test_failure_detector():
    """Test failure detector"""
    detector = FailureDetector(
        node_id=1,
        heartbeat_interval=0.5,
        timeout_threshold=2.0
    )
    
    # Register callbacks
    def on_failure(node_id):
        print(f"❌ Node {node_id} FAILED!")
    
    def on_recovery(node_id):
        print(f"✅ Node {node_id} RECOVERED!")
    
    detector.set_failure_callback(on_failure)
    detector.set_recovery_callback(on_recovery)
    
    # Register nodes
    detector.register_node(2)
    detector.register_node(3)
    
    # Start monitoring
    await detector.start_monitoring()
    
    # Simulate heartbeats
    for i in range(10):
        print(f"\n--- Second {i+1} ---")
        
        # Node 2 always sends heartbeat
        detector.record_heartbeat(2)
        
        # Node 3 stops sending after 3 seconds
        if i < 3:
            detector.record_heartbeat(3)
        
        # Print stats
        stats = detector.get_stats()
        print(f"Stats: {stats}")
        print(f"Alive nodes: {detector.get_alive_nodes()}")
        
        await asyncio.sleep(1)
    
    await detector.stop_monitoring()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_failure_detector())
