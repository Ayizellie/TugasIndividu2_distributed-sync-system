"""
Metrics collector menggunakan Prometheus.
File ini mengumpulkan data performa sistem seperti
latency, throughput, dan resource usage.
"""

from prometheus_client import Counter, Histogram, Gauge, generate_latest
from typing import Dict
import time
import psutil


class MetricsCollector:
    """
    Class untuk mengumpulkan metrics sistem.
    Menggunakan Prometheus format untuk monitoring.
    """
    
    def __init__(self):
        # Counter: nilai yang selalu naik (contoh: jumlah request)
        self.request_count = Counter(
            'request_total',
            'Total number of requests',
            ['method', 'endpoint']
        )
        
        # Histogram: distribusi nilai (contoh: response time)
        self.request_latency = Histogram(
            'request_latency_seconds',
            'Request latency in seconds',
            ['method', 'endpoint']
        )
        
        # Gauge: nilai yang bisa naik/turun (contoh: jumlah node aktif)
        self.active_nodes = Gauge(
            'active_nodes',
            'Number of active nodes'
        )
        
        self.cache_hit_rate = Gauge(
            'cache_hit_rate',
            'Cache hit rate percentage'
        )
        
        self.queue_size = Gauge(
            'queue_size',
            'Current queue size'
        )
        
        # System metrics
        self.cpu_usage = Gauge('cpu_usage_percent', 'CPU usage percentage')
        self.memory_usage = Gauge('memory_usage_percent', 'Memory usage percentage')
        
    def record_request(self, method: str, endpoint: str, duration: float):
        """
        Record request metrics.
        
        Args:
            method: HTTP method (GET, POST, etc)
            endpoint: API endpoint
            duration: Request duration in seconds
        """
        self.request_count.labels(method=method, endpoint=endpoint).inc()
        self.request_latency.labels(method=method, endpoint=endpoint).observe(duration)
    
    def update_system_metrics(self):
        """Update CPU dan memory usage"""
        self.cpu_usage.set(psutil.cpu_percent())
        self.memory_usage.set(psutil.virtual_memory().percent)
    
    def set_active_nodes(self, count: int):
        """Update jumlah node aktif"""
        self.active_nodes.set(count)
    
    def set_cache_hit_rate(self, rate: float):
        """Update cache hit rate (0.0 - 1.0)"""
        self.cache_hit_rate.set(rate * 100)
    
    def set_queue_size(self, size: int):
        """Update queue size"""
        self.queue_size.set(size)
    
    def get_metrics(self) -> bytes:
        """
        Export metrics dalam Prometheus format.
        Returns: Metrics data dalam bytes
        """
        self.update_system_metrics()
        return generate_latest()


# Context manager untuk measure request time
class measure_time:
    """
    Context manager untuk mengukur execution time.
    
    Contoh penggunaan:
        with measure_time() as timer:
            # your code here
            pass
        print(f"Execution time: {timer.elapsed}s")
    """
    
    def __init__(self):
        self.start_time = None
        self.elapsed = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.time() - self.start_time
        return False


# Singleton instance
metrics = MetricsCollector()
