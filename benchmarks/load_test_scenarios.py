"""
Load testing scenarios menggunakan Locust.

Cara menjalankan:
  locust -f benchmarks/load_test_scenarios.py --host=http://localhost:5001
"""

from locust import HttpUser, task, between, events
import random
import json
import time


class LockManagerUser(HttpUser):
    """
    Simulate user yang menggunakan Lock Manager.
    """
    wait_time = between(0.5, 2.0)  # Wait 0.5-2 seconds antara tasks
    
    def on_start(self):
        """Called saat user start"""
        self.user_id = random.randint(1, 1000)
        self.resources = [f"resource_{i}" for i in range(10)]
    
    @task(3)
    def acquire_exclusive_lock(self):
        """Acquire exclusive lock"""
        resource = random.choice(self.resources)
        
        with self.client.post(
            "/api/lock/acquire",
            json={
                'resource_id': resource,
                'lock_type': 'exclusive',
                'requester_id': self.user_id
            },
            catch_response=True
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'acquired':
                    response.success()
                    # Hold lock for a bit
                    time.sleep(random.uniform(0.1, 0.5))
                    # Release lock
                    self.release_lock(resource)
                else:
                    response.failure(f"Lock not acquired: {data['status']}")
            else:
                response.failure(f"HTTP {response.status_code}")
    
    @task(2)
    def acquire_shared_lock(self):
        """Acquire shared lock"""
        resource = random.choice(self.resources)
        
        with self.client.post(
            "/api/lock/acquire",
            json={
                'resource_id': resource,
                'lock_type': 'shared',
                'requester_id': self.user_id
            },
            catch_response=True
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'acquired':
                    response.success()
                    time.sleep(random.uniform(0.1, 0.3))
                    self.release_lock(resource)
    
    def release_lock(self, resource_id):
        """Release lock"""
        self.client.post(
            "/api/lock/release",
            json={
                'resource_id': resource_id,
                'releaser_id': self.user_id
            }
        )
    
    @task(1)
    def check_status(self):
        """Check lock status"""
        self.client.get("/api/lock/status")


class QueueUser(HttpUser):
    """
    Simulate user yang menggunakan Queue.
    """
    wait_time = between(0.3, 1.5)
    
    def on_start(self):
        self.user_id = random.randint(1, 1000)
        self.message_counter = 0
    
    @task(2)
    def enqueue_message(self):
        """Enqueue message"""
        self.message_counter += 1
        message_id = f"msg_{self.user_id}_{self.message_counter}"
        
        with self.client.post(
            "/api/queue/enqueue",
            json={
                'message_id': message_id,
                'data': {
                    'user_id': self.user_id,
                    'timestamp': time.time(),
                    'content': f"Test message {self.message_counter}"
                }
            },
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"HTTP {response.status_code}")
    
    @task(3)
    def dequeue_message(self):
        """Dequeue message"""
        with self.client.post(
            "/api/queue/dequeue",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if 'message_id' in data:
                    # Acknowledge message
                    self.client.post(
                        "/api/queue/ack",
                        json={'message_id': data['message_id']}
                    )
                    response.success()
            elif response.status_code == 404:
                # Queue empty is OK
                response.success()
            else:
                response.failure(f"HTTP {response.status_code}")
    
    @task(1)
    def check_queue_status(self):
        """Check queue status"""
        self.client.get("/api/queue/status")


class CacheUser(HttpUser):
    """
    Simulate user yang menggunakan Cache.
    """
    wait_time = between(0.2, 1.0)
    
    def on_start(self):
        self.keys = [f"key_{i}" for i in range(100)]
    
    @task(5)
    def cache_get(self):
        """Get dari cache"""
        key = random.choice(self.keys)
        
        with self.client.get(
            f"/api/cache/get?key={key}",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
    
    @task(2)
    def cache_put(self):
        """Put ke cache"""
        key = random.choice(self.keys)
        value = {
            'data': f"value_{random.randint(1, 1000)}",
            'timestamp': time.time()
        }
        
        with self.client.post(
            "/api/cache/put",
            json={'key': key, 'value': value},
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
    
    @task(1)
    def cache_status(self):
        """Check cache status"""
        self.client.get("/api/cache/status")


# Event handlers untuk custom metrics
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("Load test starting...")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("Load test complete!")
    print(f"Total requests: {environment.stats.total.num_requests}")
    print(f"Failure rate: {environment.stats.total.fail_ratio:.2%}")
