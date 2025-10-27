# Distributed Synchronization System
Sistem sinkronisasi terdistribusi yang siap produksi dengan implementasi Raft Consensus, Consistent Hashing, dan MESI Cache Coherence Protocol.

### Link Youtube : 
**https://youtu.be/aYWEIQdJzSg?si=lj9sVvp1J08UF8h-**

## FITUR
### Distributed Lock Manager
- Algoritma Raft Consensus untuk koordinasi terdistribusi
- Dukungan untuk lock Exclusive dan Shared
- Mekanisme deteksi deadlock
- Penanganan partisi jaringan
- Leader election dan log replication
- Fault-tolerant (bertahan hingga N/2-1 kegagalan)

### Distributed Queue System
- Consistent Hashing dengan 150 virtual nodes
- Jaminan pengiriman at-least-once
- Persistensi pesan via Redis
- Automatic failover saat node gagal
- Dukungan untuk multiple producers/consumers
- Distribusi beban yang merata

### Distributed Cache System
- MESI Cache Coherence Protocol (Modified, Exclusive, Shared, Invalid)
- LRU replacement policy dengan kapasitas yang dapat dikonfigurasi
- Automatic cache invalidation antar node
- Remote data fetching
- Hit rate tinggi (60-70% tipikal)
- Statistik real-time

### Containerization
- Dukungan Docker dengan multi-stage builds
- Orkestrasi Docker Compose
- Kemampuan scaling dinamis
- Konfigurasi berbasis environment
- Health checks dan monitoring

### Arsitektur
┌──────────────────────────────────────────────────────────────────┐
│ SISTEM SINKRONISASI TERDISTRIBUSI │
├──────────────────────────────────────────────────────────────────┤
│ │
│ ┌────────────────┐ ┌────────────────┐ ┌────────────────┐ │
│ │ Lock Manager │ │ Queue System │ │ Cache System │ │
│ │ (Raft) │ │ (Hashing) │ │ (MESI) │ │
│ │ │ │ │ │ │ │
│ │ Node 1: 5001 │ │ Node 1: 6001 │ │ Node 1: 7001 │ │
│ │ Node 2: 5002 │ │ Node 2: 6002 │ │ Node 2: 7002 │ │
│ │ Node 3: 5003 │ │ Node 3: 6003 │ │ Node 3: 7003 │ │
│ └────────┬───────┘ └────────┬───────┘ └────────┬───────┘ │
│ │ │ │ │
│ └───────────────────┴───────────────────┘ │
│ │ │
│ ┌──────────────┴──────────────┐ │
│ │ Redis Storage │ │
│ │ - Persistensi │ │
│ │ - Message Queue │ │
│ └─────────────────────────────┘ │
└──────────────────────────────────────────────────────────

### Penjelasan Komponen 
**Lock Manager** (Berbasis Raft)
- Mengimplementasikan Raft consensus untuk distributed locking
- Memastikan konsistensi antar node
- Memiliki arsitektur Leader/Follower

**Queue System** (Consistent Hashing)
- Arsitektur peer-to-peer
- Distribusi pesan via hash ring
- Tidak ada single point of failure

**Cache System** (MESI Protocol)
- Peer-to-peer cache coherence
- Broadcast invalidation
- Kebijakan eviction LRU

## INSTALASI

### 1. Clone Repository
- git clone (link github) 
- cd distributed-sync-system

### 2. Install Dependensi Python
- python -m venv venv

### 3. Install Dependensi
- pip install -r requirements.txt

### 4. Jalankan Redis
- docker run -d -p 6379:6379 --name redis redis:7-alpine

### 5. Konfigurasi 
- cp .env.example .env


## Memulai 

### Deployment Manual

### Lock Manager

**Terminal 1 - Node 1**
- cd distributed-sync-system
- $env:NODE_ID="1"
- $env:NODE_PORT="5001"
- $env:PEERS="localhost:5002,localhost:5003"
- python run_lock_node.py

**Terminal 2 - Node 2**
- cd distributed-sync-system
- $env:NODE_ID="2"
- $env:NODE_PORT="5002"
- $env:PEERS="localhost:5001,localhost:5003"
- python run_lock_node.py

**Terminal 3 - Node 3:**
- cd distributed-sync-system
- $env:NODE_ID="3"
- $env:NODE_PORT="5003"
- $env:PEERS="localhost:5001,localhost:5002"
- python run_lock_node.py

**Terminal 4**
- === QUICK TEST LOCK MANAGER ===

- Write-Host "`n=== TEST 1: Node Status ===" -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri "http://localhost:5001/api/status" -Method GET
    $response.Content | ConvertFrom-Json | ConvertTo-Json
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

- Write-Host "`n=== TEST 2: Acquire EXCLUSIVE Lock ===" -ForegroundColor Cyan
try {
    $body = '{"resource_id": "resource_A", "lock_type": "exclusive", "requester_id": 1}'
    $response = Invoke-WebRequest -Uri "http://localhost:5001/api/lock/acquire" -Method POST -Body $body -ContentType "application/json"
    Write-Host "Result: $($response.Content)" -ForegroundColor Green
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

- Write-Host "`n=== TEST 3: Try Same Resource (Should Wait) ===" -ForegroundColor Cyan
try {
    $body = '{"resource_id": "resource_A", "lock_type": "exclusive", "requester_id": 2}'
    $response = Invoke-WebRequest -Uri "http://localhost:5001/api/lock/acquire" -Method POST -Body $body -ContentType "application/json"
    Write-Host "Result: $($response.Content)" -ForegroundColor Yellow
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

- Write-Host "`n=== TEST 4: Lock Status ===" -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri "http://localhost:5001/api/lock/status" -Method GET
    $response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 5
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

- Write-Host "`n=== TESTS COMPLETED ===" -ForegroundColor Green

### Queue System

**Terminal 1 - Node 1**
- cd E:\distributed-sync-system
- $env:NODE_ID="1"
- $env:NODE_PORT="6001"
- $env:PEERS="localhost:6002,localhost:6003"
- $env:REDIS_HOST="localhost"
- $env:REDIS_PORT="6379"
- python run_queue_node.py 

**Terminal 2 - Node 2**
- cd E:\distributed-sync-system
- $env:NODE_ID="2"
- $env:NODE_PORT="6002"
- $env:PEERS="localhost:6001,localhost:6003"
- $env:REDIS_HOST="localhost"
- $env:REDIS_PORT="6379"
- python run_queue_node.py 

**Terminal 3 - Node 3:**
- cd E:\distributed-sync-system
- $env:NODE_ID="3"
- $env:NODE_PORT="6003"
- $env:PEERS="localhost:6001,localhost:6002"
- $env:REDIS_HOST="localhost"
- $env:REDIS_PORT="6379"
- python run_queue_node.py 

**Terminal 4**
- === DISTRIBUTED QUEUE TESTING ===

Write-Host "`n=== TEST 1: Queue Status ===" -ForegroundColor Cyan
Invoke-WebRequest -Uri "http://localhost:6001/api/queue/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json

Write-Host "`n=== TEST 2: Enqueue Messages ===" -ForegroundColor Cyan
# Message 1
$msg1 = '{"message_id": "msg_001", "data": {"content": "Hello from Queue", "timestamp": 1698765432}}'
$result1 = Invoke-WebRequest -Uri "http://localhost:6001/api/queue/enqueue" -Method POST -Body $msg1 -ContentType "application/json"
Write-Host "Result 1: $($result1.Content)" -ForegroundColor Green

# Message 2
$msg2 = '{"message_id": "msg_002", "data": {"content": "Test Message 2", "timestamp": 1698765433}}'
$result2 = Invoke-WebRequest -Uri "http://localhost:6001/api/queue/enqueue" -Method POST -Body $msg2 -ContentType "application/json"
Write-Host "Result 2: $($result2.Content)" -ForegroundColor Green

# Message 3
$msg3 = '{"message_id": "msg_003", "data": {"content": "Test Message 3", "timestamp": 1698765434}}'
$result3 = Invoke-WebRequest -Uri "http://localhost:6001/api/queue/enqueue" -Method POST -Body $msg3 -ContentType "application/json"
Write-Host "Result 3: $($result3.Content)" -ForegroundColor Green

Write-Host "`n=== TEST 3: Check Distribution (All Nodes) ===" -ForegroundColor Cyan
Write-Host "`nNode 1:" -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:6001/api/queue/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json

Write-Host "`nNode 2:" -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:6002/api/queue/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json

Write-Host "`nNode 3:" -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:6003/api/queue/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json

Write-Host "`n=== TEST 4: Dequeue Message ===" -ForegroundColor Cyan
$dequeued = Invoke-WebRequest -Uri "http://localhost:6001/api/queue/dequeue" -Method POST | Select-Object -ExpandProperty Content
Write-Host "Dequeued: $dequeued" -ForegroundColor Green

Write-Host "`n=== TEST 5: Acknowledge Message ===" -ForegroundColor Cyan
$dequeuedObj = $dequeued | ConvertFrom-Json
$ack = "{`"message_id`": `"$($dequeuedObj.message_id)`"}"
Invoke-WebRequest -Uri "http://localhost:6001/api/queue/ack" -Method POST -Body $ack -ContentType "application/json" | Select-Object -ExpandProperty Content

Write-Host "`n=== TEST 6: Final Queue Status ===" -ForegroundColor Cyan
Invoke-WebRequest -Uri "http://localhost:6001/api/queue/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json -Depth 5

Write-Host "`n=== ALL QUEUE TESTS COMPLETED ===" -ForegroundColor Green


### Cache System

**Terminal 1 - Node 1**
- cd E:\distributed-sync-system
- $env:NODE_ID="1"
- $env:NODE_PORT="7001"
- $env:PEERS="localhost:7002,localhost:7003"
- $env:CACHE_SIZE="1000"
- $env:CACHE_POLICY="LRU"
- python run_cache_node.py 

**Terminal 2 - Node 2**
- cd E:\distributed-sync-system
- $env:NODE_ID="2"
- $env:NODE_PORT="7002"
- $env:PEERS="localhost:7001,localhost:7003"
- $env:CACHE_SIZE="1000"
- $env:CACHE_POLICY="LRU"
- python run_cache_node.py 

**Terminal 3 - Node 3:**
- cd E:\distributed-sync-system
- $env:NODE_ID="3"
- $env:NODE_PORT="7003"
- $env:PEERS="localhost:7001,localhost:7002"
- $env:CACHE_SIZE="1000"
- $env:CACHE_POLICY="LRU"
- python run_cache_node.py  

**Terminal 4**
# === DISTRIBUTED CACHE TESTING (MESI PROTOCOL) ===

Write-Host "`n=== TEST 1: Initial Cache Status ===" -ForegroundColor Cyan
Invoke-WebRequest -Uri "http://localhost:7001/api/cache/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json

Write-Host "`n=== TEST 2: PUT to Node 1 (State = MODIFIED) ===" -ForegroundColor Cyan
$put1 = '{"key": "user_123", "value": {"name": "John Doe", "email": "john@example.com", "age": 30}}'
$result = Invoke-WebRequest -Uri "http://localhost:7001/api/cache/put" -Method POST -Body $put1 -ContentType "application/json"
Write-Host "Result: $($result.Content)" -ForegroundColor Green

Write-Host "`n=== TEST 3: GET from Node 1 (Local HIT) ===" -ForegroundColor Cyan
$get1 = Invoke-WebRequest -Uri "http://localhost:7001/api/cache/get?key=user_123" -Method GET
Write-Host "Result: $($get1.Content)" -ForegroundColor Green

Write-Host "`n=== TEST 4: GET from Node 2 (Remote FETCH - Both become SHARED) ===" -ForegroundColor Cyan
$get2 = Invoke-WebRequest -Uri "http://localhost:7002/api/cache/get?key=user_123" -Method GET
Write-Host "Result: $($get2.Content)" -ForegroundColor Green

Start-Sleep -Seconds 1

Write-Host "`n=== TEST 5: PUT from Node 3 (INVALIDATE Node 1 & 2) ===" -ForegroundColor Cyan
$put2 = '{"key": "user_123", "value": {"name": "Jane Doe UPDATED", "email": "jane@example.com", "status": "modified"}}'
$result2 = Invoke-WebRequest -Uri "http://localhost:7003/api/cache/put" -Method POST -Body $put2 -ContentType "application/json"
Write-Host "Result: $($result2.Content)" -ForegroundColor Green

Start-Sleep -Seconds 1

Write-Host "`n=== TEST 6: Verify Cache Status & Statistics ===" -ForegroundColor Cyan
Write-Host "`nNode 1 Statistics:" -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:7001/api/cache/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | Select-Object node_id, cache_size, hit_rate, statistics | ConvertTo-Json

Write-Host "`nNode 2 Statistics:" -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:7002/api/cache/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | Select-Object node_id, cache_size, hit_rate, statistics | ConvertTo-Json

Write-Host "`nNode 3 Statistics:" -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:7003/api/cache/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | Select-Object node_id, cache_size, hit_rate, statistics | ConvertTo-Json

Write-Host "`n=== TEST 7: Multiple Cache Operations (Test LRU) ===" -ForegroundColor Cyan
$items = @('product_001', 'product_002', 'product_003')
foreach ($item in $items) {
    $body = "{`"key`": `"$item`", `"value`": {`"name`": `"Item $item`", `"price`": 10000}}"
    Invoke-WebRequest -Uri "http://localhost:7001/api/cache/put" -Method POST -Body $body -ContentType "application/json" | Out-Null
    Write-Host "Added: $item" -ForegroundColor Gray
}

Write-Host "`n=== TEST 8: Test Hit Rate ===" -ForegroundColor Cyan
# Multiple GETs to increase hit rate
1..5 | ForEach-Object {
    Invoke-WebRequest -Uri "http://localhost:7001/api/cache/get?key=product_001" -Method GET | Out-Null
}
Write-Host "Performed 5 GET requests for product_001" -ForegroundColor Gray

Invoke-WebRequest -Uri "http://localhost:7001/api/cache/status" -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json | Select-Object node_id, cache_size, hit_rate, statistics | ConvertTo-Json

Write-Host "`n=== ALL CACHE TESTS COMPLETED ===" -ForegroundColor Green


### Deployment Docker
- docker ps | findstr redis

### Docker compose
- docker build -f docker/Docker.node -t distributed-sync-node:Latest
- docker images | findstr distributed-sync 
- docker-compose -f docker/docker-compose.yml up -d
- docker-compose -f docker/docker-compose.yml logs -f
- docker-compose -f docker/docker-compose.yml ps
- docker logs lock-node-1
- docker-compose -f docker/docker-compose.yml down

