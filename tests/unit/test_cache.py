"""
Unit tests untuk Cache dan MESI protocol.
"""

import pytest
from src.nodes.cache_node import LRUCache, MESIState, CacheLine


def test_lru_cache_basic():
    """Test basic LRU cache operations"""
    cache = LRUCache(capacity=3)
    
    # Test put
    cache.put('key1', 'value1', MESIState.EXCLUSIVE)
    assert len(cache) == 1
    assert 'key1' in cache
    
    # Test get
    line = cache.get('key1')
    assert line is not None
    assert line.value == 'value1'
    assert line.state == MESIState.EXCLUSIVE


def test_lru_cache_eviction():
    """Test LRU eviction"""
    cache = LRUCache(capacity=2)
    
    # Fill cache
    cache.put('key1', 'value1', MESIState.EXCLUSIVE)
    cache.put('key2', 'value2', MESIState.EXCLUSIVE)
    
    assert len(cache) == 2
    
    # This should evict key1 (LRU)
    evicted = cache.put('key3', 'value3', MESIState.EXCLUSIVE)
    
    assert evicted == 'key1'
    assert len(cache) == 2
    assert 'key1' not in cache
    assert 'key2' in cache
    assert 'key3' in cache


def test_lru_cache_access_order():
    """Test LRU access ordering"""
    cache = LRUCache(capacity=2)
    
    cache.put('key1', 'value1', MESIState.EXCLUSIVE)
    cache.put('key2', 'value2', MESIState.EXCLUSIVE)
    
    # Access key1 (make it most recent)
    cache.get('key1')
    
    # Now key2 is LRU, should be evicted
    evicted = cache.put('key3', 'value3', MESIState.EXCLUSIVE)
    
    assert evicted == 'key2'
    assert 'key1' in cache
    assert 'key3' in cache


def test_mesi_state_update():
    """Test MESI state transitions"""
    cache = LRUCache(capacity=10)
    
    # Start with EXCLUSIVE
    cache.put('key1', 'value1', MESIState.EXCLUSIVE)
    line = cache.get('key1')
    assert line.state == MESIState.EXCLUSIVE
    
    # Transition to SHARED
    cache.update_state('key1', MESIState.SHARED)
    line = cache.get('key1')
    assert line.state == MESIState.SHARED
    
    # Transition to MODIFIED
    cache.update_state('key1', MESIState.MODIFIED)
    line = cache.get('key1')
    assert line.state == MESIState.MODIFIED
    
    # Transition to INVALID
    cache.update_state('key1', MESIState.INVALID)
    line = cache.get('key1')
    assert line.state == MESIState.INVALID


def test_cache_hit_rate():
    """Test cache hit rate calculation"""
    cache = LRUCache(capacity=10)
    
    # Initial hit rate should be 0
    assert cache.get_hit_rate() == 0.0
    
    # Add entries
    cache.put('key1', 'value1', MESIState.EXCLUSIVE)
    cache.put('key2', 'value2', MESIState.EXCLUSIVE)
    
    # 2 hits
    cache.get('key1')
    cache.get('key2')
    
    # 1 miss
    cache.get('key3')
    
    # Hit rate = 2 / 3 = 0.666...
    assert abs(cache.get_hit_rate() - 0.666) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
