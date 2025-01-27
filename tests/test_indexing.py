"""
Tests for the indexing system core functionality.
"""

import pytest
from indexing_system.indexing_system import BloomFilter, IndexShard, IndexConfig

def test_bloom_filter():
    """Test BloomFilter functionality"""
    bf = BloomFilter(size=1000, num_hashes=3)
    test_item = "test_item"
    
    # Test addition
    bf.add(test_item)
    assert test_item in bf
    
    # Test false positive rate is reasonable
    false_positives = 0
    test_items = [f"item_{i}" for i in range(100)]
    for item in test_items:
        if item in bf:
            false_positives += 1
    assert false_positives < 10  # Less than 10% false positive rate

def test_index_shard():
    """Test IndexShard basic operations"""
    config = IndexConfig()
    shard = IndexShard(shard_id=0, config=config)
    
    # Test document addition
    doc = {"title": "Test Document", "content": "This is a test"}
    shard.add_document(1, doc)
    
    # Test search
    results = shard.search({"content": "test"})
    assert 1 in results

def test_concurrent_operations():
    """Test concurrent operations on IndexShard"""
    config = IndexConfig()
    shard = IndexShard(shard_id=0, config=config)
    
    # Test concurrent document additions
    import threading
    threads = []
    for i in range(10):
        doc = {"title": f"Doc {i}", "content": f"Content {i}"}
        t = threading.Thread(target=shard.add_document, args=(i, doc))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # Verify all documents were added
    for i in range(10):
        results = shard.search({"title": f"Doc {i}"})
        assert i in results
