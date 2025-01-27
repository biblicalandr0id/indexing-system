from typing import Dict, List, Any, Optional
import mmh3  # MurmurHash3 for fast hashing
import numpy as np
from collections import defaultdict
import mmap
import os
from concurrent.futures import ThreadPoolExecutor
import threading
from dataclasses import dataclass
import json
import heapq

@dataclass
class IndexConfig:
    """Configuration for the indexing system"""
    shard_size_bytes: int = 1024 * 1024 * 1024  # 1GB per shard
    num_partitions: int = 16
    cache_size_bytes: int = 8 * 1024 * 1024 * 1024  # 8GB cache
    max_concurrent_queries: int = 32

class BloomFilter:
    """Probabilistic data structure for fast membership testing"""
    def __init__(self, size: int, num_hashes: int):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = np.zeros(size, dtype=bool)
    
    def add(self, item: str):
        for seed in range(self.num_hashes):
            idx = mmh3.hash(item, seed) % self.size
            self.bit_array[idx] = True
    
    def __contains__(self, item: str) -> bool:
        return all(
            self.bit_array[mmh3.hash(item, seed) % self.size]
            for seed in range(self.num_hashes)
        )

class IndexShard:
    """Single shard of the index, managing a subset of the data"""
    def __init__(self, shard_id: int, config: IndexConfig):
        self.shard_id = shard_id
        self.config = config
        self.index: Dict[str, List[int]] = defaultdict(list)
        self.doc_store: Dict[int, bytes] = {}
        self.bloom_filter = BloomFilter(size=10_000_000, num_hashes=7)
        self.lock = threading.RLock()
        
    def add_document(self, doc_id: int, content: dict):
        """Index a single document"""
        with self.lock:
            serialized = json.dumps(content).encode()
            self.doc_store[doc_id] = serialized
            
            # Index each field
            for field, value in content.items():
                if isinstance(value, str):
                    terms = self._tokenize(value)
                    for term in terms:
                        self.index[f"{field}:{term}"].append(doc_id)
                        self.bloom_filter.add(f"{field}:{term}")
    
    def _tokenize(self, text: str) -> List[str]:
        """Simple whitespace tokenization with lowercasing"""
        return text.lower().split()
    
    def search(self, query: Dict[str, str]) -> List[int]:
        """Search within this shard"""
        with self.lock:
            results = []
            for field, value in query.items():
                terms = self._tokenize(value)
                field_results = None
                
                for term in terms:
                    term_key = f"{field}:{term}"
                    if term_key not in self.bloom_filter:
                        continue
                        
                    matching_docs = set(self.index.get(term_key, []))
                    if field_results is None:
                        field_results = matching_docs
                    else:
                        field_results &= matching_docs
                
                if field_results:
                    results.append(field_results)
            
            if not results:
                return []
            
            # Intersection of all field results
            final_results = set.intersection(*results)
            return sorted(final_results)

class DistributedIndex:
    """Main index class managing multiple shards"""
    def __init__(self, config: IndexConfig):
        self.config = config
        self.shards: List[IndexShard] = []
        self.doc_count = 0
        self.executor = ThreadPoolExecutor(max_workers=config.max_concurrent_queries)
        
    def initialize(self, num_shards: int):
        """Initialize the distributed index"""
        self.shards = [
            IndexShard(i, self.config)
            for i in range(num_shards)
        ]
    
    def add_document(self, content: dict):
        """Add a document to the index"""
        doc_id = self.doc_count
        shard_id = doc_id % len(self.shards)
        self.shards[shard_id].add_document(doc_id, content)
        self.doc_count += 1
        
    def search(self, query: Dict[str, str], limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search across all shards and merge results
        Returns top K results based on relevance
        """
        # Search each shard in parallel
        future_results = [
            self.executor.submit(shard.search, query)
            for shard in self.shards
        ]
        
        # Merge results using a min-heap for top K
        merged = []
        for future in future_results:
            shard_results = future.result()
            merged.extend(shard_results)
        
        # Sort by document ID for now (could be extended with scoring)
        merged.sort()
        return merged[:limit]

class IndexManager:
    """Manages index lifecycle and persistence"""
    def __init__(self, data_dir: str, config: IndexConfig):
        self.data_dir = data_dir
        self.config = config
        self.index = DistributedIndex(config)
        
    def build_index(self, documents: List[Dict[str, Any]]):
        """Build the index from a collection of documents"""
        num_shards = max(1, len(documents) // self.config.shard_size_bytes)
        self.index.initialize(num_shards)
        
        for doc in documents:
            self.index.add_document(doc)
    
    def save(self):
        """Persist the index to disk"""
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Save each shard
        for shard in self.index.shards:
            shard_path = os.path.join(self.data_dir, f"shard_{shard.shard_id}")
            with open(shard_path, 'wb') as f:
                # Save index structure
                index_data = {
                    'index': dict(shard.index),
                    'doc_store': {
                        str(k): v.decode() 
                        for k, v in shard.doc_store.items()
                    }
                }
                f.write(json.dumps(index_data).encode())
    
    def load(self) -> DistributedIndex:
        """Load the index from disk"""
        shard_files = [f for f in os.listdir(self.data_dir) if f.startswith('shard_')]
        self.index.initialize(len(shard_files))
        
        for shard_file in shard_files:
            shard_id = int(shard_file.split('_')[1])
            shard_path = os.path.join(self.data_dir, shard_file)
            
            with open(shard_path, 'rb') as f:
                data = json.loads(f.read().decode())
                shard = self.index.shards[shard_id]
                
                # Restore index structure
                shard.index = defaultdict(list, data['index'])
                shard.doc_store = {
                    int(k): v.encode()
                    for k, v in data['doc_store'].items()
                }
                
                # Rebuild bloom filter
                for term in shard.index.keys():
                    shard.bloom_filter.add(term)
        
        return self.index

# Usage example:
if __name__ == "__main__":
    # Configuration
    config = IndexConfig(
        shard_size_bytes=1024 * 1024 * 1024,  # 1GB
        num_partitions=16,
        cache_size_bytes=8 * 1024 * 1024 * 1024,  # 8GB
        max_concurrent_queries=32
    )
    
    # Sample documents
    documents = [
        {
            "id": "1",
            "title": "Big Data Processing",
            "content": "Techniques for processing large datasets..."
        },
        {
            "id": "2",
            "title": "Distributed Systems",
            "content": "Architecture of distributed systems..."
        }
    ]
    
    # Initialize and build index
    manager = IndexManager("index_data", config)
    manager.build_index(documents)
    
    # Save index to disk
    manager.save()
    
    # Load index from disk
    loaded_index = manager.load()
    
    # Perform search
    query = {
        "title": "data processing",
        "content": "distributed"
    }
    results = loaded_index.search(query, limit=10)
    print(f"Search results: {results}")
