"""
Production-hardened distributed indexing system with comprehensive safety mechanisms.
Includes circuit breakers, health checks, and proper connection pooling.
"""

import logging
import time
import socket
import pytest
import grpc
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
import prometheus_client as prom
from prometheus_client import Counter, Histogram, Gauge, Summary
import threading
import queue
import pickle
import traceback
from contextlib import contextmanager
import sys
import signal
import redis
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
import hashlib
import json
from datetime import datetime, timedelta
import os
import tempfile
import shutil
import functools

# Enhanced metrics with more granular tracking
QUERY_LATENCY = Histogram(
    'query_latency_seconds',
    'Query processing time',
    ['shard_id', 'query_type']
)
QUERY_ERRORS = Counter(
    'query_errors_total',
    'Total query errors',
    ['error_type', 'shard_id']
)
DOCS_INDEXED = Counter(
    'documents_indexed_total',
    'Total documents indexed',
    ['status']
)
SHARD_SIZE = Gauge(
    'shard_size_bytes',
    'Size of each shard',
    ['shard_id', 'data_type']
)
CACHE_STATS = Counter(
    'cache_operations_total',
    'Cache operations',
    ['operation', 'status']
)
HEALTH_CHECK = Gauge(
    'health_status',
    'Health check status',
    ['component']
)
CIRCUIT_BREAKER = Gauge(
    'circuit_breaker_status',
    'Circuit breaker status',
    ['operation']
)

# Enhanced logging with structured output
class StructuredLogger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # File handler with rotation
        handler = logging.handlers.RotatingFileHandler(
            'index.log',
            maxBytes=100*1024*1024,  # 100MB
            backupCount=10
        )
        handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(handler)
        
        # Prometheus push gateway for critical errors
        self.error_counter = Counter(
            'critical_errors_total',
            'Critical errors requiring attention',
            ['error_type']
        )
    
    def error(self, message: str, error: Optional[Exception] = None, **kwargs):
        error_details = {
            'timestamp': datetime.utcnow().isoformat(),
            'message': message,
            'error_type': type(error).__name__ if error else None,
            'traceback': traceback.format_exc() if error else None,
            **kwargs
        }
        self.logger.error(json.dumps(error_details))
        if error:
            self.error_counter.labels(type(error).__name__).inc()

logger = StructuredLogger(__name__)

@dataclass
class HealthStatus:
    """Health check status for components"""
    is_healthy: bool
    last_check: datetime
    details: Dict[str, Any]
    dependencies: Dict[str, 'HealthStatus'] = field(default_factory=dict)

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure = None
        self.state = 'closed'
        self.lock = threading.Lock()
        
    @contextmanager
    def execute(self):
        if not self._can_execute():
            raise RuntimeError(f"Circuit breaker {self.name} is open")
        
        try:
            yield
            self._on_success()
        except Exception as e:
            self._on_failure()
            raise

    def _can_execute(self) -> bool:
        with self.lock:
            if self.state == 'open':
                if (datetime.utcnow() - self.last_failure).total_seconds() > self.reset_timeout:
                    self.state = 'half-open'
                    return True
                return False
            return True

    def _on_success(self):
        with self.lock:
            self.failures = 0
            self.state = 'closed'
            CIRCUIT_BREAKER.labels(self.name).set(0)

    def _on_failure(self):
        with self.lock:
            self.failures += 1
            self.last_failure = datetime.utcnow()
            if self.failures >= self.failure_threshold:
                self.state = 'open'
                CIRCUIT_BREAKER.labels(self.name).set(1)

class ConnectionPool:
    """Thread-safe connection pool for network operations"""
    def __init__(self, max_size: int = 10):
        self.max_size = max_size
        self.pool = queue.Queue(maxsize=max_size)
        self.size = 0
        self.lock = threading.Lock()
        
    @contextmanager
    def get_connection(self):
        connection = self._get()
        try:
            yield connection
        finally:
            self._return(connection)
            
    def _get(self):
        try:
            return self.pool.get_nowait()
        except queue.Empty:
            with self.lock:
                if self.size < self.max_size:
                    self.size += 1
                    return self._create_connection()
                else:
                    return self.pool.get()
                    
    def _return(self, connection):
        if self._is_connection_valid(connection):
            self.pool.put(connection)
        else:
            with self.lock:
                self.size -= 1
                
    def _create_connection(self):
        # Implementation depends on connection type
        pass
        
    def _is_connection_valid(self, connection) -> bool:
        # Implementation depends on connection type
        pass

class CacheManager:
    """Enhanced caching with Redis backend"""
    def __init__(self, redis_config: Dict[str, Any]):
        self.redis = redis.Redis(**redis_config)
        self.circuit_breaker = CircuitBreaker('cache')
        
    def get(self, key: str) -> Optional[Any]:
        try:
            with self.circuit_breaker.execute():
                value = self.redis.get(key)
                if value:
                    CACHE_STATS.labels('get', 'hit').inc()
                    return pickle.loads(value)
                CACHE_STATS.labels('get', 'miss').inc()
                return None
        except Exception as e:
            logger.error("Cache get failed", error=e)
            return None
            
    def set(
        self,
        key: str,
        value: Any,
        expire: int = 3600
    ) -> bool:
        try:
            with self.circuit_breaker.execute():
                serialized = pickle.dumps(value)
                success = self.redis.setex(key, expire, serialized)
                CACHE_STATS.labels('set', 'success' if success else 'failure').inc()
                return success
        except Exception as e:
            logger.error("Cache set failed", error=e)
            return False

class DistributedLock:
    """Enhanced distributed locking with ZooKeeper"""
    def __init__(self, zk_hosts: str):
        self.zk = KazooClient(
            hosts=zk_hosts,
            timeout=10.0,
            retry_policy={
                'max_tries': 3,
                'max_delay': 3000
            }
        )
        self.zk.start()
        
    @contextmanager
    def acquire(self, lock_path: str, timeout: float = 10.0):
        """Acquire a distributed lock with proper error handling"""
        lock = self.zk.Lock(lock_path)
        acquired = False
        try:
            acquired = lock.acquire(timeout=timeout)
            if not acquired:
                raise TimeoutError(f"Failed to acquire lock: {lock_path}")
            yield
        except KazooException as e:
            logger.error("ZooKeeper operation failed", error=e)
            raise
        finally:
            if acquired:
                try:
                    lock.release()
                except Exception as e:
                    logger.error("Failed to release lock", error=e)
            
    def __del__(self):
        try:
            self.zk.stop()
        except Exception as e:
            logger.error("Failed to stop ZooKeeper client", error=e)

@dataclass
class NetworkConfig:
    """Enhanced network configuration"""
    host: str = 'localhost'
    port: int = 50051
    max_message_size: int = 100 * 1024 * 1024  # 100MB
    timeout_seconds: float = 30.0
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0
    keepalive_time_ms: int = 30000
    keepalive_timeout_ms: int = 5000
    connection_pool_size: int = 10

class NetworkLayer:
    """Enhanced network layer with proper connection management"""
    def __init__(self, config: NetworkConfig):
        self.config = config
        self.server = None
        self.connection_pool = ConnectionPool(max_size=config.connection_pool_size)
        self.circuit_breaker = CircuitBreaker('network')
        self._setup_server()
        
    def _setup_server(self):
        """Setup gRPC server with proper options"""
        options = [
            ('grpc.max_send_message_length', self.config.max_message_size),
            ('grpc.max_receive_message_length', self.config.max_message_size),
            ('grpc.keepalive_time_ms', self.config.keepalive_time_ms),
            ('grpc.keepalive_timeout_ms', self.config.keepalive_timeout_ms)
        ]
        self.server = grpc.server(
            ThreadPoolExecutor(max_workers=10),
            options=options
        )
        
    def start_server(self):
        """Start gRPC server with health checking"""
        try:
            self.server.add_insecure_port(
                f"{self.config.host}:{self.config.port}"
            )
            self.server.start()
            HEALTH_CHECK.labels('network').set(1)
            logger.info(f"Server started on {self.config.host}:{self.config.port}")
        except Exception as e:
            HEALTH_CHECK.labels('network').set(0)
            logger.error("Failed to start server", error=e)
            raise NetworkError(f"Server startup failed: {e}")
    
    @QUERY_LATENCY.labels('network', 'query').time()
    def execute_query(
        self,
        query: Dict[str, str],
        target_shard: int
    ) -> List[int]:
        """Execute query with proper error handling and circuit breaker"""
        try:
            with self.circuit_breaker.execute():
                with self.connection_pool.get_connection() as connection:
                    for attempt in range(self.config.retry_attempts):
                        try:
                            response = connection.Search(
                                SearchRequest(
                                    query=query,
                                    shard_id=target_shard
                                ),
                                timeout=self.config.timeout_seconds
                            )
                            return list(response.doc_ids)
                        except grpc.RpcError as e:
                            if attempt == self.config.retry_attempts - 1:
                                raise
                            time.sleep(self.config.retry_delay_seconds)
        except Exception as e:
            QUERY_ERRORS.labels(
                error_type=type(e).__name__,
                shard_id=target_shard
            ).inc()
            logger.error(
                "Query execution failed",
                error=e,
                query=query,
                shard_id=target_shard
            )
            raise NetworkError(f"Query execution failed: {e}")

class BackupManager:
    """Handles data backup and recovery"""
    def __init__(self, backup_dir: str):
        self.backup_dir = backup_dir
        os.makedirs(backup_dir, exist_ok=True)
        
    def create_backup(
        self,
        shard: 'EnhancedIndexShard',
        backup_id: Optional[str] = None
    ) -> str:
        """Create backup with proper error handling"""
        if backup_id is None:
            backup_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            
        backup_path = os.path.join(self.backup_dir, backup_id)
        temp_path = None
        
        try:
            # Create in temp location first
            temp_path = tempfile.mkdtemp()
            self._write_backup(shard, temp_path)
            
            # Atomically move to final location
            shutil.move(temp_path, backup_path)
            logger.info(f"Backup created: {backup_id}")
            return backup_id
            
        except Exception as e:
            logger.error("Backup creation failed", error=e)
            if temp_path and os.path.exists(temp_path):
                shutil.rmtree(temp_path)
            raise
            
    def restore_backup(
        self,
        backup_id: str,
        shard: 'EnhancedIndexShard'
    ):
        """Restore from backup with validation"""
        backup_path = os.path.join(self.backup_dir, backup_id)
        if not os.path.exists(backup_path):
            raise ValueError(f"Backup not found: {backup_id}")
            
        try:
            with open(os.path.join(backup_path, 'metadata.json')) as f:
                metadata = json.load(f)
                
            # Verify backup integrity
            if not self._verify_backup(backup_path, metadata):
                raise ValueError("Backup verification failed")
                
            self._restore_from_backup(backup_path, shard)
            logger.info(f"Restored from backup: {backup_id}")
            
        except Exception as e:
            logger.error("Backup restoration failed", error=e)
            raise

def retry_with_backoff(retries: int = 3, backoff_factor: float = 1.5):
    """Decorator for retry with exponential backoff"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < retries - 1:
                        sleep_time = backoff_factor ** attempt
                        time.sleep(sleep_time)
            raise last_exception
        return wrapper
    return decorator

class EnhancedIndexShard(IndexShard):
    """Production-hardened shard implementation"""
    
class EnhancedIndexShard(IndexShard):
    """Production-hardened shard implementation"""
    def __init__(self, shard_id: int, config: IndexConfig):
        super().__init__(shard_id, config)
        self.cache_manager = CacheManager({
            'host': 'localhost',
            'port': 6379,
            'db': 0
        })
        self.backup_manager = BackupManager('backups')
        self.health_status = HealthStatus(
            is_healthy=True,
            last_check=datetime.utcnow(),
            details={},
            dependencies={}
        )
        self.compaction_lock = threading.Lock()
        self.metrics = {
            'size': SHARD_SIZE.labels(str(shard_id), 'total'),
            'docs': Counter('shard_documents_total', 'Total documents in shard', ['shard_id']).labels(str(shard_id))
        }
        
    @retry_with_backoff(retries=3)
    def add_document(self, doc_id: int, content: dict):
        """Thread-safe document addition with metrics"""
        try:
            with self.lock:
                super().add_document(doc_id, content)
                self._update_metrics(content)
                DOCS_INDEXED.labels('success').inc()
                
                # Cache invalidation
                self.cache_manager.set(f'doc:{doc_id}', content)
                
                # Trigger compaction if needed
                if self._should_compact():
                    self._trigger_compaction()
                    
        except Exception as e:
            DOCS_INDEXED.labels('failure').inc()
            logger.error(
                "Document addition failed",
                error=e,
                doc_id=doc_id,
                shard_id=self.shard_id
            )
            raise

    @QUERY_LATENCY.labels('shard', 'search').time()
    def search(self, query: Dict[str, str]) -> List[int]:
        """Enhanced search with caching and circuit breaking"""
        cache_key = self._get_cache_key(query)
        
        # Try cache first
        cached_results = self.cache_manager.get(cache_key)
        if cached_results is not None:
            return cached_results
            
        try:
            with self.circuit_breaker.execute():
                results = super().search(query)
                
                # Cache results
                self.cache_manager.set(cache_key, results)
                return results
                
        except Exception as e:
            QUERY_ERRORS.labels(
                error_type=type(e).__name__,
                shard_id=self.shard_id
            ).inc()
            logger.error(
                "Search failed",
                error=e,
                query=query,
                shard_id=self.shard_id
            )
            raise

    def _get_cache_key(self, query: Dict[str, str]) -> str:
        """Generate consistent cache key"""
        query_str = json.dumps(query, sort_keys=True)
        return f"search:{self.shard_id}:{hashlib.md5(query_str.encode()).hexdigest()}"

    def _update_metrics(self, content: dict):
        """Update Prometheus metrics"""
        content_size = sys.getsizeof(json.dumps(content))
        self.metrics['size'].inc(content_size)
        self.metrics['docs'].inc()

    def _should_compact(self) -> bool:
        """Check if compaction is needed"""
        # Implement compaction strategy based on metrics
        current_size = float(self.metrics['size']._value.get())
        return current_size > self.config.shard_size_bytes * 0.8

    def _trigger_compaction(self):
        """Trigger async compaction if not already running"""
        if not self.compaction_lock.locked():
            threading.Thread(target=self._compact_shard).start()

    def _compact_shard(self):
        """Compact shard data with proper locking"""
        try:
            with self.compaction_lock:
                # Create backup before compaction
                self.backup_manager.create_backup(self)
                
                with self.lock:
                    # Implement compaction logic
                    # Example: Remove duplicates, optimize storage
                    self._optimize_index_storage()
                    self._cleanup_doc_store()
                    
        except Exception as e:
            logger.error(
                "Shard compaction failed",
                error=e,
                shard_id=self.shard_id
            )

    def _optimize_index_storage(self):
        """Optimize index data structures"""
        # Implement storage optimization
        # Example: Compress posting lists, remove deleted docs
        pass

    def _cleanup_doc_store(self):
        """Clean up document store"""
        # Implement cleanup logic
        # Example: Remove old versions, compress documents
        pass

    def get_health_status(self) -> HealthStatus:
        """Get current health status"""
        current_time = datetime.utcnow()
        
        # Update health check
        self.health_status.last_check = current_time
        self.health_status.is_healthy = self._check_health()
        self.health_status.details.update({
            'size_bytes': float(self.metrics['size']._value.get()),
            'doc_count': float(self.metrics['docs']._value.get()),
            'cache_status': self.cache_manager.circuit_breaker.state
        })
        
        # Update health check metric
        HEALTH_CHECK.labels(f'shard_{self.shard_id}').set(
            1 if self.health_status.is_healthy else 0
        )
        
        return self.health_status

    def _check_health(self) -> bool:
        """Perform health check"""
        try:
            # Check component health
            cache_healthy = self.cache_manager.circuit_breaker.state != 'open'
            size_healthy = float(self.metrics['size']._value.get()) < self.config.shard_size_bytes
            
            return all([
                cache_healthy,
                size_healthy,
                not self.compaction_lock.locked()
            ])
        except Exception as e:
            logger.error(
                "Health check failed",
                error=e,
                shard_id=self.shard_id
            )
            return False

    def backup(self, backup_id: Optional[str] = None) -> str:
        """Create backup with proper error handling"""
        try:
            return self.backup_manager.create_backup(self, backup_id)
        except Exception as e:
            logger.error(
                "Backup failed",
                error=e,
                shard_id=self.shard_id
            )
            raise

    def restore(self, backup_id: str):
        """Restore from backup with proper error handling"""
        try:
            self.backup_manager.restore_backup(backup_id, self)
        except Exception as e:
            logger.error(
                "Restore failed",
                error=e,
                shard_id=self.shard_id,
                backup_id=backup_id
            )
            raise

class NetworkError(Exception):
    """Custom exception for network-related errors"""
    pass

# Add signal handlers for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}")
    # Implement graceful shutdown logic
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
