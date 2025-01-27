import jwt
import bcrypt
import logging
import ssl
from typing import Dict, Optional, List, Any, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.requests import RequestsInstrumentation
from prometheus_client import Counter, Histogram, Gauge
import yaml
import re
from functools import wraps
import hashlib
import secrets
import base64
from enum import Enum
import threading
from ratelimit import limits, RateLimitException

# Security Enhancements

class Role(Enum):
    ADMIN = "admin"
    WRITER = "writer"
    READER = "reader"

@dataclass
class User:
    username: str
    role: Role
    rate_limit: int  # requests per minute
    permissions: List[str]

class SecurityManager:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.key)
        self.jwt_secret = self.config['jwt_secret']
        self.salt = bcrypt.gensalt()
        self._setup_ssl()
        self.user_cache: Dict[str, User] = {}
        self.user_cache_lock = threading.Lock()
        
    def _load_config(self, path: str) -> dict:
        with open(path) as f:
            config = yaml.safe_load(f)
        self._validate_config(config)
        return config
        
    def _validate_config(self, config: dict):
        required_fields = ['jwt_secret', 'ssl_cert', 'ssl_key', 'allowed_domains']
        if not all(field in config for field in required_fields):
            raise ValueError("Missing required security configuration fields")

    def _setup_ssl(self):
        self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.ssl_context.load_cert_chain(
            certfile=self.config['ssl_cert'],
            keyfile=self.config['ssl_key']
        )
        self.ssl_context.verify_mode = ssl.CERT_REQUIRED

    def authenticate(self, token: str) -> Optional[User]:
        try:
            payload = jwt.decode(token, self.jwt_secret, algorithms=['HS256'])
            username = payload['sub']
            
            with self.user_cache_lock:
                if username in self.user_cache:
                    return self.user_cache[username]
                
            user = self._load_user(username)
            if user:
                with self.user_cache_lock:
                    self.user_cache[username] = user
            return user
            
        except jwt.InvalidTokenError:
            SECURITY_METRICS.auth_failures.inc()
            return None

    def authorize(self, user: User, required_permission: str) -> bool:
        return required_permission in user.permissions

    def encrypt_data(self, data: bytes) -> bytes:
        return self.cipher_suite.encrypt(data)

    def decrypt_data(self, encrypted_data: bytes) -> bytes:
        return self.cipher_suite.decrypt(encrypted_data)

    def _load_user(self, username: str) -> Optional[User]:
        # Implementation would load from secure user store
        pass

# Observability Enhancements

class Metrics:
    def __init__(self):
        # Security metrics
        self.auth_failures = Counter(
            'auth_failures_total',
            'Total authentication failures'
        )
        self.authorization_denials = Counter(
            'authorization_denials_total',
            'Total authorization denials',
            ['permission']
        )
        
        # Operation metrics
        self.query_latency = Histogram(
            'query_latency_seconds',
            'Query processing latency',
            ['operation', 'status']
        )
        self.index_size = Gauge(
            'index_size_bytes',
            'Current index size in bytes',
            ['shard']
        )
        
        # Resource metrics
        self.memory_usage = Gauge(
            'memory_usage_bytes',
            'Current memory usage'
        )
        self.cpu_usage = Gauge(
            'cpu_usage_percent',
            'Current CPU usage percentage'
        )

SECURITY_METRICS = Metrics()

class TracingManager:
    def __init__(self):
        trace.set_tracer_provider(TracerProvider())
        jaeger_exporter = JaegerExporter(
            agent_host_name="localhost",
            agent_port=6831,
        )
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(jaeger_exporter)
        )
        self.tracer = trace.get_tracer(__name__)
        RequestsInstrumentation().instrument()

# Operational Enhancements

class OperationsManager:
    def __init__(self):
        self.upgrade_lock = threading.Lock()
        self.maintenance_mode = False
        
    @contextmanager
    def maintenance(self):
        """Context manager for maintenance mode"""
        try:
            self.maintenance_mode = True
            yield
        finally:
            self.maintenance_mode = False
    
    def perform_upgrade(self, new_version: str):
        """Handles rolling upgrades"""
        with self.upgrade_lock:
            # 1. Verify new version
            if not self._verify_version(new_version):
                raise ValueError(f"Invalid version: {new_version}")
                
            # 2. Take backup
            self._backup_current_state()
            
            # 3. Upgrade components one by one
            try:
                with self.maintenance():
                    for component in self._get_components():
                        self._upgrade_component(component, new_version)
            except Exception as e:
                # 4. Rollback on failure
                self._rollback()
                raise

    def _verify_version(self, version: str) -> bool:
        return bool(re.match(r'^\d+\.\d+\.\d+$', version))

# Decorator for securing endpoints
def secure_endpoint(required_permission: str):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Extract token from request
            token = kwargs.get('token')
            if not token:
                SECURITY_METRICS.auth_failures.inc()
                raise ValueError("Missing authentication token")
            
            # Authenticate
            user = self.security_manager.authenticate(token)
            if not user:
                SECURITY_METRICS.auth_failures.inc()
                raise ValueError("Invalid authentication token")
            
            # Authorize
            if not self.security_manager.authorize(user, required_permission):
                SECURITY_METRICS.authorization_denials.labels(required_permission).inc()
                raise ValueError(f"Missing required permission: {required_permission}")
            
            # Apply rate limiting
            rate_limit = getattr(user, 'rate_limit', 60)  # default 60 rpm
            @limits(calls=rate_limit, period=60)
            def rate_limited_func(*a, **kw):
                return func(self, *a, **kw)
            
            return rate_limited_func(*args, **kwargs)
        return wrapper
    return decorator

# Input validation
class InputValidator:
    @staticmethod
    def validate_query(query: Dict[str, str]):
        """Validates search query parameters"""
        if not query:
            raise ValueError("Empty query")
            
        # Validate query structure and content
        for field, value in query.items():
            if not isinstance(field, str) or not isinstance(value, str):
                raise ValueError("Invalid query field types")
            if len(value) > 1000:  # Prevent extremely long queries
                raise ValueError("Query value too long")
            if not re.match(r'^[a-zA-Z0-9_\s\-\.]+$', value):
                raise ValueError("Invalid query characters")

    @staticmethod
    def validate_document(document: Dict[str, Any]):
        """Validates document content before indexing"""
        if not document:
            raise ValueError("Empty document")
            
        required_fields = ['id', 'content']
        if not all(field in document for field in required_fields):
            raise ValueError("Missing required fields")
            
        # Validate content size
        content_size = len(str(document['content']))
        if content_size > 10_000_000:  # 10MB limit
            raise ValueError("Document too large")

# Example usage:
def setup_security_and_monitoring(config_path: str):
    """Initialize security and monitoring systems"""
    security_manager = SecurityManager(config_path)
    tracing_manager = TracingManager()
    operations_manager = OperationsManager()
    
    return security_manager, tracing_manager, operations_manager
