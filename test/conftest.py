"""
Pytest configuration and fixtures for PKBrokers tests
"""

import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock, PropertyMock

# Set environment variables EARLY before any imports
os.environ['KTOKEN'] = 'test_token'
os.environ['KUSER'] = 'test_user'
os.environ['DB_TYPE'] = 'local'
os.environ['DB_TICKS'] = '0'
os.environ['PKDevTools_Default_Log_Level'] = '0'


class MockPKEnvironment:
    """Mock PKEnvironment class"""
    def __init__(self):
        self.DB_TYPE = 'local'
        self.DB_TICKS = 0
        self.KTOKEN = 'test_token'
        self.KUSER = 'test_user'
        self.allSecrets = {}
        self.ZH_CLIENT_CODE = 'test_code'
        self.ZH_AUTH_TOKEN = 'test_auth'
    
    def __getattr__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            return None


# Patch PKEnvironment before modules import it
sys.modules['PKEnvironment'] = MockPKEnvironment
original_pke = None

try:
    from PKDevTools.classes.Environment import PKEnvironment as OrigPKEnvironment
    original_pke = OrigPKEnvironment
    
    # Replace with our mock
    def mock_pke_constructor(*args, **kwargs):
        return MockPKEnvironment()
    
    # Patch at module level
    import PKDevTools.classes.Environment
    PKDevTools.classes.Environment.PKEnvironment = MockPKEnvironment
except ImportError:
    pass


@pytest.fixture
def mock_logger():
    """Provide a mock logger"""
    logger = Mock()
    logger.debug = Mock()
    logger.info = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    return logger


