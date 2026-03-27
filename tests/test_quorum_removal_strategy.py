# encoding: utf-8
"""
Unit tests for quorum removal strategies
"""

import sys
import time
from unittest.mock import patch
from pathlib import Path
import pytest

# Add src to path to import the module directly
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import quorum_removal_strategy

ImmediateRemovalStrategy = quorum_removal_strategy.ImmediateRemovalStrategy
DelayedRemovalStrategy = quorum_removal_strategy.DelayedRemovalStrategy


class TestImmediateRemovalStrategy:
    """Tests for ImmediateRemovalStrategy"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup method executed before each test"""
        self.strategy = ImmediateRemovalStrategy('test-host')
    
    def test_should_remove_host_always_true(self):
        """Strategy always returns True"""
        result = self.strategy.should_remove_host(
            'host1',
            ['host1', 'host2'],
            ['host2']
        )
        assert result is True
    
    def test_get_hosts_to_keep_removes_immediately(self):
        """Disappeared hosts are removed immediately"""
        current_quorum = ['host1', 'host2']
        quorum_hosts = ['host2']
        
        result = self.strategy.get_hosts_to_keep(current_quorum, quorum_hosts)
        
        assert set(result) == {'host2'}


class TestDelayedRemovalStrategy:
    """Tests for DelayedRemovalStrategy"""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup method executed before each test"""
        self.delay = 10.0
        self.strategy = DelayedRemovalStrategy('test-host', self.delay)
    
    def test_host_kept_within_delay(self):
        """Host remains in quorum if not enough time has passed"""
        current_quorum = ['host1', 'host2']
        quorum_hosts = ['host2']
        
        # First call - host disappeared
        result1 = self.strategy.get_hosts_to_keep(current_quorum, quorum_hosts)
        
        # Host should remain (delay seconds haven't passed yet)
        assert set(result1) == {'host1', 'host2'}
    
    def test_host_removed_after_delay(self):
        """Host is removed after delay expires"""
        current_quorum = ['host1', 'host2']
        quorum_hosts = ['host2']
        
        # Record the timestamp when host disappeared
        start_time = time.time()
        with patch('quorum_removal_strategy.time.monotonic', return_value=start_time):
            self.strategy.get_hosts_to_keep(current_quorum, quorum_hosts)
        
        # Simulate time passing - now return time after delay
        with patch('quorum_removal_strategy.time.monotonic', return_value=start_time + self.delay + 1):
            result = self.strategy.get_hosts_to_keep(current_quorum, quorum_hosts)
            
            # Host should be removed
            assert set(result) == {'host2'}
    
    def test_host_returned_cancels_removal(self):
        """Host return cancels removal"""
        current_quorum = ['host1', 'host2']
        quorum_hosts_disappeared = ['host2']
        quorum_hosts_returned = ['host1', 'host2']
        
        # Host disappeared
        self.strategy.get_hosts_to_keep(current_quorum, quorum_hosts_disappeared)
        
        # Host returned
        result = self.strategy.get_hosts_to_keep(current_quorum, quorum_hosts_returned)
        
        # Both hosts should be in quorum
        assert set(result) == {'host1', 'host2'}
        
        # Timestamp should be cleared
        assert 'host1' not in self.strategy._removal_timestamps
    
    def test_multiple_hosts_independent_timers(self):
        """Each host has an independent timer"""
        current_quorum = ['host1', 'host2', 'host3']
        
        # host1 disappeared
        start_time = time.time()
        with patch('quorum_removal_strategy.time.monotonic', return_value=start_time):
            self.strategy.get_hosts_to_keep(current_quorum, ['host2', 'host3'])
        
        # host2 disappeared slightly later
        with patch('quorum_removal_strategy.time.monotonic', return_value=start_time + 0.1):
            self.strategy.get_hosts_to_keep(current_quorum, ['host3'])
        
        # Check that hosts have different timestamps
        assert 'host1' in self.strategy._removal_timestamps
        assert 'host2' in self.strategy._removal_timestamps
        assert (self.strategy._removal_timestamps['host1'] !=
                self.strategy._removal_timestamps['host2'])
    
    def test_memory_cleanup_after_removal(self):
        """Timestamp is cleaned up after host removal to prevent memory leak"""
        current_quorum = ['host1', 'host2']
        quorum_hosts = ['host2']
        
        # Record the timestamp when host disappeared
        start_time = time.time()
        with patch('quorum_removal_strategy.time.monotonic', return_value=start_time):
            self.strategy.get_hosts_to_keep(current_quorum, quorum_hosts)
        
        # Verify timestamp was recorded
        assert 'host1' in self.strategy._removal_timestamps
        
        # Simulate time passing - trigger removal
        with patch('quorum_removal_strategy.time.monotonic', return_value=start_time + self.delay + 1):
            result = self.strategy.get_hosts_to_keep(current_quorum, quorum_hosts)
            
            # Host should be removed
            assert set(result) == {'host2'}
            
            # Timestamp should be cleaned up to prevent memory leak
            assert 'host1' not in self.strategy._removal_timestamps
    
    def test_own_host_not_delayed(self):
        """Own host removal is not delayed"""
        my_hostname = 'host1'
        strategy = DelayedRemovalStrategy(my_hostname, self.delay)
        
        current_quorum = ['host1', 'host2']
        quorum_hosts = ['host2']  # host1 disappeared
        
        # First call - host1 (own host) disappeared
        result = strategy.get_hosts_to_keep(current_quorum, quorum_hosts)
        
        # Own host should be removed immediately without delay
        assert set(result) == {'host2'}
        
        # Timestamp should not be recorded for own host
        assert 'host1' not in strategy._removal_timestamps
