# encoding: utf-8
"""
Unit tests for replication manager factory and configuration builder
"""

import pytest
import sys
from pathlib import Path
from configparser import RawConfigParser

# Add src to path to import modules directly
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import replication_manager_factory

build_replication_manager_config = replication_manager_factory.build_replication_manager_config
ReplicationManagerConfig = replication_manager_factory.ReplicationManagerConfig


def create_test_config(quorum_removal_delay=30.0):
    """Helper to create a test configuration"""
    config = RawConfigParser()
    
    config.add_section('global')
    config.set('global', 'priority', '100')
    
    config.add_section('replica')
    config.set('replica', 'primary_unavailability_timeout', '60.0')
    
    config.add_section('primary')
    config.set('primary', 'change_replication_metric', 'count')
    config.set('primary', 'weekday_change_hours', '9-18')
    config.set('primary', 'weekend_change_hours', '0-0')
    config.set('primary', 'overload_sessions_ratio', '0.8')
    config.set('primary', 'before_async_unavailability_timeout', '10.0')
    config.set('primary', 'quorum_removal_delay', str(quorum_removal_delay))
    
    return config


class TestReplicationManagerConfigBuilder:
    """Tests for build_replication_manager_config function"""
    
    def test_valid_config_creation(self):
        """Test that valid configuration is created correctly"""
        config = create_test_config(quorum_removal_delay=30.0)
        result = build_replication_manager_config(config)
        
        assert isinstance(result, ReplicationManagerConfig)
        assert result.priority == 100
        assert result.primary_unavailability_timeout == 60.0
        assert result.change_replication_metric == 'count'
        assert result.weekday_change_hours == '9-18'
        assert result.weekend_change_hours == '0-0'
        assert result.overload_sessions_ratio == 0.8
        assert result.before_async_unavailability_timeout == 10.0
        assert result.quorum_removal_delay == 30.0
    
    def test_valid_delay_zero(self):
        """Test that delay=0 is accepted (immediate removal)"""
        config = create_test_config(quorum_removal_delay=0.0)
        result = build_replication_manager_config(config)
        assert result.quorum_removal_delay == 0.0
    
    def test_valid_delay_positive(self):
        """Test that positive delays in valid range are accepted"""
        test_values = [1.0, 30.0, 60.0, 120.0]
        
        for delay in test_values:
            config = create_test_config(quorum_removal_delay=delay)
            result = build_replication_manager_config(config)
            assert result.quorum_removal_delay == delay, f"Failed for delay={delay}"
    
    def test_negative_delay_corrected_to_zero(self):
        """Test that negative delays are corrected to 0"""
        config = create_test_config(quorum_removal_delay=-10.0)
        result = build_replication_manager_config(config)
        assert result.quorum_removal_delay == 0.0
    
    def test_large_delay_capped_at_120(self):
        """Test that delays > 120 are capped at 120"""
        test_values = [121.0, 200.0, 300.0, 1000.0]
        
        for delay in test_values:
            config = create_test_config(quorum_removal_delay=delay)
            result = build_replication_manager_config(config)
            assert result.quorum_removal_delay == 120.0, f"Failed for delay={delay}"
    
    def test_boundary_value_120(self):
        """Test that delay=120 is accepted"""
        config = create_test_config(quorum_removal_delay=120.0)
        result = build_replication_manager_config(config)
        assert result.quorum_removal_delay == 120.0
    
    def test_boundary_value_120_point_1(self):
        """Test that delay=120.1 is capped"""
        config = create_test_config(quorum_removal_delay=120.1)
        result = build_replication_manager_config(config)
        assert result.quorum_removal_delay == 120.0
    
    def test_fractional_delays(self):
        """Test that fractional delays in valid range are accepted"""
        test_values = [0.5, 1.5, 30.5, 59.9, 119.9]
        
        for delay in test_values:
            config = create_test_config(quorum_removal_delay=delay)
            result = build_replication_manager_config(config)
            assert result.quorum_removal_delay == delay, f"Failed for delay={delay}"
    
    def test_recommended_range(self):
        """Test that values in recommended range (0-60) are accepted"""
        test_values = [0.0, 1.0, 30.0, 60.0]
        
        for delay in test_values:
            config = create_test_config(quorum_removal_delay=delay)
            result = build_replication_manager_config(config)
            assert result.quorum_removal_delay == delay, f"Failed for delay={delay}"
    
    def test_above_recommended_but_below_max(self):
        """Test that values between 60 and 120 are accepted"""
        test_values = [61.0, 90.0, 119.0, 120.0]
        
        for delay in test_values:
            config = create_test_config(quorum_removal_delay=delay)
            result = build_replication_manager_config(config)
            assert result.quorum_removal_delay == delay, f"Failed for delay={delay}"
    
    def test_very_negative_value(self):
        """Test that very negative values are corrected to 0"""
        config = create_test_config(quorum_removal_delay=-1000.0)
        result = build_replication_manager_config(config)
        assert result.quorum_removal_delay == 0.0
    
    def test_edge_case_just_below_zero(self):
        """Test that values just below zero are corrected to 0"""
        config = create_test_config(quorum_removal_delay=-0.1)
        result = build_replication_manager_config(config)
        assert result.quorum_removal_delay == 0.0
