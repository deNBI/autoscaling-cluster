"""Tests for converter utilities."""

from autoscaling.utils.converter import (
    convert_gb_to_mb,
    convert_gb_to_mib,
    convert_mib_to_gb,
    convert_tb_to_mb,
    convert_tb_to_mib,
    reduce_flavor_memory,
)


class TestUnitConversions:
    """Tests for unit conversion functions."""

    def test_convert_gb_to_mb(self):
        """Test GB to MB conversion."""
        assert convert_gb_to_mb(1) == 1000
        assert convert_gb_to_mb(8) == 8000
        assert convert_gb_to_mb(64) == 64000

    def test_convert_gb_to_mib(self):
        """Test GB to MiB conversion."""
        assert convert_gb_to_mib(1) == 1024
        assert convert_gb_to_mib(8) == 8192
        assert convert_gb_to_mib(64) == 65536

    def test_convert_mib_to_gb(self):
        """Test MiB to GB conversion."""
        assert convert_mib_to_gb(1024) == 1
        assert convert_mib_to_gb(8192) == 8
        assert convert_mib_to_gb(65536) == 64

    def test_convert_tb_to_mb(self):
        """Test TB to MB conversion."""
        assert convert_tb_to_mb(1) == 1000000
        assert convert_tb_to_mb(2) == 2000000

    def test_convert_tb_to_mib(self):
        """Test TB to MiB conversion."""
        assert convert_tb_to_mib(1) == 1024000
        assert convert_tb_to_mib(2) == 2048000


class TestFlavorMemory:
    """Tests for flavor memory calculations."""

    def test_reduce_flavor_memory_small(self):
        """Test memory reduction for small flavors (< 16GB)."""
        # 16 GB -> 16000 MB - 1000 (1/16) = 15000 MB, but min is 512
        result = reduce_flavor_memory(16)
        assert result < 16000
        assert result >= 512

    def test_reduce_flavor_memory_medium(self):
        """Test memory reduction for medium flavors."""
        result = reduce_flavor_memory(32)
        assert result < 32000

    def test_reduce_flavor_memory_large(self):
        """Test memory reduction for large flavors (> 16GB)."""
        # For large flavors, reduction is capped at 4000 MB
        result = reduce_flavor_memory(128)
        # 128 GB = 128000 MB, 1/16 = 8000, but max reduction is 4000
        assert result == 128000 - 4000
