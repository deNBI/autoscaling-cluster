"""
Unit tests for the data module.
"""
import json
import tempfile
import unittest
from pathlib import Path

from autoscaling.data.manager import DatabaseManager
from autoscaling.data.models import (
    DatabaseContent,
    DatabaseMetadata,
    FlavorStats,
    JobHistory,
)


class TestDataModels(unittest.TestCase):
    """Tests for data models."""

    def test_job_history_dataclass(self):
        """Test JobHistory dataclass."""
        history = JobHistory(
            job_name="test-job",
            flavor_name="test-flavor",
            elapsed_time=3600,
            normalized_time=1.5,
            timestamp=1234567890,
        )
        self.assertEqual(history.job_name, "test-job")
        self.assertEqual(history.flavor_name, "test-flavor")
        self.assertEqual(history.elapsed_time, 3600)
        self.assertEqual(history.normalized_time, 1.5)
        self.assertEqual(history.timestamp, 1234567890)

    def test_flavor_stats_dataclass(self):
        """Test FlavorStats dataclass."""
        stats = FlavorStats(
            flavor_name="test-flavor",
            time_norm=1.5,
            time_avg=3600,
            time_sum=10800,
            time_cnt=3,
            similar_jobs={"job1": JobHistory("job1", "test-flavor", 3600, 1.0, 1234567890)},
        )
        self.assertEqual(stats.flavor_name, "test-flavor")
        self.assertEqual(stats.time_avg, 3600)
        self.assertEqual(stats.time_cnt, 3)
        self.assertIn("job1", stats.similar_jobs)

    def test_database_metadata_dataclass(self):
        """Test DatabaseMetadata dataclass."""
        metadata = DatabaseMetadata(
            version="2.0.0",
            config_hash="abc123",
            update_time=1234567890,
        )
        self.assertEqual(metadata.version, "2.0.0")
        self.assertEqual(metadata.config_hash, "abc123")
        self.assertEqual(metadata.update_time, 1234567890)

    def test_database_content_dataclass(self):
        """Test DatabaseContent dataclass."""
        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "abc123", 1234567890),
            flavor_stats={
                "test-flavor": FlavorStats(
                    flavor_name="test-flavor",
                    time_norm=1.0,
                    time_avg=3600,
                    time_sum=3600,
                    time_cnt=1,
                )
            },
        )
        self.assertEqual(content.metadata.version, "2.0.0")
        self.assertIn("test-flavor", content.flavor_stats)


class TestDatabaseManager(unittest.TestCase):
    """Tests for DatabaseManager."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.database_file = str(Path(self.temp_dir) / "test_db.json")

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_nonexistent_file(self):
        """Test loading a non-existent database file."""
        manager = DatabaseManager("/nonexistent/path/db.json")
        data = manager.load()
        self.assertIsNone(data)

    def test_save_and_load_empty(self):
        """Test saving and loading an empty database."""
        manager = DatabaseManager(self.database_file)
        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={},
        )
        result = manager.save(content)
        self.assertTrue(result)

        loaded = manager.load()
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.metadata.version, "2.0.0")

    def test_update_flavor_time(self):
        """Test updating flavor time statistics."""
        manager = DatabaseManager(self.database_file)

        # First save some data
        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={},
        )
        manager.save(content)
        # Load to populate _data before update
        manager.load()

        # Update flavor time
        result = manager.update_flavor_time(
            flavor_name="test-flavor",
            elapsed_time=3600,
            normalized_time=1.5,
            job_name="test-job-1",
        )
        self.assertTrue(result)

        # Verify data was saved
        loaded = manager.load()
        self.assertIsNotNone(loaded)
        self.assertIn("test-flavor", loaded.flavor_stats)

        stats = loaded.flavor_stats["test-flavor"]
        self.assertEqual(stats.time_cnt, 1)
        self.assertEqual(stats.time_sum, 3600)

    def test_update_flavor_time_multiple_jobs(self):
        """Test updating flavor time with multiple jobs."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={},
        )
        manager.save(content)
        # Load to populate _data before update
        manager.load()

        # Add multiple jobs
        for i in range(3):
            manager.update_flavor_time(
                flavor_name="test-flavor",
                elapsed_time=3600,
                normalized_time=1.5,
                job_name=f"test-job-{i}",
            )

        loaded = manager.load()
        stats = loaded.flavor_stats["test-flavor"]
        self.assertEqual(stats.time_cnt, 3)
        self.assertEqual(stats.time_sum, 10800)
        self.assertEqual(stats.time_avg, 3600)
        self.assertEqual(len(stats.similar_jobs), 3)

    def test_get_flavor_stats(self):
        """Test getting flavor statistics."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={
                "test-flavor": FlavorStats(
                    flavor_name="test-flavor",
                    time_norm=1.5,
                    time_avg=3600,
                    time_sum=3600,
                    time_cnt=1,
                )
            },
        )
        manager.save(content)
        # Load to populate _data
        manager.load()

        stats = manager.get_flavor_stats("test-flavor")
        self.assertIsNotNone(stats)
        self.assertEqual(stats.time_avg, 3600)

        stats_missing = manager.get_flavor_stats("nonexistent")
        self.assertIsNone(stats_missing)

    def test_get_job_history_exact_match(self):
        """Test getting job history with exact match."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={
                "test-flavor": FlavorStats(
                    flavor_name="test-flavor",
                    time_norm=1.5,
                    time_avg=3600,
                    time_sum=3600,
                    time_cnt=1,
                    similar_jobs={
                        "test-job": JobHistory(
                            job_name="test-job",
                            flavor_name="test-flavor",
                            elapsed_time=3600,
                            normalized_time=1.5,
                            timestamp=1234567890,
                        )
                    },
                )
            },
        )
        manager.save(content)
        # Load to populate _data before get_job_history
        manager.load()

        history = manager.get_job_history("test-flavor", "test-job")
        self.assertIsNotNone(history)
        self.assertEqual(history.job_name, "test-job")

    def test_get_job_history_fuzzy_match(self):
        """Test getting job history with fuzzy match."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={
                "test-flavor": FlavorStats(
                    flavor_name="test-flavor",
                    time_norm=1.5,
                    time_avg=3600,
                    time_sum=3600,
                    time_cnt=1,
                    similar_jobs={
                        "my-test-job": JobHistory(
                            job_name="my-test-job",
                            flavor_name="test-flavor",
                            elapsed_time=3600,
                            normalized_time=1.5,
                            timestamp=1234567890,
                        )
                    },
                )
            },
        )
        manager.save(content)
        # Load to populate _data
        manager.load()

        # Fuzzy match should work (exact match first)
        history = manager.get_job_history("test-flavor", "my-test-job", match_threshold=0.5)
        self.assertIsNotNone(history)
        self.assertEqual(history.job_name, "my-test-job")

    def test_get_job_history_no_match(self):
        """Test getting job history when no match found."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={},
        )
        manager.save(content)
        # Load to populate _data
        manager.load()

        history = manager.get_job_history("test-flavor", "nonexistent-job")
        self.assertIsNone(history)

    def test_normalize_flavor_name(self):
        """Test flavor name normalization."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={},
        )
        manager.save(content)
        # Load to populate _data before update
        manager.load()

        # Update with ephemeral flavor
        manager.update_flavor_time(
            flavor_name="test-flavor ephemeral",
            elapsed_time=3600,
            normalized_time=1.5,
            job_name="test-job",
        )

        loaded = manager.load()
        # Should be stored without " ephemeral" suffix
        self.assertIn("test-flavor", loaded.flavor_stats)

    def test_reset_database(self):
        """Test resetting the database."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={
                "test-flavor": FlavorStats(
                    flavor_name="test-flavor",
                    time_norm=1.0,
                    time_avg=3600,
                    time_sum=3600,
                    time_cnt=1,
                )
            },
        )
        manager.save(content)
        # Load to populate _data before update
        manager.load()

        result = manager.reset()
        self.assertTrue(result)

        loaded = manager.load()
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.metadata.version, "")
        self.assertEqual(len(loaded.flavor_stats), 0)

    def test_update_metadata(self):
        """Test updating database metadata."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("1.0.0", "oldhash", 1234567890),
            flavor_stats={},
        )
        manager.save(content)
        # Load to populate _data before update
        manager.load()

        result = manager.update_metadata("2.0.0", "newhash")
        self.assertTrue(result)

        loaded = manager.load()
        self.assertEqual(loaded.metadata.version, "2.0.0")
        self.assertEqual(loaded.metadata.config_hash, "newhash")

    def test_get_version(self):
        """Test getting database version."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={},
        )
        manager.save(content)
        # Load to populate _data
        manager.load()

        self.assertEqual(manager.get_version(), "2.0.0")

    def test_get_update_time(self):
        """Test getting database update time."""
        manager = DatabaseManager(self.database_file)

        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={},
        )
        manager.save(content)
        # Load to populate _data
        manager.load()

        self.assertEqual(manager.get_update_time(), 1234567890)

    def test_load_corrupt_json(self):
        """Test loading a corrupt JSON file."""
        # Create a corrupt JSON file
        with open(self.database_file, "w") as f:
            f.write("{ invalid json }")

        manager = DatabaseManager(self.database_file)
        data = manager.load()
        self.assertIsNone(data)

    def test_update_flavor_time_without_load(self):
        """Test that update_flavor_time returns False when not loaded."""
        manager = DatabaseManager(self.database_file)
        # Don't load first, just save empty content
        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "hash123", 1234567890),
            flavor_stats={},
        )
        manager.save(content)
        # Don't call load() - update should fail
        result = manager.update_flavor_time(
            flavor_name="test-flavor",
            elapsed_time=3600,
            normalized_time=1.5,
            job_name="test-job",
        )
        self.assertFalse(result)

    def test_get_flavor_stats_without_load(self):
        """Test that get_flavor_stats returns None when not loaded."""
        manager = DatabaseManager(self.database_file)
        stats = manager.get_flavor_stats("test-flavor")
        self.assertIsNone(stats)

    def test_fuzzy_match_exact(self):
        """Test _fuzzy_match with exact match."""
        manager = DatabaseManager(self.database_file)
        self.assertTrue(manager._fuzzy_match("test", "test", 0.95))

    def test_fuzzy_match_similar(self):
        """Test _fuzzy_match with similar strings."""
        manager = DatabaseManager(self.database_file)
        # "test-job" and "test-job-1" share 8 chars out of 10 = 80%
        self.assertTrue(manager._fuzzy_match("test-job", "test-job-1", 0.75))

    def test_fuzzy_match_different(self):
        """Test _fuzzy_match with different strings."""
        manager = DatabaseManager(self.database_file)
        self.assertFalse(manager._fuzzy_match("test-job", "other-job", 0.95))

    def test_fuzzy_match_empty_strings(self):
        """Test _fuzzy_match with empty strings."""
        manager = DatabaseManager(self.database_file)
        self.assertTrue(manager._fuzzy_match("", "", 0.95))

    def test_normalize_flavor_name_ephemeral(self):
        """Test _normalize_flavor_name with ephemeral suffix."""
        manager = DatabaseManager(self.database_file)
        normalized = manager._normalize_flavor_name("test-flavor ephemeral")
        self.assertEqual(normalized, "test-flavor")

    def test_normalize_flavor_name_no_change(self):
        """Test _normalize_flavor_name without ephemeral suffix."""
        manager = DatabaseManager(self.database_file)
        normalized = manager._normalize_flavor_name("test-flavor")
        self.assertEqual(normalized, "test-flavor")

    def test_update_metadata_without_load(self):
        """Test that update_metadata returns False when not loaded."""
        manager = DatabaseManager(self.database_file)
        result = manager.update_metadata("2.0.0", "newhash")
        self.assertFalse(result)

    def test_reset_without_load(self):
        """Test that reset works when not loaded."""
        manager = DatabaseManager(self.database_file)
        result = manager.reset()
        self.assertTrue(result)
        # After reset, we should have empty data
        loaded = manager.load()
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.metadata.version, "")


class TestDatabaseIntegration(unittest.TestCase):
    """Integration tests for database operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.database_file = str(Path(self.temp_dir) / "test_db.json")

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_full_workflow(self):
        """Test complete workflow: save, load, update, verify."""
        manager = DatabaseManager(self.database_file)

        # Step 1: Create and save initial database
        content = DatabaseContent(
            metadata=DatabaseMetadata("2.0.0", "initial-hash", 1234567890),
            flavor_stats={},
        )
        self.assertTrue(manager.save(content))

        # Step 2: Load and verify (need to load to populate _data)
        loaded = manager.load()
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.metadata.version, "2.0.0")

        # Step 3: Update flavor stats (need to load before update)
        manager.load()
        self.assertTrue(manager.update_flavor_time(
            flavor_name="de.NBI small",
            elapsed_time=7200,
            normalized_time=1.2,
            job_name="job-abc",
        ))

        # Step 4: Verify update
        loaded = manager.load()
        stats = loaded.flavor_stats["de.NBI small"]
        self.assertEqual(stats.time_cnt, 1)
        self.assertEqual(stats.time_sum, 7200)

        # Step 5: Update again (need to load before update)
        manager.load()
        self.assertTrue(manager.update_flavor_time(
            flavor_name="de.NBI small",
            elapsed_time=3600,
            normalized_time=1.0,
            job_name="job-def",
        ))

        # Step 6: Verify aggregation
        loaded = manager.load()
        stats = loaded.flavor_stats["de.NBI small"]
        self.assertEqual(stats.time_cnt, 2)
        self.assertEqual(stats.time_sum, 10800)
        self.assertEqual(stats.time_avg, 5400)


if __name__ == "__main__":
    unittest.main()
