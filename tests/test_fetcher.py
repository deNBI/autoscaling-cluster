"""Tests for data fetcher."""

from unittest.mock import MagicMock

from autoscaling.data.fetcher import DataFetcher


class TestDataFetcher:
    """Tests for DataFetcher class."""

    def test_fetcher_init(self):
        """Test DataFetcher initialization."""
        mock_scheduler = MagicMock()
        fetcher = DataFetcher(mock_scheduler)
        assert fetcher.scheduler == mock_scheduler
        assert fetcher.ignore_workers == []

    def test_set_ignore_workers(self):
        """Test setting ignored workers."""
        mock_scheduler = MagicMock()
        fetcher = DataFetcher(mock_scheduler)
        fetcher.set_ignore_workers(["worker-01", "worker-02"])
        assert fetcher.ignore_workers == ["worker-01", "worker-02"]

    def test_fetch_job_data(self):
        """Test fetching job data."""
        mock_scheduler = MagicMock()
        mock_scheduler.job_data_live.return_value = {
            1: {
                "jobid": 1,
                "jobname": "job1",
                "state": 0,
                "state_str": "PENDING",
                "req_cpus": 4,
                "req_mem": 8192,
            },
            2: {
                "jobid": 2,
                "jobname": "job2",
                "state": 1,
                "state_str": "RUNNING",
                "req_cpus": 8,
                "req_mem": 16384,
            },
        }

        fetcher = DataFetcher(mock_scheduler)
        pending, running = fetcher.fetch_job_data()

        assert len(pending) == 1
        assert len(running) == 1
        assert 1 in pending
        assert 2 in running
        assert pending[1].is_pending() is True
        assert running[2].is_running() is True

    def test_fetch_completed_jobs(self):
        """Test fetching completed jobs."""
        mock_scheduler = MagicMock()
        mock_scheduler.fetch_scheduler_job_data.return_value = {
            1: {
                "state": 3,
                "end": 1234567890,
                "end": 1234567890,
                "elapsed": 100,
                "jobname": "job1",
            },
            2: {
                "state": 0,
                "end": 1234567890,
                "elapsed": 0,
                "jobname": "job2",
            },  # Not finished
            3: {"state": 3, "end": 1234567890, "elapsed": 200, "jobname": "job3"},
        }

        fetcher = DataFetcher(mock_scheduler)
        completed = fetcher.fetch_completed_jobs(7)

        assert len(completed) == 2
        assert 1 in completed
        assert 3 in completed
        assert completed[1].is_finished() is True
