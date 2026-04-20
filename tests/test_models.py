"""Tests for data models."""

from autoscaling.data.models import ClusterWorker, Flavor, Job, Worker


class TestJob:
    """Tests for Job model."""

    def test_job_from_dict(self):
        """Test creating Job from dictionary."""
        data = {
            "jobid": 123,
            "jobname": "test_job",
            "state": 0,
            "state_str": "PENDING",
            "req_cpus": 4,
            "req_mem": 8192,
            "priority": 100,
            "temporary_disk": 1000,
        }
        job = Job.from_dict(123, data)
        assert job.jobid == 123
        assert job.jobname == "test_job"
        assert job.state == 0
        assert job.is_pending() is True
        assert job.is_running() is False
        assert job.is_finished() is False

    def test_job_running_state(self):
        """Test job in running state."""
        data = {
            "jobid": 456,
            "state": 1,
            "state_str": "RUNNING",
        }
        job = Job.from_dict(456, data)
        assert job.is_running() is True
        assert job.is_pending() is False

    def test_job_finished_state(self):
        """Test job in finished state."""
        data = {
            "jobid": 789,
            "state": 3,
            "state_str": "COMPLETED",
        }
        job = Job.from_dict(789, data)
        assert job.is_finished() is True


class TestWorker:
    """Tests for Worker model."""

    def test_worker_from_dict(self):
        """Test creating Worker from dictionary."""
        data = {
            "total_cpus": 28,
            "real_memory": 236000,
            "state": "MIX",
            "temporary_disk": 1000000,
        }
        worker = Worker.from_dict("worker-01", data)
        assert worker.hostname == "worker-01"
        assert worker.total_cpus == 28
        assert worker.real_memory == 236000
        assert worker.is_mix() is True
        assert worker.is_idle() is False

    def test_worker_allocated_state(self):
        """Test worker in allocated state."""
        data = {"state": "ALLOC"}
        worker = Worker.from_dict("worker-02", data)
        assert worker.is_allocated() is True

    def test_worker_drain_state(self):
        """Test worker in drain state."""
        data = {"state": "IDLE+DRAIN"}
        worker = Worker.from_dict("worker-03", data)
        assert worker.is_drain() is True

    def test_worker_in_use(self):
        """Test worker in use detection."""
        data = {"state": "MIX"}
        worker = Worker.from_dict("worker-04", data)
        assert worker.is_in_use() is True


class TestFlavor:
    """Tests for Flavor model."""

    def test_flavor_from_dict(self):
        """Test creating Flavor from dictionary."""
        data = {
            "flavor": {
                "name": "de.NBI large",
                "vcpus": 28,
                "ram": 65536,
                "ram_gib": 64,
                "ephemeral_disk": 100,
            },
            "available_memory": 58000,
            "usable_count": 10,
        }
        flavor = Flavor.from_dict(data)
        assert flavor.name == "de.NBI large"
        assert flavor.vcpus == 28
        assert flavor.ram == 65536
        assert flavor.ephemeral_disk == 100
        assert flavor.usable_count == 10

    def test_flavor_fits_job(self):
        """Test flavor job fitting."""
        data = {
            "flavor": {
                "name": "de.NBI large",
                "vcpus": 28,
                "ram": 65536,
                "ephemeral_disk": 100,
            },
            "available_memory": 58000,
        }
        flavor = Flavor.from_dict(data)

        # Job fits (disk=50 <= temporary_disk=100)
        assert flavor.fits_job(8, 16000, 50, False) is True
        # Job requires ephemeral but flavor doesn't have
        assert flavor.fits_job(8, 16000, 500, True) is False

    def test_flavor_ephemeral_required(self):
        """Test flavor with ephemeral requirement."""
        data = {
            "flavor": {
                "name": "de.NBI large ephemeral",
                "vcpus": 28,
                "ram": 65536,
                "ephemeral_disk": 0,  # No ephemeral
            },
            "available_memory": 58000,
        }
        flavor = Flavor.from_dict(data)
        # Job requires ephemeral but flavor doesn't have
        assert flavor.fits_job(8, 16000, 500, True) is False


class TestClusterWorker:
    """Tests for ClusterWorker model."""

    def test_cluster_worker_from_dict(self):
        """Test creating ClusterWorker from dictionary."""
        data = {
            "hostname": "worker-01",
            "flavor": {"name": "de.NBI large"},
            "status": "ACTIVE",
            "ip": "192.168.1.100",
        }
        worker = ClusterWorker.from_dict(data)
        assert worker.hostname == "worker-01"
        assert worker.status == "ACTIVE"
        assert worker.is_active() is True

    def test_cluster_worker_error_state(self):
        """Test cluster worker in error state."""
        data = {"status": "ERROR"}
        worker = ClusterWorker.from_dict(data)
        assert worker.is_error() is True

    def test_cluster_worker_failed_state(self):
        """Test cluster worker in failed state."""
        data = {"status": "CREATION_FAILED"}
        worker = ClusterWorker.from_dict(data)
        assert worker.is_error() is True
