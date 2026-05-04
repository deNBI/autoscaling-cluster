"""
Unit tests for CLI command handlers.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock, call
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from autoscaling.cli.commands import (
    _init_scheduler,
    _load_config,
    cmd_scale_up,
    cmd_scale_up_specific,
    cmd_scale_up_choice,
    cmd_scale_down,
    cmd_scale_down_specific,
    cmd_scale_down_choice,
    cmd_scale_down_batch,
    cmd_show_nodes,
    cmd_show_flavors,
    cmd_change_mode,
    cmd_ignore_workers,
    cmd_drain_workers,
    cmd_run_playbook,
)


class TestCommandHelpers(unittest.TestCase):
    """Tests for helper functions used by commands."""

    @patch('autoscaling.cli.commands.ConfigLoader')
    def test_load_config_success(self, mock_config_loader):
        """Test successful config loading."""
        mock_logger = Mock()
        mock_loader = Mock()
        mock_loader.load.return_value = {"scaling": {}}
        mock_config_loader.return_value = mock_loader

        result = _load_config(mock_logger)

        self.assertEqual(result, {"scaling": {}})
        mock_config_loader.assert_called_once()

    @patch('autoscaling.cli.commands.ConfigLoader')
    def test_load_config_failure(self, mock_config_loader):
        """Test config loading failure."""
        mock_logger = Mock()
        mock_config_loader.side_effect = Exception("File not found")

        result = _load_config(mock_logger)

        self.assertEqual(result, {})
        mock_logger.error.assert_called()

    @patch('autoscaling.cli.commands.ConfigLoader')
    @patch('autoscaling.cli.commands.SlurmScheduler')
    def test_init_scheduler_success(self, mock_slurm, mock_config_loader):
        """Test successful scheduler initialization."""
        mock_logger = Mock()
        mock_config_loader.return_value.load.return_value = {
            "scaling": {"scheduler": "slurm"}
        }
        mock_scheduler = Mock()
        mock_slurm.return_value = mock_scheduler
        mock_scheduler.test_connection.return_value = True

        result = _init_scheduler(mock_logger)

        self.assertEqual(result, mock_scheduler)
        mock_scheduler.set_logger.assert_called_once()
        mock_scheduler.test_connection.assert_called_once()

    @patch('autoscaling.cli.commands.ConfigLoader')
    @patch('autoscaling.cli.commands.SlurmScheduler')
    def test_init_scheduler_connection_failed(self, mock_slurm, mock_config_loader):
        """Test scheduler initialization with failed connection."""
        mock_logger = Mock()
        mock_config_loader.return_value.load.return_value = {
            "scaling": {"scheduler": "slurm"}
        }
        mock_scheduler = Mock()
        mock_slurm.return_value = mock_scheduler
        mock_scheduler.test_connection.return_value = False

        result = _init_scheduler(mock_logger)

        self.assertIsNone(result)
        mock_logger.error.assert_called_with("Scheduler connection failed")

    @patch('autoscaling.cli.commands.ConfigLoader')
    @patch('autoscaling.cli.commands.SlurmScheduler')
    def test_init_scheduler_unsupported(self, mock_slurm, mock_config_loader):
        """Test scheduler initialization with unsupported scheduler."""
        mock_logger = Mock()
        mock_config_loader.return_value.load.return_value = {
            "scaling": {"scheduler": "pbs"}
        }

        result = _init_scheduler(mock_logger)

        self.assertIsNone(result)
        mock_logger.error.assert_called_with("Unsupported scheduler")


class TestScaleUpCommands(unittest.TestCase):
    """Tests for scale up command handlers."""

    @patch('autoscaling.cli.commands._load_config')
    def test_cmd_scale_up_missing_config(self, mock_load_config):
        """Test scale up with missing scaling config."""
        mock_logger = Mock()
        mock_load_config.return_value = {"scaling": {}}

        result = cmd_scale_up(2, mock_logger)

        self.assertEqual(result, 1)
        mock_logger.error.assert_called_with(
            "Scaling link or cluster ID not configured"
        )

    @patch('autoscaling.cli.commands._load_config')
    def test_cmd_scale_up_choice_no_pending_jobs(self, mock_load_config):
        """Test scale up choice with no pending jobs."""
        mock_logger = Mock()
        mock_load_config.return_value = {
            "scaling": {
                "portal_scaling_link": "http://test",
                "cluster_id": "test-cluster",
                "password_file": "pw.json"
            }
        }

        with patch('autoscaling.cli.commands.receive_node_data_live') as mock_node, \
             patch('autoscaling.cli.commands.receive_job_data') as mock_jobs, \
             patch('autoscaling.cli.commands._init_scheduler') as mock_scheduler:

            mock_scheduler.return_value = Mock()
            mock_node.return_value = {}
            mock_jobs.return_value = ([], [])

            result = cmd_scale_up_choice(mock_logger)

            self.assertEqual(result, 0)
            mock_logger.info.assert_any_call("No pending jobs or no node data")


class TestScaleDownCommands(unittest.TestCase):
    """Tests for scale down command handlers."""

    @patch('autoscaling.cli.commands._load_config')
    def test_cmd_scale_down_no_idle_workers(self, mock_load_config):
        """Test scale down with no idle workers."""
        mock_logger = Mock()
        mock_load_config.return_value = {
            "scaling": {
                "portal_scaling_link": "http://test",
                "cluster_id": "test-cluster",
                "password_file": "pw.json"
            }
        }

        with patch('autoscaling.cli.commands.receive_node_data_live') as mock_node, \
             patch('autoscaling.cli.commands._init_scheduler') as mock_scheduler:

            mock_scheduler.return_value = Mock()
            mock_node.return_value = {
                "worker-1": Mock(state="BUSY"),
                "worker-2": Mock(state="RESERVED")
            }

            result = cmd_scale_down(mock_logger)

            self.assertEqual(result, 0)
            mock_logger.info.assert_any_call("No idle workers to scale down")

    @patch('autoscaling.cli.commands._load_config')
    def test_cmd_scale_down_specific_success(self, mock_load_config):
        """Test successful scale down specific workers command."""
        mock_logger = Mock()
        mock_load_config.return_value = {
            "scaling": {
                "portal_scaling_link": "http://test",
                "cluster_id": "test-cluster",
                "password_file": "pw.json"
            }
        }

        # Mock ClusterAPI at the point of import inside the function
        with patch('autoscaling.cluster.api.ClusterAPI') as mock_cluster_api:
            mock_api_instance = Mock()
            mock_cluster_api.return_value = mock_api_instance
            mock_api_instance.scale_down.return_value = True

            result = cmd_scale_down_specific("worker-1, worker-2", mock_logger)

            self.assertEqual(result, 0)
            mock_api_instance.scale_down.assert_called_once_with(["worker-1", "worker-2"])


class TestShowCommands(unittest.TestCase):
    """Tests for show information commands."""

    def test_cmd_show_nodes(self):
        """Test show nodes command."""
        mock_scheduler = Mock()
        mock_logger = Mock()

        with patch('autoscaling.cli.commands.receive_node_data_live') as mock_node:
            mock_node.return_value = {"worker-1": Mock()}

            result = cmd_show_nodes(mock_scheduler, mock_logger)

            self.assertEqual(result, 0)
            mock_node.assert_called_once_with(mock_scheduler)


class TestModeAndWorkerCommands(unittest.TestCase):
    """Tests for mode and worker management commands."""

    @patch('autoscaling.cli.commands.ConfigLoader')
    def test_cmd_change_mode(self, mock_config_loader):
        """Test change mode command."""
        mock_logger = Mock()
        mock_loader = Mock()
        mock_config_loader.return_value = mock_loader

        result = cmd_change_mode({"scaling": {}}, mock_logger)

        self.assertEqual(result, 0)

    @patch('autoscaling.cli.commands.ConfigLoader')
    def test_cmd_ignore_workers(self, mock_config_loader):
        """Test ignore workers command."""
        mock_logger = Mock()
        mock_loader = Mock()
        mock_config_loader.return_value = mock_loader

        result = cmd_ignore_workers({"scaling": {}}, mock_logger)

        self.assertEqual(result, 0)

    @patch('autoscaling.cli.commands.receive_node_data_live')
    def test_cmd_drain_workers_success(self, mock_receive_node):
        """Test drain workers command with successful draining."""
        mock_scheduler = Mock()
        mock_logger = Mock()
        mock_receive_node.return_value = {
            "worker-1": Mock(state="IDLE"),
            "worker-2": Mock(state="IDLE")
        }
        mock_scheduler.drain_node.return_value = True

        result = cmd_drain_workers(mock_scheduler, mock_logger)

        self.assertEqual(result, 0)
        mock_scheduler.drain_node.assert_any_call("worker-1")
        mock_scheduler.drain_node.assert_any_call("worker-2")
        mock_logger.info.assert_any_call("Successfully drained 2 workers")

    @patch('autoscaling.cli.commands.receive_node_data_live')
    def test_cmd_drain_workers_no_idle(self, mock_receive_node):
        """Test drain workers command with no idle workers."""
        mock_scheduler = Mock()
        mock_logger = Mock()
        mock_receive_node.return_value = {
            "worker-1": Mock(state="BUSY")
        }

        result = cmd_drain_workers(mock_scheduler, mock_logger)

        self.assertEqual(result, 0)
        mock_logger.info.assert_any_call("No idle workers to drain")

    @patch('autoscaling.cli.commands.receive_node_data_live')
    def test_cmd_drain_workers_failure(self, mock_receive_node):
        """Test drain workers command with draining failure."""
        mock_scheduler = Mock()
        mock_logger = Mock()
        mock_receive_node.return_value = {
            "worker-1": Mock(state="IDLE")
        }
        mock_scheduler.drain_node.return_value = False

        result = cmd_drain_workers(mock_scheduler, mock_logger)

        self.assertEqual(result, 1)
        mock_logger.error.assert_any_call("Failed to drain worker: worker-1")


class TestPlaybookCommand(unittest.TestCase):
    """Tests for playbook command."""

    @patch('autoscaling.cloud.ansible.AnsibleRunner')
    def test_cmd_run_playbook_success(self, mock_ansible):
        """Test successful playbook run."""
        mock_logger = Mock()

        mock_runner = Mock()
        mock_ansible.return_value = mock_runner
        mock_runner.run.return_value = True  # run() returns boolean, not tuple

        result = cmd_run_playbook({}, mock_logger)

        self.assertEqual(result, 0)
        mock_runner.run.assert_called_once()

    @patch('autoscaling.cli.commands.AnsibleRunner')
    def test_cmd_run_playbook_failure(self, mock_ansible):
        """Test playbook run failure."""
        mock_logger = Mock()

        mock_runner = Mock()
        mock_ansible.return_value = mock_runner
        mock_runner.run.return_value = False  # run() returns boolean, not tuple

        result = cmd_run_playbook({}, mock_logger)

        self.assertEqual(result, 1)
        mock_logger.error.assert_called_with("Ansible playbook failed")


class TestCommandLineIntegration(unittest.TestCase):
    """Tests for the main command routing."""

    @patch('autoscaling.cli.commands._init_scheduler')
    @patch('autoscaling.cli.commands._load_config')
    @patch('autoscaling.cli.commands.setup_logger')
    def test_run_command_scale_up(self, mock_setup_logger, mock_load_config, mock_init_scheduler):
        """Test command routing for scale up."""
        mock_logger = Mock()
        mock_load_config.return_value = {"scaling": {}}
        mock_init_scheduler.return_value = Mock()

        args = Mock()
        args.scaleup = 2
        args.version = False
        args.scaleupspecific = None
        args.scaleupchoice = False
        args.scaledown = False
        args.scaledownspecific = None
        args.scaledownchoice = False
        args.scaledownbatch = False
        args.node = False
        args.jobdata = False
        args.jobhistory = False
        args.flavor = False
        args.clusterdata = False
        args.mode = False
        args.ignore = False
        args.rescale = False
        args.drain = False
        args.playbook = False
        args.checkworker = False
        args.test = False
        args.clean = False
        args.reset = False
        args.update = False
        args.stop = False
        args.kill = False
        args.status = False
        args.visual = None
        args.password = None

        with patch('autoscaling.cli.commands.cmd_scale_up') as mock_cmd:
            mock_cmd.return_value = 0
            from autoscaling.cli.commands import run_command
            result = run_command(args, Mock())
            self.assertEqual(result, 0)
            # The logger passed is the one from setup_logger, not our mock
            actual_call = mock_cmd.call_args
            self.assertEqual(actual_call[0][0], 2)  # count argument
            self.assertIsNotNone(actual_call[0][1])  # logger argument

    @patch('autoscaling.cli.commands._init_scheduler')
    @patch('autoscaling.cli.commands._load_config')
    @patch('autoscaling.cli.commands.setup_logger')
    def test_run_command_default_service(self, mock_setup_logger, mock_load_config, mock_init_scheduler):
        """Test default command (run service)."""
        mock_logger = Mock()
        mock_load_config.return_value = {"scaling": {}}
        mock_init_scheduler.return_value = Mock()

        args = Mock()
        args.scaleup = None
        args.version = False
        args.scaleupspecific = None
        args.scaleupchoice = False
        args.scaledown = False
        args.scaledownspecific = None
        args.scaledownchoice = False
        args.scaledownbatch = False
        args.node = False
        args.jobdata = False
        args.jobhistory = False
        args.flavor = False
        args.clusterdata = False
        args.mode = False
        args.ignore = False
        args.rescale = False
        args.drain = False
        args.playbook = False
        args.checkworker = False
        args.test = False
        args.clean = False
        args.reset = False
        args.update = False
        args.stop = False
        args.kill = False
        args.status = False
        args.visual = None
        args.password = None

        with patch('autoscaling.cli.commands.cmd_run_service') as mock_cmd:
            mock_cmd.return_value = 0
            from autoscaling.cli.commands import run_command
            result = run_command(args, Mock())
            self.assertEqual(result, 0)
            mock_cmd.assert_called_once()


if __name__ == '__main__':
    unittest.main()
