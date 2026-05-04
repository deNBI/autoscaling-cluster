"""
Unit tests for CLI argument parser.
"""
import unittest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from autoscaling.cli.parser import create_argument_parser


class TestArgumentParser(unittest.TestCase):
    """Tests for the argument parser."""

    def setUp(self):
        """Set up test fixtures."""
        self.parser = create_argument_parser()

    def test_parser_exists(self):
        """Test that parser is created successfully."""
        self.assertIsNotNone(self.parser)

    def test_parser_has_expected_arguments(self):
        """Test that parser has all expected arguments."""
        # Get all option strings from parser
        option_strings = []
        for action in self.parser._actions:
            option_strings.extend(action.option_strings)

        # Test some key arguments exist (double-check that argparse parsed them correctly)
        # Note: Some args like -clean, -reset, -status only have single dash versions
        key_args = [
            '-v', '--version',
            '-s', '--service',
            '-su', '--scaleup',
            '-sd', '--scaledown',
            '-nd', '--node',
            '-j', '--jobdata',
            '-fv', '--flavor',
            '-m', '--mode',
            '-d', '--drain',
            '-pb', '--playbook',
            '-t', '--test',
            '-password', '-p', '--password',
        ]

        for arg in key_args:
            self.assertIn(arg, option_strings, f"Argument '{arg}' not found in parser")

    def test_version_flag(self):
        """Test version flag parsing."""
        args = self.parser.parse_args(['-v'])
        self.assertTrue(args.version)

    def test_service_flag(self):
        """Test service flag parsing."""
        args = self.parser.parse_args(['-s'])
        self.assertTrue(args.service)

    def test_scaleup_flag(self):
        """Test scaleup flag parsing."""
        args = self.parser.parse_args(['-su', '5'])
        self.assertEqual(args.scaleup, 5)

    def test_scaleup_specific_flag(self):
        """Test scaleup specific flag parsing."""
        args = self.parser.parse_args(['-sus', 'large', '3'])
        self.assertEqual(args.scaleupspecific, ['large', '3'])

    def test_scaledown_specific_flag(self):
        """Test scaledown specific flag parsing."""
        args = self.parser.parse_args(['-sds', 'worker-1,worker-2'])
        self.assertEqual(args.scaledownspecific, 'worker-1,worker-2')

    def test_show_nodes_flag(self):
        """Test show nodes flag parsing."""
        args = self.parser.parse_args(['-nd'])
        self.assertTrue(args.node)

    def test_show_jobs_flag(self):
        """Test show jobs flag parsing."""
        args = self.parser.parse_args(['-j'])
        self.assertTrue(args.jobdata)

    def test_show_flavors_flag(self):
        """Test show flavors flag parsing."""
        args = self.parser.parse_args(['-fv'])
        self.assertTrue(args.flavor)

    def test_change_mode_flag(self):
        """Test change mode flag parsing."""
        args = self.parser.parse_args(['-m'])
        self.assertTrue(args.mode)

    def test_drain_workers_flag(self):
        """Test drain workers flag parsing."""
        args = self.parser.parse_args(['-d'])
        self.assertTrue(args.drain)

    def test_run_playbook_flag(self):
        """Test run playbook flag parsing."""
        args = self.parser.parse_args(['-pb'])
        self.assertTrue(args.playbook)

    def test_test_mode_flag(self):
        """Test test mode flag parsing."""
        args = self.parser.parse_args(['-t'])
        self.assertTrue(args.test)

    def test_visualize_flag_with_argument(self):
        """Test visualize flag with argument."""
        args = self.parser.parse_args(['-visual', '7d'])
        self.assertEqual(args.visual, '7d')

    def test_visualize_flag_without_argument(self):
        """Test visualize flag without argument."""
        args = self.parser.parse_args(['-visual'])
        self.assertEqual(args.visual, '')

    def test_all_flags_together(self):
        """Test parsing multiple flags together."""
        args = self.parser.parse_args([
            '-su', '2',
            '-j',
            '-fv',
            '-m'
        ])
        self.assertEqual(args.scaleup, 2)
        self.assertTrue(args.jobdata)
        self.assertTrue(args.flavor)
        self.assertTrue(args.mode)

    def test_default_service_mode(self):
        """Test that default mode is service (all flags None)."""
        args = self.parser.parse_args([])
        self.assertIsNone(args.scaleup)
        self.assertIsNone(args.scaleupspecific)
        self.assertFalse(args.service)
        self.assertFalse(args.test)


class TestParserHelp(unittest.TestCase):
    """Tests for parser help and documentation."""

    def setUp(self):
        """Set up test fixtures."""
        self.parser = create_argument_parser()

    def test_parser_has_description(self):
        """Test that parser has a description."""
        self.assertIn("Autoscaling", self.parser.description)

    def test_parser_has_examples(self):
        """Test that parser help contains examples."""
        help_text = self.parser.format_help()
        self.assertIn("Examples:", help_text)
        self.assertIn("autoscaling -su 2", help_text)

    def test_parser_has_modes_description(self):
        """Test that parser help contains mode descriptions."""
        help_text = self.parser.format_help()
        self.assertIn("Modes:", help_text)
        self.assertIn("basic", help_text)


if __name__ == '__main__':
    unittest.main()
