"""
Ansible integration for autoscaling.
Runs Ansible playbooks for cluster configuration.
"""
import os
import subprocess
from typing import Optional


class AnsibleRunner:
    """
    Runner for Ansible playbooks.
    """

    def __init__(
        self,
        playbook_dir: str = "~/playbook",
        ansible_hosts: str = "ansible_hosts",
        playbook: str = "site.yml",
    ):
        """
        Initialize the Ansible runner.

        Args:
            playbook_dir: Directory containing the playbook
            ansible_hosts: Path to the ansible hosts file
            playbook: Name of the playbook to run
        """
        self.playbook_dir = os.path.expanduser(playbook_dir)
        self.ansible_hosts = ansible_hosts
        self.playbook = playbook

    def run(self, verbose: bool = False) -> bool:
        """
        Run the Ansible playbook.

        Args:
            verbose: Enable verbose output

        Returns:
            True if playbook succeeded, False otherwise
        """
        playbook_path = os.path.join(self.playbook_dir, self.playbook)

        if not os.path.exists(playbook_path):
            print(f"Playbook not found: {playbook_path}")
            return False

        if not os.path.exists(os.path.join(self.playbook_dir, self.ansible_hosts)):
            print(f"Ansible hosts not found: {self.ansible_hosts}")
            return False

        cmd = ["ansible-playbook", "-i", self.ansible_hosts, self.playbook]

        if verbose:
            cmd.append("-v")

        # Calculate forks based on CPU count
        cpu_count = os.cpu_count() or 1
        forks_num = str(cpu_count * 4)
        cmd.extend(["--forks", forks_num])

        print(f"--- Running Ansible Playbook ---")
        print(f"Directory: {self.playbook_dir}")
        print(f"Command: {' '.join(cmd)}")

        try:
            # Change to playbook directory
            original_cwd = os.getcwd()
            os.chdir(self.playbook_dir)

            # Run the playbook
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )

            # Log output
            for line in process.stdout:
                print(f"[ANSIBLE] {line.strip()}")

            # Wait for completion
            return_code = process.wait()

            # Restore original directory
            os.chdir(original_cwd)

            if return_code == 0:
                print("Ansible playbook succeeded")
                return True
            else:
                print(f"Ansible playbook failed with return code: {return_code}")
                return False

        except FileNotFoundError:
            print("ansible-playbook not found in PATH")
            return False
        except Exception as e:
            print(f"Error running ansible-playbook: {e}")
            return False

    def set_directory(self, directory: str) -> None:
        """
        Set the playbook directory.

        Args:
            directory: Path to the playbook directory
        """
        self.playbook_dir = os.path.expanduser(directory)

    def set_hosts(self, hosts_file: str) -> None:
        """
        Set the ansible hosts file.

        Args:
            hosts_file: Path to the ansible hosts file
        """
        self.ansible_hosts = hosts_file

    def set_playbook(self, playbook: str) -> None:
        """
        Set the playbook name.

        Args:
            playbook: Name of the playbook
        """
        self.playbook = playbook


def run_ansible_playbook(
    playbook_dir: str = "~/playbook",
    verbose: bool = False,
) -> bool:
    """
    Convenience function to run the Ansible playbook.

    Args:
        playbook_dir: Directory containing the playbook
        verbose: Enable verbose output

    Returns:
        True if playbook succeeded, False otherwise
    """
    runner = AnsibleRunner(playbook_dir=playbook_dir)
    return runner.run(verbose=verbose)
