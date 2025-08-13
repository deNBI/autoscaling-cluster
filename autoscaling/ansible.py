import os
import subprocess

from constants import PLAYBOOK_DIR
from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


def run_ansible_playbook():
    """
    Run ansible playbook with system call.
        - ansible-playbook -v -i ansible_hosts site.yml
    :return: boolean, success
    """
    logger.debug("--- run playbook ---")
    os.chdir(PLAYBOOK_DIR)
    forks_num = str(os.cpu_count() * 4)
    result_ = False

    try:
        # Run the subprocess and capture its output
        process = subprocess.Popen(
            [
                "ansible-playbook",
                "--forks",
                forks_num,
                "-v",
                "-i",
                "ansible_hosts",
                "site.yml",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,  # To work with text output
        )

        # Log the output with the [ANSIBLE] prefix
        for line in process.stdout:
            logger.debug("[ANSIBLE] " + line.strip())

        # Wait for the subprocess to complete and get the return code
        return_code = process.wait()

        if return_code == 0:
            logger.debug("ansible playbook success")
            result_ = True
        else:
            logger.error(
                "ansible playbook failed with return code: " + str(return_code)
            )
    except Exception as e:
        logger.error("Error running ansible-playbook: " + str(e))

    return result_
