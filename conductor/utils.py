import logging
import shlex
import json
from subprocess import Popen, PIPE

from conductor.exceptions import RunCommandException

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def write_json_to_new_file(previous_filepath, new_filepath):
    """
    Move JSON file to new JSON file.
    """
    with open(previous_filepath) as file:
        json_contents = json.load(file)

    with open(new_filepath, 'w') as file:
        json.dump(json_contents, file)


def is_json_file(filepath):
    """
    Detects if a string is a valid json or not
    """
    try:
        with open(filepath) as file:
            json.load(file)
    except Exception:
        return False
    return True


def run_command(command):
    """
    Runs a shell command with or without log file with STDOUT and STDERR
    """
    piped_command = f"/bin/bash -o pipefail -c '{command}'"
    LOGGER.debug('Running command %s', piped_command)

    # STDOUT and STDERR returned in an array once the command finished
    proc = Popen(shlex.split(piped_command), stdout=PIPE, stderr=PIPE)
    proc_tuple = proc.communicate()
    proc_rc = proc.returncode
    stdout = proc_tuple[0].decode('utf-8')
    stderr = proc_tuple[1].decode('utf-8')

    LOGGER.info(stderr)

    if proc_rc != 0:
        LOGGER.error(stderr)
        raise RunCommandException(f'Command failed. Return code: {proc_rc}')

    return [proc_rc, stdout, stderr]
