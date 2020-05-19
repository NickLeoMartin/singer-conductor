from conductor.utils import (
    is_json_file,
    run_command)

TEST_CONFIG = 'tests/test_config.json'


def test_is_json_file():
    assert is_json_file(TEST_CONFIG)


def test_run_command():
    command = 'echo "testing"'
    proc_rc, stdout, stderr = run_command(command)

    assert proc_rc == 0
    assert stdout == 'testing\n'
    assert stderr == ''
