import unittest

from conductor.client import SingerConductor


TEST_CONFIG = 'tests/test_config.json'

tap_bin = "~/.virtualenvs/venv_tap/bin/tap-postgres"
tap_config_filepath = "tap/config.json"
tap_catalog_filepath = "catalog.json"
selector_bin = "~/.virtualenvs/venv_selector/bin/selector-postgresql"
selector_config_filepath = "selector/config.json"
transformer_bin = "~/.virtualenvs/venv_transformer/bin/transform-field"
transformer_config_filepath = "transformer/config.json"
target_bin = "~/.virtualenvs/venv_target/bin/target-bigquery"
target_config_filepath = "target/config.json"
state_persistence_filepath = "state.json"
use_previous_state = True
store_latest_state = True


class TestSingerConductor(unittest.TestCase):

    def setUp(self):
        self.conductor = SingerConductor.load(TEST_CONFIG)

    def test_load(self):
        conductor = self.conductor

        # Check attributes
        self.assertEqual(conductor.tap_bin, tap_bin)

        self.assertEqual(
            conductor.tap_config_filepath,
            tap_config_filepath)

        self.assertEqual(
            conductor.tap_catalog_filepath,
            tap_catalog_filepath)

        self.assertEqual(
            conductor.selector_bin,
            selector_bin)

        self.assertEqual(
            conductor.selector_config_filepath,
            selector_config_filepath)

        self.assertEqual(
            conductor.transformer_bin,
            transformer_bin)

        self.assertEqual(
            conductor.transformer_config_filepath,
            transformer_config_filepath)

        self.assertEqual(
            conductor.target_bin,
            target_bin)

        self.assertEqual(
            conductor.target_config_filepath,
            target_config_filepath)

        self.assertEqual(
            conductor.state_persistence_filepath,
            state_persistence_filepath)

        self.assertEqual(
            conductor.use_previous_state,
            use_previous_state)

        self.assertEqual(
            conductor.store_latest_state,
            store_latest_state)

    def test_validate_configs(self):
        pass

    def test_validate_environments(self):
        pass

    def test_tap_discovery_command(self):
        expected_command = (
            '~/.virtualenvs/venv_tap/bin/tap-postgres '
            '--config tap/config.json --discover > temp_catalog.json')

        self.assertEqual(
            self.conductor.tap_discovery_command,
            expected_command)

    def test_tap_selector_command(self):
        expected_command = (
            '~/.virtualenvs/venv_selector/bin/selector-postgresql '
            '--config selector/config.json '
            '--catalog catalog.json > temp_catalog.json')

        self.assertEqual(
            self.conductor.tap_selector_command,
            expected_command)

    def test_tap_replication_command(self):
        expected_command = (
            '~/.virtualenvs/venv_tap/bin/tap-postgres '
            '--config tap/config.json --catalog catalog.json '
            '--state previous_state.json')

        self.assertEqual(
            self.conductor.tap_replication_command,
            expected_command)

    def test_tap_replication_command_use_properties_flag(self):
        expected_command = (
            '~/.virtualenvs/venv_tap/bin/tap-postgres '
            '--config tap/config.json --properties catalog.json '
            '--state previous_state.json')

        self.conductor.use_properties_flag_for_tap = True

        self.assertEqual(
            self.conductor.tap_replication_command,
            expected_command)

    def test_transformer_replication_command(self):
        expected_command = (
            '~/.virtualenvs/venv_transformer/bin/transform-field '
            '--config transformer/config.json')

        self.assertEqual(
            self.conductor.transformer_replication_command,
            expected_command)

    def test_replication_command(self):
        expected_command = (
            '  ~/.virtualenvs/venv_tap/bin/tap-postgres '
            '--config tap/config.json --catalog catalog.json '
            '--state previous_state.json '
            '| ~/.virtualenvs/venv_transformer/bin/transform-field '
            '--config transformer/config.json '
            '| ~/.virtualenvs/venv_target/bin/target-bigquery '
            '--config target/config.json > latest_state.json')

        self.assertEqual(
            self.conductor.replication_command,
            expected_command)
