import unittest

from conductor.client import SingerConductor


TEST_CONFIG = 'tests/test_config.json'


tap_bin = "~/.virtualenvs/venv_tap/bin/tap-postgres",
tap_config_filepath = "tap/config.json"
tap_catalog_filepath = "raw_catalog.json"
selector_bin = "~/.virtualenvs/venv_selector/bin/selector-postgresql"
selector_config_filepath = "selector/config.json"
selector_catalog_filepath = "edited_catalog.json"
transformer_bin = "~/.virtualenvs/venv_transformer/bin/transform-field"
transformer_config_filepath = "transformer/config.json"
target_bin = "~/.virtualenvs/venv_target/bin/target-bigquery"
target_config_filepath = "target/config.json"
state_persistence_filepath = "state.json"


class TestSingerConductor(unittest.TestCase):

    def setUp(self):
        self.conductor = SingerConductor.load(TEST_CONFIG)

    def test_load(self):
        conductor = self.conductor

        # Check attributes
        tap_bin = "~/.virtualenvs/venv_tap/bin/tap-postgres"
        self.assertEqual(conductor.tap_bin, tap_bin)

        tap_config_filepath = "tap/config.json"
        self.assertEqual(
            conductor.tap_config_filepath,
            tap_config_filepath)

        tap_catalog_filepath = "raw_catalog.json"
        self.assertEqual(
            conductor.tap_catalog_filepath,
            tap_catalog_filepath)

        selector_bin = "~/.virtualenvs/venv_selector/bin/selector-postgresql"
        self.assertEqual(
            conductor.selector_bin,
            selector_bin)

        selector_config_filepath = "selector/config.json"
        self.assertEqual(
            conductor.selector_config_filepath,
            selector_config_filepath)

        selector_catalog_filepath = "edited_catalog.json"
        self.assertEqual(
            conductor.selector_catalog_filepath,
            selector_catalog_filepath)

        transformer_bin = "~/.virtualenvs/venv_transformer/bin/transform-field"
        self.assertEqual(
            conductor.transformer_bin,
            transformer_bin)

        transformer_config_filepath = "transformer/config.json"
        self.assertEqual(
            conductor.transformer_config_filepath,
            transformer_config_filepath)

        target_bin = "~/.virtualenvs/venv_target/bin/target-bigquery"
        self.assertEqual(
            conductor.target_bin,
            target_bin)

        target_config_filepath = "target/config.json"
        self.assertEqual(
            conductor.target_config_filepath,
            target_config_filepath)

        state_persistence_filepath = "state.json"
        self.assertEqual(
            conductor.state_persistence_filepath,
            state_persistence_filepath)

    def test_validate_configs(self):
        pass

    def test_validate_environments(self):
        pass

    def test_tap_discovery_command(self):
        expected_command = (
            '~/.virtualenvs/venv_tap/bin/tap-postgres '
            '--config tap/config.json --discover > raw_catalog.json')

        self.assertEqual(
            self.conductor.tap_discovery_command,
            expected_command)

    def test_tap_selector_command(self):
        expected_command = (
            '~/.virtualenvs/venv_selector/bin/selector-postgresql '
            '--selector selector/config.json '
            '--catalog raw_catalog.json > edited_catalog.json')

        self.assertEqual(
            self.conductor.tap_selector_command,
            expected_command)

    def test_tap_replication_command(self):
        expected_command = (
            '~/.virtualenvs/venv_tap/bin/tap-postgres '
            '--config tap/config.json --properties edited_catalog.json')

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
            '--config tap/config.json --properties edited_catalog.json '
            '| ~/.virtualenvs/venv_transformer/bin/transform-field '
            '--config transformer/config.json '
            '| ~/.virtualenvs/venv_target/bin/target-bigquery '
            '--config target/config.json > latest_state.json')

        self.assertEqual(
            self.conductor.replication_command,
            expected_command)
