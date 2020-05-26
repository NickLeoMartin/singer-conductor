"""
Co-ordinates tap-to-target replication.
"""
import os
import json
import logging

from conductor import utils
from conductor import state

HOME_ADDRESS = os.path.expanduser('~')

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class BaseConductor(object):

    def __init__(self,
                 tap_bin,
                 tap_config_filepath,
                 tap_catalog_filepath,
                 target_bin,
                 target_config_filepath,
                 selector_bin=None,
                 selector_config_filepath=None,
                 selector_catalog_filepath=None,
                 transformer_bin=None,
                 transformer_config_filepath=None,
                 state_persistence_filepath=None,
                 use_previous_state=True,
                 store_latest_state=True,
                 local_previous_state_filepath='previous_state.json',
                 local_latest_state_filepath='latest_state.json'):
        self.tap_bin = tap_bin
        self.tap_config_filepath = tap_config_filepath
        self.tap_catalog_filepath = tap_catalog_filepath
        self.selector_bin = selector_bin
        self.selector_config_filepath = selector_config_filepath
        self.selector_catalog_filepath = selector_catalog_filepath
        self.transformer_bin = transformer_bin
        self.transformer_config_filepath = transformer_config_filepath
        self.target_bin = target_bin
        self.target_config_filepath = target_config_filepath
        self.state_persistence_filepath = state_persistence_filepath
        self.use_previous_state = use_previous_state
        self.store_latest_state = store_latest_state
        self.local_previous_state_filepath = local_previous_state_filepath
        self.local_latest_state_filepath = local_latest_state_filepath

    @classmethod
    def load(cls, filepath):
        """Loads from file"""
        raise NotImplementedError()

    def validate_configs(self):
        """Checks if configs exist"""

        config_filepaths = [
            self.tap_config_filepath,
            self.selector_config_filepath,
            self.transformer_config_filepath,
            self.target_config_filepath
        ]

        # Ignore unused parameters
        filepaths = [f for f in config_filepaths if f]

        for filepath in filepaths:

            # Replace tilde with full home path
            filepath = filepath.replace('~', HOME_ADDRESS)

            # Load/download to validate
            config = state.SmartStorage(filepath).load()

            if not config:
                error_message = f'{filepath} is not a valid config path'
                LOGGER.error(error_message)
                raise ValueError(error_message)

        LOGGER.info('All configs exist')

    def validate_environments(self):
        """Checks if virtual environments exist"""

        environment_filepaths = [
            self.tap_bin,
            self.selector_bin,
            self.transformer_bin,
            self.target_bin
        ]

        # Ignore unused parameters
        filepaths = [f for f in environment_filepaths if f]

        for filepath in filepaths:

            # Replace tilde with full home path
            filepath = filepath.replace('~', HOME_ADDRESS)

            # Check is environment exists at path
            if not os.path.isfile(filepath):
                error_message = f'{filepath} cannot be found'
                LOGGER.error(error_message)
                raise FileNotFoundError(error_message)

        LOGGER.info('All environments exist')

    def discover(self):
        """Runs discovery mode"""
        raise NotImplementedError()

    def select(self):
        """Runs selection mode"""
        raise NotImplementedError()

    def replicate(self):
        """Runs replication mode"""
        raise NotImplementedError()

    @property
    def state(self):
        """Returns state if it exists"""
        raise NotImplementedError()


class SingerConductor(BaseConductor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def load(cls, filepath):
        """Loads from config file"""
        LOGGER.info(f'Loading conductor config: {filepath}')

        if not os.path.isfile(filepath):
            error_message = f'{filepath} cannot be found'
            LOGGER.error(error_message)
            raise FileNotFoundError(error_message)

        with open(filepath) as file:
            config = json.loads(file.read())
        return SingerConductor(**config)

    @property
    def tap_discovery_command(self):
        """Full command to run tap in discovery mode"""
        return ' '.join([
            f'{self.tap_bin}',
            f'--config {self.tap_config_filepath}',
            '--discover',
            f'> {self.tap_catalog_filepath}'
        ])

    @property
    def tap_selector_command(self):
        """Full command to run stream & field selector post-discovery mode"""
        return ' '.join([
            f'{self.selector_bin}',
            f'--config {self.selector_config_filepath}',
            f'--catalog {self.tap_catalog_filepath}',
            f'> {self.selector_catalog_filepath}'
        ])

    @property
    def tap_replication_command(self):
        """Piecewise command for data extraction"""
        catalog = self.tap_catalog_filepath

        if self.selector_catalog_filepath:
            catalog = self.selector_catalog_filepath

        # Required
        commands = [
            f'{self.tap_bin}',
            f'--config {self.tap_config_filepath}',
            f'--catalog {catalog}'
        ]

        # Optional
        if self.use_previous_state:
            state_arg = f'--state {self.local_previous_state_filepath}'
            commands.append(state_arg)

        return ' '.join(commands)

    @property
    def transformer_replication_command(self):
        """Piecewise command for field transformation"""
        return ' '.join([
            f'{self.transformer_bin}',
            f'--config {self.transformer_config_filepath}'
        ])

    @property
    def target_replication_command(self):
        """Piecewise command for data loading"""
        return ' '.join([
            f'{self.target_bin}',
            f'--config {self.target_config_filepath}'
        ])

    @property
    def replication_command(self):
        """Full command for end-to-end replication"""
        commands = []

        # Required
        tap_command = f'  {self.tap_replication_command}'
        commands.append(tap_command)

        # Optional
        if self.transformer_bin:
            transformer_command = f'| {self.transformer_replication_command}'
            commands.append(transformer_command)

        # Required
        target_command = f'| {self.target_replication_command}'
        commands.append(target_command)

        # Optional
        if self.store_latest_state:
            state_command = f'> {self.local_latest_state_filepath}'
            commands.append(state_command)

        return ' '.join(commands)

    def discover(self):
        """Runs discovery mode"""
        LOGGER.info('Discovering...')

        LOGGER.info(f'Executing: {self.tap_discovery_command}')
        _, _, stderr = utils.run_command(
            command=self.tap_discovery_command)

        LOGGER.info('Completed discovery')

    def select(self):
        """Runs selection mode"""
        LOGGER.info('Selecting...')

        LOGGER.info(f'Executing: {self.tap_selector_command}')
        _, _, stderr = utils.run_command(
            command=self.tap_selector_command)

        LOGGER.info('Completed selection')

    def replicate(self):
        """Runs replication mode"""
        LOGGER.info('Replicating...')

        # Load local or external state file
        if self.use_previous_state:
            state_storage = state.SmartStorage(self.state_persistence_filepath)
            previous_state = state_storage.load()

            LOGGER.info(
                f'Obtaining previous state from storage: {previous_state}')
            state_storage.dump(file_contents=previous_state,
                               filepath=self.local_previous_state_filepath)

        # Execute replication
        LOGGER.info(f'Executing: {self.replication_command}')
        _, _, stderr = utils.run_command(
            command=self.replication_command)

        # Persist local or external state file
        if self.store_latest_state:
            state_storage = state.SmartStorage(
                self.local_latest_state_filepath)
            latest_state = state_storage.load()

            LOGGER.info(
                f'Updating state storage with latest state: {latest_state}')
            state_storage.update(file_contents=latest_state,
                                 filepath=self.state_persistence_filepath)

        LOGGER.info('Completed replication')
