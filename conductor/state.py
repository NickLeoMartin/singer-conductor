import json
import logging

from smart_open import smart_open

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class BaseStorage(object):

    def __init__(self, file_contents=None):
        if not file_contents:
            self.file_contents = file_contents

    def load(self):
        raise NotImplementedError()

    def dump(self, file_contents, filepath=None):
        raise NotImplementedError()

    def update(self, file_contents, filepath=None):
        self.dump(file_contents, filepath)

    def close(self):
        pass


class SmartStorage(BaseStorage):

    def __init__(self, filepath, *args, **kwargs):
        self.filepath = filepath
        super(SmartStorage, self).__init__(*args, **kwargs)

    def load(self):
        try:
            with smart_open(self.filepath, 'r') as file:
                self.file_contents = json.load(file)
        except ValueError:
            self.file_contents = None
        return self.file_contents

    def dump(self, file_contents, filepath=None):
        filepath = self.filepath if filepath is not None else filepath
        with smart_open(filepath, 'w') as file:
            json.dump(file_contents, file)
        self.file_contents = file_contents
        LOGGER.info(f'Dumped {self.filepath} to {filepath}')
