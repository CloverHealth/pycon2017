import logging
from collections import namedtuple

MockLogRecord = namedtuple('MockLogRecord', ['level', 'msg', 'args', 'kwargs'])


class MockLogger(logging.Logger):
    """
    Fake logger object that collects all logged messages for later inspection by regression tests

    This is a simplified alternative to using the 'pytest-catchlog' plugin
    """
    def __init__(self, *args, **kwargs):
        # pylint: disable=super-init-not-called
        # pylint: disable=unused-argument
        self.messages = []

    def isEnabledFor(self, level):
        return True

    def _log(self, level, message, args, **kwargs):
        self.messages.append(
            MockLogRecord(level, message, args, kwargs))
