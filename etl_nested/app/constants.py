import enum


DEFAULT_CONFIG_DIR = 'conf'
PROCESSOR_CONFIG_FILE = 'processors.conf.json'


class AnswerType(enum.Enum):
    number = 1
    text = 2
    boolean = 3
    date = 4
