import logging
import logging.config
import re

from app.util import sqldebug


class SQLPrettyPrintFilter(logging.Filter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._regex_sql = re.compile(r'^\s*SELECT|INSERT', re.IGNORECASE | re.MULTILINE)

    def filter(self, record):
        msg = record.msg
        if self._regex_sql.match(msg):
            record.msg = '\n' + sqldebug.format_query(msg.strip())
        return True


def setup_logging(sql_logging=False):
    """
    Sets up the application logging

    :param sql_logging: enables SQLAlchemy to log all SQL statements (use with caution!)
    """
    config = {
        'version': 1,
        'formatters': {
            'default': {
                'format': '[%(levelname)s:%(name)s]: %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'default'
            },
        },
        'filters': {
            'sql_pretty_print': {
                '()': 'app.logs.SQLPrettyPrintFilter'
            }
        },
        'loggers': {
            'app': {
                'level': 'INFO',
                'handlers': ['console']
            },
            '__main__': {
                'level': 'INFO',
                'handlers': ['console']
            }
        }
    }

    if sql_logging:
        config['loggers']['sqlalchemy.engine'] = {
            'level': 'INFO',
            'handlers': ['console']
        }
        config['handlers']['console']['filters'] = ['sql_pretty_print']

    logging.config.dictConfig(config)
