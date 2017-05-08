import logging
import logging.config
import re

from app.util import sqldebug


class SQLSummaryFilter(logging.Filter):
    """
    This logging filter pretty prints SELECT and INSERT queries while suppressing output of any parameters
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._regex_sql = re.compile(r'^\s*SELECT|INSERT', re.IGNORECASE | re.MULTILINE)

    @staticmethod
    def _count_sql_parameters(record):
        return sum(len(a.params) for a in record.args if getattr(a, 'params', None))

    def filter(self, record):
        msg = record.msg
        # pretty-print SQL queries
        if self._regex_sql.match(msg):
            record.msg = '\n' + sqldebug.format_query(msg.strip())
        # suppress INSERT values
        elif msg == '%r':
            record.msg = '(...suppressed {} parameters...)'.format(self._count_sql_parameters(record))
            record.args = tuple()
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
                'formatter': 'default',
                # for the purposes of this workshop sample, all logs will be sent to stdout to make
                # shell redirection easier
                'stream': 'ext://sys.stdout'
            },
        },
        'filters': {
            'sql_pretty_print': {
                '()': 'app.logs.SQLSummaryFilter'
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
