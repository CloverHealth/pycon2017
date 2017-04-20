import logging

import more_itertools
import sqlalchemy.orm as sa_orm


LOGGER = logging.getLogger(__name__)


def log_metrics(loader_func):
    def _wrapper(*args, **kwargs):
        num_events = loader_func(*args, **kwargs)
        LOGGER.info('Inserted %d response events into database', num_events)
        return num_events

    return _wrapper


@log_metrics
def naive_loader(session: sa_orm.Session, events):
    num_events = 0
    for event in events:
        session.add(event)
        num_events += 1
    session.flush()
    return num_events


def naive_add_all_loader(session: sa_orm.Session, events):
    # NOTE: Session.add_all() does not return a count, so we do not support metric logging here
    session.add_all(events)
    session.flush()


@log_metrics
def chunked_bulk_save_objects_loader(session: sa_orm.Session, events, chunk_size=None, return_defaults=False):
    assert chunk_size

    num_events = 0
    batches = more_itertools.chunked(events, chunk_size)
    for batch in batches:
        num_events += len(batch)
        session.bulk_save_objects(batch, return_defaults=return_defaults)
    return num_events

# TODO test with chunked loader Session.bulk_insert_mappings()
