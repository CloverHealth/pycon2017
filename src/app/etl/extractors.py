import sqlalchemy.orm as sa_orm

from app import models


def _submission_query(session: sa_orm.Session):
    return session.query(models.Submission)


def naive_extractor(session: sa_orm.Session):
    return _submission_query(session)


def naive_load_all_extractor(session: sa_orm.Session):
    return _submission_query(session).all()


def chunked_extractor(session: sa_orm.Session, chunk_size=None):
    assert chunk_size

    # TODO determine if eager loads should be disabled or use selective joinedload
    return _submission_query(session).yield_per(chunk_size)
