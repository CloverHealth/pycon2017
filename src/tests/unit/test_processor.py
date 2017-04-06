import pytest
import sqlalchemy.orm as sa_orm

from app import models, processor


@pytest.mark.usefixtures('mock_logger')
def test_processor(session: sa_orm.Session, extractor, transformer, loader, submission_metrics):
    processor.process(extractor, transformer, loader)
    session.flush()

    # one event per submission
    expected_num_events = submission_metrics.submissions
    actual_num_events = session.query(models.ResponseEvent).count()

    assert actual_num_events == expected_num_events
