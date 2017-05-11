"""Unit Test for current_scores.sql query"""
import datetime

import pytest
import sqlalchemy as sqla

#import python.clover_analytics.test_suite.utilities as utils


# This is the Query we are going to test.
QUERY_FILENAME = '../queries/schema/tutorial_analytics/doctor_dashboard.sql'

TARGET_SCHEMA = 'tutorial_analytics'
TARGET_TABLE_NAME = 'doctor_dashboard'

METADATA = sqla.MetaData()

TUTORIAL_DATA_INGEST__PATIENTS = sqla.Table(
    'patients', METADATA,
    sqla.Column('patient_id', sqla.Integer),
    sqla.Column('name', sqla.Text),
    sqla.Column('doctor_id', sqla.Text),
    schema='tutorial_data_ingest')

TUTORIAL_ANALYTICS__CURRENT_SCORES = sqla.Table(
    'current_scores', METADATA,
    sqla.Column('patient_id', sqla.Integer),
    sqla.Column('assessment_type', sqla.Text),
    sqla.Column('risk_score', sqla.Integer),
    sqla.Column('date_modified', sqla.Date),
    schema='tutorial_analytics')

SOURCE_TABLES = [
    TUTORIAL_DATA_INGEST__PATIENTS,
    TUTORIAL_ANALYTICS__CURRENT_SCORES,
]

TARGET_QUERY = 'SELECT * FROM %s.%s' % (TARGET_SCHEMA, TARGET_TABLE_NAME)


@pytest.fixture(autouse=True)
def db_setup(tpostgres):
    """Set up Database"""
    tpostgres.set_up_database(SOURCE_TABLES, TARGET_SCHEMA)


def test_nothing_in_nothing_out(tpostgres):
    """Simple no data in, no data out test"""
    # Execute
    tpostgres.run_transform_query(QUERY_FILENAME)

    # Verify
    results = tpostgres.connection.execute(TARGET_QUERY)
    assert results.rowcount == 0


@pytest.mark.parametrize(
    'to_insert_patients, to_insert_scores, expected',
    [
        # Test 1: Each test should represent a different behavior you want to test.
        (
            [
                {'patient_id': 1, 'doctor_id': 'dr1'},
                {'patient_id': 2, 'doctor_id': 'dr1'},
                {'patient_id': 3, 'doctor_id': 'dr2'},
                {'patient_id': 4, 'doctor_id': 'dr2'},
            ],
            [
                {'patient_id': 1, 'assessment_type': 'physical', 'risk_score': 96},
                {'patient_id': 1, 'assessment_type': 'psych', 'risk_score': 90},
                {'patient_id': 2, 'assessment_type': 'physical', 'risk_score': 86},
                {'patient_id': 2, 'assessment_type': 'psych', 'risk_score': 90},
                {'patient_id': 3, 'assessment_type': 'physical', 'risk_score': 78},
                {'patient_id': 3, 'assessment_type': 'psych', 'risk_score': 96},
                {'patient_id': 4, 'assessment_type': 'physical', 'risk_score': 91},
                {'patient_id': 4, 'assessment_type': 'psych', 'risk_score': 90},
            ],
            [
                ('dr1', 'physical', 91),
                ('dr1', 'psych', 90),
                ('dr2', 'physical', 84.5),
                ('dr2', 'psych', 93),
            ],
        ),
    ])
def test_aggregation(tpostgres, to_insert_patients, to_insert_scores, expected):
    """Tests that we are performing join and aggregating."""
    tpostgres.connection.execute(
        sqla.insert(TUTORIAL_DATA_INGEST__PATIENTS, values=to_insert_patients))
    tpostgres.connection.execute(
        sqla.insert(TUTORIAL_ANALYTICS__CURRENT_SCORES, values=to_insert_scores))

    # Execute
    tpostgres.run_transform_query(QUERY_FILENAME)

    # Verify
    results = tpostgres.connection.execute(TARGET_QUERY)
    rows = [(row.doctor_id, row.assessment_type, row.avg_risk_score) for row in results]
    assert rows == expected
