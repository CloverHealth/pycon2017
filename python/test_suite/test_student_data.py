import pytest
import uuid
import sqlalchemy as sqla
from datetime import datetime
import pytz
import psycopg2.extras as pgx
import sqlalchemy.dialects.postgresql as pg

import python.clover_analytics.test_suite.utilities as utils


# This is the Query we are going to test.
QUERY_FILENAME = '../../queries/schema/stg_analytics/______.sql'

TARGET_SCHEMA = 'tutorial'
TARGET_TABLE_NAME = 'student_data'

METADATA = sqla.MetaData()

SOURCE_TABLES = [
]

TARGET_QUERY = 'SELECT * FROM %s.%s' % (TARGET_SCHEMA, TARGET_TABLE_NAME)


@pytest.fixture(autouse=True)
def db_setup(tpostgres):
    tpostgres.set_up_database(SOURCE_TABLES, TARGET_SCHEMA)

    # This is executed for each test
    tpostgres.connection.execute(
        sqla.insert(utils.MEDICAL_CLAIMS__FACT_MEDICAL_CLAIMS_ALL, values=[
            {'claim_id': 'AAA00', 'claim_stem': 'AAA', 'claim_status': '02', 'claim_internal_status':'CPG010'},
            {'claim_id': 'AAA00', 'claim_stem': 'AAA', 'claim_status': '91', 'claim_internal_status':'CPG010'},
        ]))

def test_nothing_in_nothing_out(tpostgres):
    # Execute
    tpostgres.run_transform_query(QUERY_FILENAME)

    # Verify
    results = tpostgres.connection.execute(TARGET_QUERY)
    assert results.rowcount == 0


@pytest.mark.parametrize(
    'to_insert_claim_lines, expected, comments',
    [
        ([{'claim_id': 'AAA00', 'revenue_code': '0540', 'place_of_service_code': None,
           'procedure_code_modifier_1': None, 'procedure_code_modifier_2': None}],
         ['AAA00'],
         'Revenue Code on list'),
    ])
def test_matching_valid_claim_lines(tpostgres, to_insert_claim_lines, expected, comments):
    """Tests all of the cases of revenue code, place of service code and procedure code modifier that generate an ambulance claim."""
    tpostgres.connection.execute(
        sqla.insert(utils.MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL, values=to_insert_claim_lines))

    # Execute
    tpostgres.run_transform_query(QUERY_FILENAME)

    # Verify
    results = tpostgres.connection.execute(TARGET_QUERY)
    rows = [row.claim_id for row in results]
    assert rows == expected
