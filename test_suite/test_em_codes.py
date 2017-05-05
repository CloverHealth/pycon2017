"""
Tests for EM Upcoding
"""
import datetime as dt
import decimal as de

import pytest
import sqlalchemy as sa

from em_upcoding import em_codes as em


# Test database setup

TARGET_SCHEMA = "data_science"
TARGET_TABLE_NAME = "upcoding__em_codes"

METADATA = sa.MetaData()

MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL = sa.Table(
    'fact_medical_claim_lines_all',
    METADATA,
    sa.Column('servicing_provider_npi', sa.Text),
    sa.Column('claim_line_internal_status', sa.Text),
    sa.Column('claim_status', sa.Text),
    sa.Column('date_of_service_from_date', sa.Date),
    sa.Column('procedure_code', sa.Text),
    sa.Column('procedure_name', sa.Text),
    schema='medical_claims')

SOURCE_TABLES = [
    MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL,
]

# Mocking utilities
CLAIM_LINES_DEFAULTS = {
    'claim_line_internal_status': 'CPC010',
    'claim_status': '02',
    'date_of_service_from_date': dt.date(2016, 1, 1),
    'procedure_code': '99281',
    'procedure_name': 'name,foo',
    'servicing_provider_npi': 'doctor1'
}

CODES = ['99281', '99282', '99283', '99284', '99285']


def _claim_lines_row(**kwargs):
    """Defining Rows"""
    row = CLAIM_LINES_DEFAULTS.copy()
    row.update(kwargs)
    return row


@pytest.fixture(autouse=True)
def db_setup(tpostgres):
    """Setting up testing database"""
    tpostgres.set_up_database(SOURCE_TABLES, TARGET_SCHEMA)


@pytest.mark.parametrize(
    'source_rows, expected', [
        # nothing in nothing out
        (
            [],
            []
        ),
        # single component
        (
            [_claim_lines_row()],
            [
                (
                    'doctor1',
                    '99281',
                    ['name', 'foo'],
                )
            ]
        ),
    ]
)
def test_get_claims_base_data(tpostgres, source_rows, expected):
    """Test occurrences by code"""
    for row in source_rows:
        tpostgres.connection.execute(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL.insert(), row)  # pylint: disable=no-value-for-parameter
    results = tpostgres.connection.execute(
        em.get_claims_base_data(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL, CODES))
    rows = [(row.servicing_provider_npi,
             row.procedure_code,
             row.procedure_name_array,) for row in results]
    assert rows == expected


@pytest.mark.parametrize(
    'source_rows, expected', [
        # nothing in nothing out
        (
            [],
            []
        ),
        # single component
        (
            [_claim_lines_row()],
            [
                (
                    'doctor1',
                    'name,foo',
                    'name',
                )
            ]
        ),
        # With duplicates (perhaps because there's a procedure modifier)
        # TODO this test is disabled because add_claims_procedure_stems() has no deduplication logic
        # (
        #     [_claim_lines_row(), _claim_lines_row()],
        #     [
        #         (
        #             'doctor1',
        #             'name,foo',
        #             'name',
        #         )
        #     ]
        # ),
    ]
)
def test_add_claims_procedure_stems(tpostgres, source_rows, expected):
    """Test adding procedure name stems"""
    for row in source_rows:
        tpostgres.connection.execute(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL.insert(), row)  # pylint: disable=no-value-for-parameter
    results = tpostgres.connection.execute(
        em.add_claims_procedure_stems(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL, CODES))
    rows = [(row.servicing_provider_npi,
             row.procedure_name,
             row.procedure_name_stem,) for row in results]
    assert rows == expected


@pytest.mark.parametrize(
    'source_rows, expected', [
        # nothing in nothing out
        (
            [],
            []
        ),
        # testing counts
        (
            [
                _claim_lines_row(),
                _claim_lines_row(procedure_code='99282'),
                _claim_lines_row(
                    procedure_code='99282', date_of_service_from_date=dt.date(2016, 2, 1)),
                _claim_lines_row(
                    procedure_code='99285', date_of_service_from_date=dt.date(2016, 2, 1)),
                _claim_lines_row(
                    procedure_code='99285', date_of_service_from_date=dt.date(2016, 3, 1)),
                _claim_lines_row(
                    servicing_provider_npi='doctor2'),
                _claim_lines_row(
                    servicing_provider_npi='doctor2', procedure_code='99282'),
                _claim_lines_row(
                    servicing_provider_npi='doctor2', procedure_code='99282',
                    date_of_service_from_date=dt.date(2016, 3, 1)),
            ],
            [
                (
                    'doctor1',
                    '99281',
                    1,
                ),
                (
                    'doctor1',
                    '99282',
                    2,
                ),
                (
                    'doctor1',
                    '99285',
                    2,
                ),
                (
                    'doctor2',
                    '99281',
                    1,
                ),
                (
                    'doctor2',
                    '99282',
                    2,
                ),
            ]
        ),
    ]
)
def test_provider_level_counts(tpostgres, source_rows, expected):
    """Test counting procedure code occurrences by provider and procedure_code"""
    for row in source_rows:
        tpostgres.connection.execute(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL.insert(), row)  # pylint: disable=no-value-for-parameter
    query = em.provider_level_counts(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL, CODES)
    query = query.order_by(query.c.servicing_provider_npi, query.c.procedure_code)
    results = tpostgres.connection.execute(query)
    rows = [(row.servicing_provider_npi,
             row.procedure_code,
             row.occurrences,) for row in results]
    assert rows == expected


@pytest.mark.parametrize(
    'source_rows, expected', [
        # nothing in nothing out
        (
            [],
            []
        ),
        # testing counts
        (
            [
                _claim_lines_row(),
                _claim_lines_row(procedure_code='99282'),
                _claim_lines_row(
                    procedure_code='99282', date_of_service_from_date=dt.date(2016, 2, 1)),
                _claim_lines_row(
                    procedure_code='99285', date_of_service_from_date=dt.date(2016, 2, 1)),
                _claim_lines_row(
                    procedure_code='99285', date_of_service_from_date=dt.date(2016, 3, 1)),
                _claim_lines_row(
                    servicing_provider_npi='doctor2'),
                _claim_lines_row(
                    servicing_provider_npi='doctor2', procedure_code='99282'),
                _claim_lines_row(
                    servicing_provider_npi='doctor2', procedure_code='99282',
                    date_of_service_from_date=dt.date(2016, 3, 1)),
            ],
            [
                (
                    'doctor1',
                    '99285',
                    2,
                    5,
                    1
                ),
                (
                    'doctor1',
                    '99282',
                    2,
                    5,
                    2
                ),
                (
                    'doctor2',
                    '99282',
                    2,
                    3,
                    2
                ),
                (
                    'doctor1',
                    '99281',
                    1,
                    5,
                    4
                ),
                (
                    'doctor2',
                    '99281',
                    1,
                    3,
                    4
                ),
            ]
        ),
    ]
)
def test_provider_all_counts(tpostgres, source_rows, expected):
    """Test counting procedure code occurrences by provider and procedure_code"""
    for row in source_rows:
        tpostgres.connection.execute(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL.insert(), row)  # pylint: disable=no-value-for-parameter
    results = tpostgres.connection.execute(
        em.provider_all_counts(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL, CODES))
    rows = [(row.servicing_provider_npi,
             row.procedure_code,
             row.occurrences,
             row.stem_occurrences,
             row.procedure_rank) for row in results]
    assert rows == expected


@pytest.mark.parametrize(
    'source_rows, expected', [
        # nothing in nothing out
        (
            [],
            []
        ),
        # testing counts
        (
            [
                _claim_lines_row(),
                _claim_lines_row(procedure_code='99282'),
                _claim_lines_row(
                    procedure_code='99282', date_of_service_from_date=dt.date(2016, 2, 1)),
                _claim_lines_row(
                    procedure_code='99285', date_of_service_from_date=dt.date(2016, 2, 1)),
                _claim_lines_row(
                    procedure_code='99285', date_of_service_from_date=dt.date(2016, 3, 1)),
                _claim_lines_row(
                    servicing_provider_npi='doctor2'),
                _claim_lines_row(
                    servicing_provider_npi='doctor2', procedure_code='99282'),
                _claim_lines_row(
                    servicing_provider_npi='doctor2', procedure_code='99282',
                    date_of_service_from_date=dt.date(2016, 3, 1)),
            ],
            [
                (
                    'doctor1',
                    de.Decimal('5'),
                    de.Decimal('2')
                ),
                (
                    'doctor2',
                    de.Decimal('3'),
                    de.Decimal('0')
                ),
            ]
        ),
    ]
)
def test_calc_provider_high_encounters(tpostgres, source_rows, expected):
    """Test counting procedure code occurrences by provider and procedure_code"""
    for row in source_rows:
        tpostgres.connection.execute(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL.insert(), row)  # pylint: disable=no-value-for-parameter
    results = tpostgres.connection.execute(
        em.calc_provider_high_encounters(MEDICAL_CLAIMS__FACT_MEDICAL_CLAIM_LINES_ALL, CODES))
    rows = [(row.servicing_provider_npi,
             row.encounters,
             row.high_encounters,) for row in results]
    assert rows == expected
