"""
Defining Functions To help with looking at EM Upcoding and Ranking
"""
import datetime as dt

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg


def get_claims_base_data(claim_lines, codes):
    """
    Keyword Args:
    claim_lines: Medical Claim Lines table

    Returns: sqlalchemy selectable that filters claim lines table to only include last iteration
             of paid claims and a certain set of procedure codes.
    """
    columns = [
        claim_lines.c.servicing_provider_npi,
        claim_lines.c.procedure_code,
        claim_lines.c.procedure_name,
        sa.sql.func.string_to_array(
            claim_lines.c.procedure_name, ',').label('procedure_name_array')]
    condition = sa.and_(
        claim_lines.c.procedure_code is not None,
        claim_lines.c.procedure_code.in_(codes))
    return sa.select(columns).where(condition)


def add_claims_procedure_stems(claim_lines, codes):
    """
    Keyword Args:
    claim_lines: Medical Claim Lines table

    Returns: sqlalchemy selectable that runs get_claims_base_data and adds the procedure stem
    """
    base_without_stems = get_claims_base_data(claim_lines, codes)\
        .correlate(None)\
        .alias('base_without_stems')
    stem = sa.type_coerce(
        base_without_stems.c.procedure_name_array,
        pg.ARRAY(sa.Text))[1].label('procedure_name_stem')
    columns = [
        base_without_stems.c.servicing_provider_npi,
        base_without_stems.c.procedure_code,
        base_without_stems.c.procedure_name,
        stem]
    return sa.select(columns)


def provider_level_counts(claim_lines, codes):
    """
    Keyword Args:
    claim_lines: Medical Claim Lines table

    Returns: sqlalchemy selectable that takes medical claim lines table and aggregate counts by
             servicing_provider_npi and procedure_code level.
    """
    claims = add_claims_procedure_stems(claim_lines, codes).correlate(None).alias('claims')
    columns = [
        claims.c.servicing_provider_npi,
        claims.c.procedure_name_stem,
        claims.c.procedure_code,
        sa.sql.func.count().label('occurrences')]
    groups = [
        claims.c.servicing_provider_npi,
        claims.c.procedure_name_stem,
        claims.c.procedure_code]
    return sa.select(columns).group_by(*groups)


def provider_all_counts(claim_lines, codes):
    """
    Keyword Args:
    claim_lines: Medical Claim Lines Table

    Returns: sqlalchemy selectable that takes medical claim lines table and runs
             provider_level_counts. In addition, adds a total count by provider level
             and ranks procedures to flag which ones are "high".
    """
    provider_counts = provider_level_counts(claim_lines, codes).correlate(None).alias(
        'provider_counts')
    columns = [
        provider_counts.c.servicing_provider_npi,
        provider_counts.c.procedure_name_stem,
        provider_counts.c.procedure_code,
        provider_counts.c.occurrences,
        sa.sql.func.sum(provider_counts.c.occurrences)
        .over(partition_by=[provider_counts.c.servicing_provider_npi,
                            provider_counts.c.procedure_name_stem])
        .label('stem_occurrences'),

        sa.sql.func.rank().over(
            partition_by=[provider_counts.c.procedure_name_stem],
            order_by=sa.sql.expression.desc(
                provider_counts.c.procedure_code)).label('procedure_rank')]
    return sa.select(columns)


def calc_provider_high_encounters(claim_lines, codes):
    """Aggregate Claims Data at the Physician level to get

    1. Number of high encounters
    2. Percentage of high encounters

    Keyword arguments:
    claims: A sqlalchemy table object with the relevant claims for a particular procedure group

    Returns: A selectable unique at the NPI level with number of high encounters and percentage of
    high encounters
    """
    aggregated_provider_counts = provider_all_counts(claim_lines, codes).correlate(None).alias(
        'aggregated_provider_counts')
    encounters = sa.sql.func.sum(aggregated_provider_counts.c.occurrences).label('encounters')
    high_encounters = sa.sql.func.sum(sa.case([
        (aggregated_provider_counts.c.procedure_rank == 1,
         aggregated_provider_counts.c.occurrences)], else_=0)).label('high_encounters')
    columns = [
        aggregated_provider_counts.c.servicing_provider_npi,
        encounters,
        high_encounters,
        (1.0 * high_encounters / encounters).label('pct_high_encounters')
    ]
    groups = [aggregated_provider_counts.c.servicing_provider_npi]
    return sa.select(columns).group_by(*groups)
