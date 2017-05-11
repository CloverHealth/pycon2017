"""
Analysis looking at EM Upcoding and Ranking
"""
from collections import OrderedDict
import pandas as pd

import beta_binomial as bb
import em_codes as em


def generate_groupings(query_str, con):
    """A wrapper for building a table mapping NPIs to one of a finite set of group elements.

    Keyword arguments:
    query_str: A string defining the SQL query which generates the grouping table.
    """
    dfr = pd.read_sql(sql=query_str, con=con)
    dfr.columns = ['servicing_provider_npi', 'group']

    return dfr


def put_data_to_df(con, data, groupings=None):
    """Take SQL Alchemy table and put into pandas data frame

    Keyword arguments:
    grouping: Dataframe with mapping of the entity (e.g. NPI) to 'group'.
    """
    dfr = pd.read_sql(data, con)
    dfr.rename(columns={'high_encounters': 'successes', 'encounters': 'observations'}, inplace=True)
    if groupings is not None:
        dfr = dfr.merge(groupings, how='left', on=['servicing_provider_npi'])
    dfr['naive_perc'] = dfr.successes / dfr.observations
    # Remove whenever naive_perc is 0. One option could be to plug in epsilon?
    dfr = dfr[dfr.naive_perc != 0]
    dfr = dfr[dfr.naive_perc != 1]
    dfr.set_index('servicing_provider_npi', inplace=True)
    return dfr


def calculate_distribution_values(results, groupings=None):
    """
    Calculates distribution values (MAKE A BETTER DOCSTRING)
    Keyword Args:
    results - a selectable statement
    groupings - (npi, group) dataframe
    """
    result_output = {}
    distribution_values_output = {}
    if groupings is None:
        groups = results
        groups['group'] = 'All'
    groups = results.groupby(['group'])
    for name, group in groups:
        alpha, beta = bb.fit_beta_prior(group['naive_perc'])
        group['a'] = group['successes'] + alpha
        group['b'] = group['observations'] - group['successes'] + beta
        group['regressed_prob'] = bb.beta_expected_value(group['a'], group['b'])
        group['prob_above_average'] = [
            bb.compare_beta_distros(a, b, alpha, beta)
            for a, b in zip(group['a'], group['b'])
        ]

        distribution_values = OrderedDict()
        distribution_values['prior'] = bb.beta_values(alpha, beta)
        for person in group.index:
            distribution_values[person] = bb.beta_values((group.loc[person])['a'],
                                                         (group.loc[person])['b'])
        result_output[name] = group
        distribution_values_output[name] = distribution_values
    return result_output, distribution_values_output


def run(con, claim_lines, codes, groups=None):
    """
    Run the analysis
    """
    if isinstance(groups, str):
        groupings = generate_groupings(groups, con)
    elif isinstance(groups, pd.DataFrame):
        groupings = groups
    else:
        groupings = None
    results = put_data_to_df(
        con, em.calc_provider_high_encounters(claim_lines, codes), groupings)
    return calculate_distribution_values(results, groupings)
