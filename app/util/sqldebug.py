import sqlparse


def format_query(query):
    if type(query) == str:
        query_str = query
    else:
        query_str = str(query)

    return sqlparse.format(query_str, reindent=True, keyword_case='upper')


def pp_query(query):
    """
    Pretty prints a query
    :param query: Raw SQL string or SQLAlchemy query
    :return: str
    """
    print(format_query(query))
