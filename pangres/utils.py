# +
import logging
import pandas as pd
from math import floor
from sqlalchemy.engine import Connectable

# local imports
from pangres.helpers import _sqlite_gt3_32_0, validate_chunksize_param
from pangres.logger import log
from pangres.exceptions import (DuplicateLabelsException,
                                TooManyColumnsForUpsertException,
                                UnnamedIndexLevelsException)


# -

# # Function to fix bad column names for psycopg2

def fix_psycopg2_bad_cols(df: pd.DataFrame, replacements: dict = {'%': '', '(': '', ')': ''}) -> pd.DataFrame:
    """
    Replaces '%', '(' and ')' (characters that won't play nicely or even
    at all with psycopg2) in column and index names in a deep copy of df.
    This is a workaround for the unresolved issue
    described here: https://github.com/psycopg/psycopg2/issues/167

    **IMPORTANT**:
    You will need to apply the same changes in the database as
    well if the SQL table already exists for a given DataFrame.
    Otherwise you will for instance end up with a column
    "total_%" and "total_" in your SQL table.

    Parameters
    ----------
    df : pd.DataFrame
    replacements : dict {'%':str, '(':str, ')':str}, default {'%':'', '(':'', ')':''}
        The keys '%', '(' and ')' are mandatory.
        There cannot be any extra keys.

    Raises
    ------
    pangres.exceptions.UnnamedIndexLevelsException
        When you pass a df where not all index levels are named
    pangres.exceptions.DuplicateLabelsException
        When you pass a df with duplicated labels accross index/columns
        or when after cleaning we end up with duplicated labels
        e.g. "test(" and "test)" would by default both be renamed to "test"
    TypeError
        When `replacements` is not of the expected type or has wrong keys
        or has non string values

    Returns
    -------
    new_df : pd.DataFrame

    Examples
    --------
    * fix bad col/index names with default replacements (empty string for '(', ')' and '%')
    >>> from pangres import fix_psycopg2_bad_cols
    >>> import pandas as pd
    >>> df = pd.DataFrame({'test()':[0],
    ...                    'foo()%':[0]}).set_index('test()')
    >>> print(df.to_markdown())
    |   test() |   foo()% |
    |---------:|---------:|
    |        0 |        0 |

    >>> df_fixed = fix_psycopg2_bad_cols(df)
    >>> print(df_fixed.to_markdown())
    |   test |   foo |
    |-------:|------:|
    |      0 |     0 |

    * fix bad col/index names with custom replacements - you MUST provide replacements for '(', ')' and '%'!
    >>> import pandas as pd
    >>> df = pd.DataFrame({'test()':[0],
    ...                    'foo()%':[0]}).set_index('test()')
    >>> print(df.to_markdown())
    |   test() |   foo()% |
    |---------:|---------:|
    |        0 |        0 |

    >>> df_fixed = fix_psycopg2_bad_cols(df, replacements={'%':'percent', '(':'', ')':''})
    >>> print(df_fixed.to_markdown())
    |   test |   foopercent |
    |-------:|-------------:|
    |      0 |            0 |
    """
    # verify all index levels are named
    index_names = list(df.index.names)
    if any([ix_name is None for ix_name in index_names]):
        raise UnnamedIndexLevelsException("All index levels must be named!")

    # verify duplicated columns
    fields = list(df.index.names) + df.columns.tolist()
    if len(set(fields)) != len(fields):
        duplicates = [c for c in fields if fields.count(c) > 1]
        raise DuplicateLabelsException("There cannot be duplicated names amongst "
                                       f"index levels and/or columns! Duplicates found: {duplicates}")
    # verify replacements arg
    expected_keys = ('%', '(', ')')
    not_a_dict = not isinstance(replacements, dict)
    not_all_keys_present = not_a_dict or set(replacements.keys()) - set(expected_keys) != set()
    bad_nb_keys = not_a_dict or len(replacements) != len(expected_keys)
    if not_all_keys_present or bad_nb_keys:
        raise TypeError(f'replacements must be a dict containing the following keys (and none other): {expected_keys}')
    if not all((isinstance(v, str) for v in replacements.values())):
        raise TypeError('The values of replacements must all be strings')

    # replace bad col names
    translator = {ord(k): v for k, v in replacements.items()}
    new_df = df.copy(deep=True)
    renamer = lambda col: col.translate(translator) if isinstance(col, str) else col
    new_df = new_df.rename(columns=renamer).rename_axis(index=renamer)

    # check columns are unique after renaming
    fields = list(new_df.index.names) + new_df.columns.tolist()
    if len(set(fields)) != len(fields):
        duplicates = [c for c in fields if fields.count(c) > 1]
        raise DuplicateLabelsException("Columns/index are not unique after renaming! "
                                       f"Duplicates found: {duplicates}")

    # compare columns (and index)
    before = df.reset_index().columns.tolist()
    after = new_df.reset_index().columns.tolist()
    for i, j in zip(before, after):
        if (isinstance(i, str) and isinstance(j, str) and i != j):
            log(f'Renamed column/index "{i}" to "{j}s"')
    return new_df


# ## Function to adjust the size of chunks to upsert
# (depending on a DataFrame's shape and what a database allows for SQL parameters)

def adjust_chunksize(con: Connectable, df: pd.DataFrame, chunksize: int) -> int:
    """
    Checks if given `chunksize` is appropriate for upserting rows in given database using
    given DataFrame.
    The `chunksize` represents the number of rows you would like to upsert in a chunk when
    using the `pangres.upsert` function.

    Some databases have limitations on the number of SQL parameters and we need one parameter
    per value for upserting data.
    E.g. a DataFrame of 5 **columns+index levels** and 4 rows will require 5*4=20 SQL parameters.

    This function will check the database type (e.g. SQlite) and the number of **columns+index levels**
    to determine if the `chunksize` does not exceed limits and propose a lower one if it does.
    Otherwise the same `chunksize` that you gave as input is returned.

    This function currently takes into account max parameters limitations for the following cases:
    * sqlite (32766 max for version >= 3.22.0 otherwise 999)
    * asyncpg (32767 max)

    If you know about more parameter limitations relevant for this library (PostgreSQL, MySQL, SQlite
    or other databases I have not tested with this library that you managed to have working),
    please contact me.

    Parameters
    ----------
    con
        sqlalchemy Engine or Connection
    df
        DataFrame you would wish to upsert
    chunksize
        Size of chunks you would wish to use for upserting (represents the number of rows
        in each chunk)

    Raises
    ------
    TooManyColumnsForUpsertException
        When a DataFrame has more columns+index levels than the maximum number of allowed SQL variables
        for a SQL query for given database.
        In such a case even inserting row by row would not be possible because we would already
        have too many variables.
        For more information you can for instance google "SQLITE_MAX_VARIABLE_NUMBER".

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>>
    >>> # config (this assumes you have SQlite version >= 3.22.0)
    >>> engine = create_engine("sqlite://")
    >>>
    >>> # some df we want to upsert
    >>> df = pd.DataFrame({'name':['Albert']}).rename_axis(index='profileid')
    >>> print(df.to_markdown())
    |   profileid | name   |
    |------------:|:-------|
    |           0 | Albert |

    {{python}}
    >>> # adjust chunksize: 100,000 is too big of a chunksize in general for given database
    >>> # SQlite only allows 32766 parameters (values) at once maximum in a query
    >>> # since we have two columns (technically 1 column + 1 index level)
    >>> # we can only upsert in chunks of FLOOR(32766/2) rows maximum which is 16383
    >>> adjust_chunksize(con=engine, df=df, chunksize=100_000)  # doctest: +SKIP
    16383
    """
    validate_chunksize_param(chunksize=chunksize)

    # get maximum number of parameters depending on the database
    dialect = con.dialect.dialect_description  # type: ignore  # dialect attribute does exist
    if 'sqlite' in dialect:
        maximum = 32766 if _sqlite_gt3_32_0() else 999
    elif 'asyncpg' in dialect:
        maximum = 32767
    else:
        maximum = None

    # simple case we can solve early
    if maximum is None:
        return chunksize

    # adjust chunksize
    new_chunksize = floor(maximum / (len(df.columns) + df.index.nlevels))
    if new_chunksize < 1:
        raise TooManyColumnsForUpsertException('The df has more columns+index levels than the maxmimum '
                                               'number of allowed parameters '
                                               'for given database for a query (we could not even upsert row by row).')
    if chunksize > new_chunksize:
        log(f'Reduced chunksize from {chunksize} to {new_chunksize} due '
            f'to max variable restriction of given dialect (max {maximum} for dialect {dialect}).',
            level=logging.INFO)
        chunksize = new_chunksize
    return chunksize
