#!/usr/bin/env python
# coding: utf-8
"""
Main functions of pangres that will be directly exposed to its users.
"""
from pangres.helpers import PandasSpecialEngine


# # upsert

def upsert(engine,
           df,
           table_name,
           if_row_exists,
           schema=None,
           create_schema=False,
           create_table=True,
           add_new_columns=False,
           adapt_dtype_of_empty_db_columns=False,
           chunksize=10000,
           dtype=None,
           yield_chunks=False):
    """
    Insert updates/ignores a pandas DataFrame into a SQL table (or
    creates a SQL table from the DataFrame if it does not exist).

    This index of the DataFrame must be the primary key or a unique key
    (can be a unique constraint with multiple columns too) of the SQL
    table and meet the following conditions:
    1. The index must be unique.
    2. All levels of the index have to be named.
    3. The index cannot contain any null value except for MySQL where
    rows with null in indices will be skipped and warnings will be raised.

    **GOTCHAS**:

    Please head over to https://github.com/ThibTrip/pangres/#Gotchas-and-caveats
    or read pangres/README.md.

    Notes
    -----
    It is recommanded to use this function with big batches of data
    as there is quite the overhead. Setting the arguments create_schema,
    add_new_columns and adapt_dtype_of_empty_db_columns to False should
    drastically reduce the overhead if you do not need such features.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        See https://docs.sqlalchemy.org/en/latest/core/engines.html
    df : pd.DataFrame
        See https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html
    table_name : str
        Name of the SQL table
    if_row_exists : {'ignore', 'update'}
        Behavior if a row exists in the SQL table.
        Irrelevant if the SQL table does not exist already.
        For both 'ignore' and 'update' rows with new primary/unique
        key values are inserted.

        * 'ignore': rows for which primary/unique keys exist in the SQL
        table are skipped
        * 'update': rows for which primary/unique keys exist in the SQL
        table are updated (with the columns available in the pandas
        DataFrame)
    schema : str or None, default None
        Name of the SQL schema that contains or will contain the table
        For postgres if it is None it will default to "public".
        For MySQL and SQlite the schema should be None since
        those SQL flavors do not have this system.
    create_schema : bool, default False
        If True the schema is created if it does not exist
    create_table : bool, default True
        If True the table is created if it does not exist
    add_new_columns : bool, default False
        If True adds columns present in the DataFrame that
        are not in the SQL table.
    adapt_dtype_of_empty_db_columns : bool, default False
        If True looks for columns that have no data in the
        SQL table but have data in the DataFrame;
        if those columns have datatypes that do not match
        (e.g. "TEXT" in the SQL table and "int64" in the DataFrame)
        then they are altered in the SQL table.
        Data type conversion must be supported by the SQL flavor!
        E.g. for Postgres converting from BOOLEAN to TIMESTAMP
        will not work even if the column is empty.
    chunksize : int
        Number of rows to insert at once.
        Please note that for SQlite a maximum of 999 parameters
        per queries means that the chunksize will be automatically
        reduced to math.floor(999/nb_columns) where nb_columns is
        the number of columns + index levels in the DataFrame.
    dtype : None or dict {str:SQL_TYPE}, default None
        Similar to pd.to_sql dtype argument.
        This is especially useful for MySQL where the length of
        primary keys with text has to be provided (see Examples)
    yield_chunks : bool, default False
        If True gives back an sqlalchemy object
        (sqlalchemy.engine.cursor.LegacyCursorResult)
        at each chunk with which you can for instance count rows.

    Raises
    ------
    pangres.HasNoSchemaSystemException
        When `create_schema` is True but the SQL flavor of given
        engine has no schema system (AFAIK only PostgreSQL has a
        schema system)

    Examples
    --------
    #### 1. Workflow example

    ##### 1.1. Creating a SQL table
    >>> import pandas as pd
    >>> from pangres import upsert, DocsExampleTable
    >>> from sqlalchemy import create_engine, VARCHAR
    >>> 
    >>> # create a SQLalchemy engine
    >>> engine = create_engine("sqlite:///:memory:")
    >>> 
    >>> # this is necessary if you want to test with MySQL
    >>> # instead of SQlite or Postgres because MySQL needs
    >>> # to have a definite limit for text primary keys/indices
    >>> dtype = {'full_name':VARCHAR(50)}
    >>> 
    >>> # get or create a pandas DataFrame
    >>> # for our example full_name is the index
    >>> # and will thus be used as primary key
    >>> df = DocsExampleTable.df
    >>> print(df.to_markdown()) # to_markdown exists since pandas v1
    | full_name     | likes_sport   | updated                   |   size_in_meters |
    |:--------------|:--------------|:--------------------------|-----------------:|
    | John Rambo    | True          | 2020-02-01 00:00:00+00:00 |             1.77 |
    | The Rock      | True          | 2020-04-01 00:00:00+00:00 |             1.96 |
    | John Travolta | False         | NaT                       |           nan    |

    >>> # create SQL table
    >>> # it does not matter if if_row_exists is set
    >>> # to "update" or "ignore" for table creation
    >>> upsert(engine=engine,
    ...        df=df,
    ...        table_name='example',
    ...        if_row_exists='update',
    ...        dtype=dtype)

    ##### 1.2. Updating the SQL table we created with if_row_exists='update'
    >>> new_df = DocsExampleTable.new_df
    >>> print(new_df.to_markdown())
    | full_name             | likes_sport   | updated                   |   size_in_meters |
    |:----------------------|:--------------|:--------------------------|-----------------:|
    | John Travolta         | True          | 2020-04-04 00:00:00+00:00 |             1.88 |
    | Arnold Schwarzenegger | True          | NaT                       |             1.88 |

    >>> # insert update using our new data
    >>> upsert(engine=engine,
    ...        df=new_df,
    ...        table_name='example',
    ...        if_row_exists='update',
    ...        dtype=dtype)
    >>> 
    >>> # Now we read from the database to check what we got and as you can see
    >>> # John Travolta was updated and Arnold Schwarzenegger was added!
    >>> print(pd.read_sql('SELECT * FROM example', con=engine, index_col='full_name')
    ...  .astype({'likes_sport':bool}).to_markdown())
    | full_name             | likes_sport   | updated                    |   size_in_meters |
    |:----------------------|:--------------|:---------------------------|-----------------:|
    | John Rambo            | True          | 2020-02-01 00:00:00.000000 |             1.77 |
    | The Rock              | True          | 2020-04-01 00:00:00.000000 |             1.96 |
    | John Travolta         | True          | 2020-04-04 00:00:00.000000 |             1.88 |
    | Arnold Schwarzenegger | True          |                            |             1.88 |

    ##### 1.3. Updating the SQL table with if_row_exists='ignore'
    >>> new_df2 = DocsExampleTable.new_df2
    >>> print(new_df2.to_markdown())
    | full_name     | likes_sport   | updated   |   size_in_meters |
    |:--------------|:--------------|:----------|-----------------:|
    | John Travolta | True          | NaT       |             2.5  |
    | John Cena     | True          | NaT       |             1.84 |

    >>> upsert(engine=engine,
    ...        df=new_df2,
    ...        table_name='example',
    ...        if_row_exists='ignore',
    ...        dtype=dtype)
    >>> # Now we read from the database to check what we got and as you can see
    >>> # John Travolta was NOT updated and John Cena was added!
    >>> print(pd.read_sql('SELECT * FROM example', con=engine, index_col='full_name')
    ...  .astype({'likes_sport':bool}).to_markdown())
    | full_name             | likes_sport   | updated                    |   size_in_meters |
    |:----------------------|:--------------|:---------------------------|-----------------:|
    | John Rambo            | True          | 2020-02-01 00:00:00.000000 |             1.77 |
    | The Rock              | True          | 2020-04-01 00:00:00.000000 |             1.96 |
    | John Travolta         | True          | 2020-04-04 00:00:00.000000 |             1.88 |
    | Arnold Schwarzenegger | True          |                            |             1.88 |
    | John Cena             | True          |                            |             1.84 |

    #### 2. Example for getting information on upserted chunks (parameter `yield_chunks` == True)
    >>> import pandas as pd
    >>> from pangres import upsert, DocsExampleTable
    >>> from sqlalchemy import create_engine, VARCHAR
    >>>
    >>> # config
    >>> engine = create_engine("sqlite:///:memory:")
    >>> chunksize = 2
    >>>
    >>> # get a DataFrame from somwhere
    >>> df = DocsExampleTable.df
    >>> print(df.to_markdown())
    | full_name     | likes_sport   | updated                   |   size_in_meters |
    |:--------------|:--------------|:--------------------------|-----------------:|
    | John Rambo    | True          | 2020-02-01 00:00:00+00:00 |             1.77 |
    | The Rock      | True          | 2020-04-01 00:00:00+00:00 |             1.96 |
    | John Travolta | False         | NaT                       |           nan    |

    >>>
    >>> # upsert in chunks of size `chunksize` and get
    >>> # back the number of rows updated for each chunk
    >>> iterator = upsert(engine=engine, df=df, table_name='test_row_count',
    ...                   chunksize=chunksize, if_row_exists='update',
    ...                   yield_chunks=True)
    >>> for result in iterator:
    ...     print(f'{result.rowcount} row(s) updated')
    2 row(s) updated
    1 row(s) updated
    """
    pse = PandasSpecialEngine(engine=engine,
                              df=df,
                              table_name=table_name,
                              schema=schema,
                              dtype=dtype)
    # add new columns from frame
    if add_new_columns and pse.table_exists():
        pse.add_new_columns()

    # change dtype of empty columns in db
    if adapt_dtype_of_empty_db_columns and pse.table_exists():
        pse.adapt_dtype_of_empty_db_columns()

    # create schema and table if not exists then insert values
    if create_schema and schema is not None:
        pse.create_schema_if_not_exists()
    if create_table:
        pse.create_table_if_not_exists()

    # stop if no rows
    ## note: use simple check with len() as df.empty returns True if there are index values but no columns
    if len(df) == 0:
        return

    # returns an iterator when we yield chunks otherwise None
    return pse.upsert(if_row_exists=if_row_exists, chunksize=chunksize, yield_chunks=yield_chunks)

# # async upsert

async def aupsert(engine, df, table_name, if_row_exists, schema=None, create_schema=False,
                  create_table=True, chunksize=10000, dtype=None,
                  handle_concurrent_objects_creation=True):
    """
    Async variant of `pangres.upsert`. The engine must use an asynchronous driver
    such as `asyncpg` for PostgreSQL, or `aiomysql` for MySQL etc.
    You will need to install this driver e.g. `pip install asyncpg`.

    Such asynchronous engines are only available from version 1.4. onwards.
    Not all databases/drivers are supported by `sqlalchemy` yet.
    E.g. `aiosqlite` does not seem to work in version 1.4.x.

    WARNING:
    This is an experimental feature. Usage may change without warnings in the next
    versions of pangres and compatibility with sqlalchemy versions different than
    1.4. is not guaranteed.
    While this function has been well tested in `pangres` there could still be changes
    when it comes to asynchronous engines in `sqlalchemy`:

    > The asyncio extension as of SQLAlchemy 1.4.3 can now be considered to be
    > beta level software.
    > API details are subject to change however at this point it is unlikely for
    > there to be significant backwards-incompatible changes.
    >
    > Source: https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html (2021-11-03)

    See docstring of `pangres.upsert` for details on the parameters that are not described below.
    Some parameters are missing in this asynchronous variant of `pangres.uspert` because
    of a lack of time/knowledge. I'd be glad if you are willing to help.

    Parameters
    ----------
    handle_concurrent_objects_creation : bool, default True
        Only relevant when `create_schema` or `create_table` are set to True.
        If True, handles the case when multiple connections try to create the same
        object (given table or schema) at the same time.
        This can happen for instance if you execute in parallel multiple `aupsert`
        coroutines for a same table which does not exist yet.
        In such a case it is possible that the SQL server tells more than one coroutine
        that the table does not exist yet. We would then have multiple create statements
        issued at the same time for the same table which would throw an error.

    Notes
    -----
    If executing from an IPython context (e.g. Jupyter) you will need to run this following code
    for running asynchronous code "on top" of IPython:

    ```python
    import nest_asyncio # pip install nest_asyncio
    nest_asyncio.apply()
    ```

    Examples
    --------
    >>> import asyncio
    >>> import pandas as pd
    >>> from pangres import aupsert, DocsExampleTable
    >>> from sqlalchemy import VARCHAR
    >>> from sqlalchemy.ext.asyncio import create_async_engine # doctest: +SKIP
    >>> 
    >>> # IMPORTANT: Change the connection string (this is a fake one), and do not forget to precise postgresql+asyncpg!
    >>> engine = create_async_engine("postgresql+asyncpg://user:password@localhost:5432/postgres") # doctest: +SKIP
    >>> df = DocsExampleTable.df
    >>> print(df.to_markdown()) # to_markdown exists since pandas v1
    | full_name     | likes_sport   | updated                   |   size_in_meters |
    |:--------------|:--------------|:--------------------------|-----------------:|
    | John Rambo    | True          | 2020-02-01 00:00:00+00:00 |             1.77 |
    | The Rock      | True          | 2020-04-01 00:00:00+00:00 |             1.96 |
    | John Travolta | False         | NaT                       |           nan    |

    >>> # create SQL table
    >>> # it does not matter if if_row_exists is set
    >>> # to "update" or "ignore" for table creation
    >>> loop = asyncio.get_event_loop()
    >>> coroutines = [aupsert(engine=engine, df=df, table_name='example', if_row_exists='update')] # doctest: +SKIP
    >>> tasks = asyncio.gather(*coroutines, return_exceptions=True) # doctest: +SKIP
    >>> results = loop.run_until_complete(tasks) # doctest: +SKIP
    >>> for r in results: # doctest: +SKIP
    ...     if isinstance(r, Exception):
    ...         raise r
    """
    pse = PandasSpecialEngine(engine=engine, df=df, table_name=table_name,
                              schema=schema, dtype=dtype)

    # create schema and table if not exists then insert values
    if create_schema and schema is not None:
        await pse.acreate_schema_if_not_exists()
    if create_table:
        await pse.acreate_table_if_not_exists()

    # stop if no rows
    ## note: use simple check with len() as df.empty returns True if there are index values but no columns
    if len(df) == 0:
        return

    return await pse.aupsert(if_row_exists=if_row_exists, chunksize=chunksize)
