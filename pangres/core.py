#!/usr/bin/env python
# coding: utf-8
"""
Main functions of pangres that
will be directly exposed to its users.
"""
from pangres.helpers import PandasSpecialEngine


def pg_upsert(engine,
              df,
              table_name,
              if_exists,
              schema='public',
              create_schema=True,
              add_new_columns=True,
              adapt_dtype_of_empty_db_columns=True,
              clean_column_names=False,
              chunksize=10000):
    """
    Insert updates/ignores a pandas DataFrame into a postgres table.
    Will also create a table if it does not exist.

    The index is used as primary key (this implies it must be unique).
    All levels of the index have to be named.
    The index cannot contain any null value (this is due to a postgres
    restriction for the primary key).

    Important Notes
    ---------------
    The characters "(", ")" and "%" are not allowed in column names
    as testing has shown they may cause issues with psycopg2 (even in
    parameterized queries). If clean_column_names is True those characters
    will be removed.

    It is recommanded to use this function with big batches of data
    as there is quite the overhead.

    Parameters
    ----------
    engine : sqlalchemy.engine.base.Engine
        Engine from sqlalchemy (see sqlalchemy.create_engine)
    df : pd.DataFrame
        A pandas DataFrame
    table_name : str
        Name of the postgres table
    if_exists : str
        One of 'upsert_overwrite' or 'upsert_keep'
        if 'upsert_overwrite' all the data is updated
        where the primary key matches,
        if 'upsert_keep' all the data is kept
        where the primary key matches.
    schema : str, default 'public'
        Name of the postgres schema that contains the table
    create_schema : bool, default True
        If True the schema is created if it does not exist
    add_new_columns : bool, default True
        If True adds columns present in the DataFrame that
        are not in the postgres table.
    adapt_dtype_of_empty_db_columns : bool, default True
        If True looks for columns that have no data in the
        postgres table but have data in the DataFrame;
        if those columns have datatypes that do not match
        (e.g. "TEXT" in postgres and "int64" in the DataFrame)
        then they are altered in the postgres table.
    clean_column_names : bool, default False
            If False raises a ValueError if any of the following
            characters are found in the column/index names: "(", ")" and "%".
            If True removes any of the aforementionned characters
            in the column/index names before updating the table.
            Our tests seem to indicate those characters can
            cause issues with psycopg2 even in parameterized
            queries (they are not properly escaped).

    Examples
    --------
    >>> import pandas as pd
    >>> from sqlalchemy import create_engine
    >>> # configure schema, table_name and engine
    >>> schema = 'tests'
    >>> table_name = 'pg_upsert_doctest'
    >>> engine = create_engine('postgresql://user:password@localhost:5432/mydatabase')
    >>> df = pd.DataFrame({'profileid':[0,1],
    ...                    'favorite_fruit':['banana','apple']})
    >>> df.set_index('profileid', inplace = True)

    **pg_upsert will create the table if it does not exist**

    >>> pg_upsert(engine = engine,
    ...           df = df,
    ...           schema = schema,
    ...           table_name = table_name,
    ...           if_exists = 'upsert_overwrite') # no difference for table creation

    **Update existing records and add new ones**

    >>> # update profile id 1 and add profile id 2
    >>> new_df = pd.DataFrame({'profileid':[1,2],
    ...                        'favorite_fruit':['pear','orange']})
    >>> new_df.set_index('profileid', inplace = True)
    >>> pg_upsert(engine = engine,
    ...           df = new_df,
    ...           schema = schema,
    ...           table_name = table_name,
    ...           if_exists = 'upsert_overwrite')

    **Keep existing records and add new ones**

    >>> # don't update profile id 2 and add profile id 3
    >>> new_df = pd.DataFrame({'profileid':[2,3],
    ...                        'favorite_fruit':['pineapple','lemon']})
    >>> new_df.set_index('profileid', inplace = True)
    >>> pg_upsert(engine = engine,
    ...           df = new_df,
    ...           schema = schema,
    ...           table_name = table_name,
    ...           if_exists = 'upsert_keep')

    **If you ran the whole example you should get the DataFrame below**

    >>> pd.read_sql(f'SELECT * FROM {schema}."{table_name}"',
    ...             con = engine, index_col = 'profileid')
              favorite_fruit
    profileid
    0                 banana
    1                   pear
    2                 orange
    3                  lemon
    """

    pse = PandasSpecialEngine(engine=engine,
                              df=df,
                              table_name=table_name,
                              schema=schema,
                              clean_column_names=clean_column_names)

    # add new columns from frame
    if add_new_columns and pse.table_exists():
        pse.add_new_columns()

    # change dtype of empty columns in db
    if adapt_dtype_of_empty_db_columns and pse.table_exists():
        pse.adapt_dtype_of_empty_db_columns()

    # create schema and table if not exists then insert values
    if create_schema:
        pse.create_schema_if_not_exists()
    pse.create_table_if_not_exists()
    
    # stop if no rows
    if df.empty:
        return
    pse.insert(if_exists=if_exists, chunksize=chunksize)
