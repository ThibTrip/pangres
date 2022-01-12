#!/usr/bin/env python
# coding: utf-8
"""
End to end test similar to the scenario proposed in the docstring
of `pangres.upsert`.

We will create a table and then insert with update and then ignore for the
`ON CONFLICT` clause.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text, VARCHAR
from sqlalchemy.exc import ProgrammingError, OperationalError
from pangres import upsert
from pangres.examples import _TestsExampleTable
from pangres.exceptions import HasNoSchemaSystemException
from pangres.tests.conftest import (commit,
                                    drop_table,
                                    drop_table_for_test,
                                    get_table_namespace,
                                    read_example_table_from_db,
                                    schema_for_testing_creation,
                                    TableNames)


# # Helpers

utc_ts = lambda s: pd.Timestamp(s, tz='UTC')

# # Config

table_name = 'test_upsert_end_to_end'


# # Test data

# +
# df from which we will create a SQL table
df = _TestsExampleTable.create_example_df(nb_rows=6)
# test nulls won't create any problems
# avoid nulls in boolean column though as this is unpractical with pandas in older versions
df.iloc[0,[ix for ix, col in enumerate(df.columns) if col != 'likes_pizza']] = None

# df from which we will upsert update or upsert ignore
# remove one record from above and add one
# so we know that 1) old records are still there, 2) new ones get added
# 3) we can check whether the update/ignore of existing records worked
df2 = _TestsExampleTable.create_example_df(nb_rows=7)
df2 = df2.iloc[1:]
# -

# # Expectations

# +
df_after_insert_update = pd.concat(objs=(df.loc[~df.index.isin(df2.index.tolist())], # everything that is not in df2
                                         df2))

df_after_insert_ignore = pd.concat(objs=(df,
                                         df2.loc[~df2.index.isin(df.index.tolist())])) # everything that is not in df


# -

# # Tests

# +
# after the table is created with a first `upsert`
# using `create_table=False` or `create_table=True` should both work
@pytest.mark.parametrize('create_table', [False, True], ids=['create_table_false', 'create_table_true'])
@pytest.mark.parametrize('if_row_exists, df_expected', [['update', df_after_insert_update],
                                                        ['ignore', df_after_insert_ignore]],
                         ids=['update', 'ignore'])
@drop_table_for_test(TableNames.END_TO_END)
def test_end_to_end(engine, schema, create_table, if_row_exists, df_expected):
    # config
    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    table_name = TableNames.END_TO_END
    # common kwargs for every time we use the upsert function
    common_kwargs = dict(if_row_exists=if_row_exists, dtype=dtype, table_name=table_name)
    read_table = lambda: read_example_table_from_db(engine=engine, schema=schema, table_name=table_name).sort_index()

    # 1. create table
    upsert(con=engine, schema=schema, df=df, create_table=True, **common_kwargs)
    pd.testing.assert_frame_equal(df, read_table())

    # 2. insert update/ignore
    upsert(con=engine, schema=schema, df=df2, create_table=create_table, **common_kwargs)
    pd.testing.assert_frame_equal(df_expected, read_table())


def test_bad_value_if_row_exists(_):
    df = pd.DataFrame({'id':[0]}).set_index('id')
    engine = create_engine('sqlite:///')
    with pytest.raises(ValueError) as excinfo:
        upsert(con=engine, df=df, table_name=TableNames.NO_TABLE, if_row_exists='test')
    assert 'must be "ignore" or "update"' in str(excinfo.value)


@drop_table_for_test(TableNames.ADD_NEW_COLUMN)
def test_add_new_column(engine, schema):
    # config
    table_name = TableNames.ADD_NEW_COLUMN
    ns = get_table_namespace(schema=schema, table_name=table_name)
    dtype = {'id':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = pd.DataFrame({'id':['foo']}).set_index('id')
    # common kwargs for all the times we use upsert
    common_kwargs = dict(con=engine, schema=schema, df=df, table_name=table_name,
                         if_row_exists='update', dtype=dtype)

    # 1. create table
    upsert(**common_kwargs)
    # 2. add a new column and repeat upsert
    df['new_column'] = 'bar'
    upsert(**common_kwargs, add_new_columns=True)
    # verify content matches
    with engine.connect() as connection:
        df_db = pd.read_sql(text(f'SELECT * FROM {ns}'), con=connection, index_col='id')
        pd.testing.assert_frame_equal(df, df_db)


@drop_table_for_test(TableNames.CHANGE_EMPTY_COL_TYPE)
def test_adapt_column_type(engine, schema):
    # skip for sqlite as it does not support such alteration
    if 'sqlite' in engine.dialect.dialect_description:
        pytest.skip()

    # config
    table_name = TableNames.CHANGE_EMPTY_COL_TYPE
    dtype = {'id':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = pd.DataFrame({'id':['foo'], 'empty_column':[None]}).set_index('id')
    # common kwargs for all the times we use upsert
    common_kwargs = dict(con=engine, schema=schema, df=df, table_name=table_name,
                         if_row_exists='update', dtype=dtype)

    # 1. create table
    upsert(**common_kwargs)
    # 2. add non string data in empty column and repeat upsert
    df['empty_column'] = 1
    upsert(**common_kwargs, adapt_dtype_of_empty_db_columns=True)


@drop_table_for_test(TableNames.NO_TABLE)
def test_cannot_insert_missing_table_no_create(engine, schema):
    """
    Check if an error is raised when trying to insert in a missing table
    and `create_table` is False.
    """
    df = pd.DataFrame({'id':[0]}).set_index('id')
    with pytest.raises((OperationalError, ProgrammingError)) as excinfo:
        upsert(con=engine, schema=schema, df=df, table_name=TableNames.NO_TABLE,
               if_row_exists='update', create_table=False)
    assert any(s in str(excinfo.value) for s in ('no such table', 'does not exist', "doesn't exist"))


@drop_table_for_test(TableNames.CREATE_SCHEMA_NONE)
def test_create_schema_none(engine, schema):
    """
    If `create_schema` is True in `pangres.upsert` but the schema is `None`
    we should not raise an error even if it is a database that does not
    support schemas
    """
    df = pd.DataFrame({'id':[0]}).set_index('id')
    upsert(con=engine, schema=None, df=df, if_row_exists='update', create_schema=True,
            table_name=TableNames.CREATE_SCHEMA_NONE, create_table=True)


def test_create_schema_not_none(engine, schema):
    # local helpers
    is_postgres = 'postgres' in engine.dialect.dialect_description

    def drop_schema():
        if not is_postgres:
            return
        with engine.connect() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS {schema};'))
            commit(connection)

    # overwrite schema
    schema = schema_for_testing_creation

    # config
    df = pd.DataFrame({'id':[0]}).set_index('id')
    table_name = TableNames.CREATE_SCHEMA_NOT_NONE

    # drop table before test (could not get my decorator to work with another schema
    # when having an optional arg schema=None due to variable scopes problems)
    if is_postgres:
        drop_table(engine=engine, schema=schema, table_name=table_name)
        drop_schema()

    try:
        upsert(con=engine, schema=schema, df=df, if_row_exists='update', create_schema=True,
               table_name=table_name, create_table=True)
        if not is_postgres:
            raise AssertionError('Expected the upsert to fail when not using a postgres database')
    except Exception as e:
        # for postgres this should have worked
        if is_postgres:
            raise e
        else:
            assert isinstance(e, HasNoSchemaSystemException)
    finally:
        # drop table and schema after test
        if is_postgres:
            drop_table(engine=engine, schema=schema, table_name=table_name)
            drop_schema()
