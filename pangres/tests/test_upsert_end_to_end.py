#!/usr/bin/env python
# coding: utf-8
# +
"""
End to end test similar to the scenario proposed in the docstring
of `pangres.upsert_or_aupsert`.

We will create a table and then insert with update and then ignore for the
`ON CONFLICT` clause.
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, VARCHAR
from sqlalchemy.exc import ProgrammingError, OperationalError

# local imports
from pangres.examples import _TestsExampleTable
from pangres.exceptions import HasNoSchemaSystemException
from pangres.tests.conftest import (drop_table, drop_table_for_test, drop_schema,
                                    read_example_table_from_db,
                                    schema_for_testing_creation,
                                    select_table, TableNames, upsert_or_aupsert)
# -


# # Helpers

utc_ts = lambda s: pd.Timestamp(s, tz='UTC')

# # Test data

# +
# df from which we will create a SQL table
df = _TestsExampleTable.create_example_df(nb_rows=6)
# test nulls won't create any problems
# avoid nulls in boolean column though as this is unpractical with pandas in older versions
df.iloc[0,[ix for ix, col in enumerate(df.columns) if col != 'likes_pizza']] = None

# df from which we will upsert_or_aupsert update or upsert_or_aupsert ignore
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
# after the table is created with a first `upsert_or_aupsert`
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
    # common kwargs for every time we use the upsert_or_aupsert function
    common_kwargs = dict(if_row_exists=if_row_exists, dtype=dtype, table_name=table_name)
    read_table = lambda: read_example_table_from_db(engine=engine, schema=schema, table_name=table_name).sort_index()

    # 1. create table
    upsert_or_aupsert(con=engine, schema=schema, df=df, create_table=True, **common_kwargs)
    pd.testing.assert_frame_equal(df, read_table())

    # 2. insert update/ignore
    upsert_or_aupsert(con=engine, schema=schema, df=df2, create_table=create_table, **common_kwargs)
    pd.testing.assert_frame_equal(df_expected, read_table())


def test_bad_value_if_row_exists(_):
    df = pd.DataFrame({'id':[0]}).set_index('id')
    engine = create_engine('sqlite:///')
    with pytest.raises(ValueError) as excinfo:
        upsert_or_aupsert(con=engine, df=df, table_name=TableNames.NO_TABLE, if_row_exists='test')
    assert 'must be "ignore" or "update"' in str(excinfo.value)


@drop_table_for_test(TableNames.ADD_NEW_COLUMN)
def test_add_new_column(engine, schema):
    # config
    table_name = TableNames.ADD_NEW_COLUMN
    dtype = {'id':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = pd.DataFrame({'id':['foo']}).set_index('id')
    # common kwargs for all the times we use upsert_or_aupsert
    common_kwargs = dict(con=engine, schema=schema, table_name=table_name,
                         if_row_exists='update', dtype=dtype)

    # 1. create table
    upsert_or_aupsert(df=df, **common_kwargs)
    # 2. add a new column and repeat upsert_or_aupsert
    df['new_column'] = 'bar'
    upsert_or_aupsert(df=df, add_new_columns=True, **common_kwargs)
    # verify content matches
    df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='id').sort_index()
    pd.testing.assert_frame_equal(df, df_db)


@drop_table_for_test(TableNames.CHANGE_EMPTY_COL_TYPE)
def test_adapt_column_type(engine, schema):
    # skip for sqlite as it does not support such alteration
    if 'sqlite' in engine.dialect.dialect_description:
        pytest.skip('such column alteration is not possible with SQlite')

    # config
    table_name = TableNames.CHANGE_EMPTY_COL_TYPE
    dtype = {'id':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = pd.DataFrame({'id':['foo'], 'empty_column':[None]}).set_index('id')
    # common kwargs for all the times we use upsert_or_aupsert
    common_kwargs = dict(con=engine, schema=schema, df=df, table_name=table_name,
                         if_row_exists='update', dtype=dtype)

    # 1. create table
    upsert_or_aupsert(**common_kwargs)
    # 2. add non string data in empty column and repeat upsert_or_aupsert
    df['empty_column'] = 1
    upsert_or_aupsert(**common_kwargs, adapt_dtype_of_empty_db_columns=True)


@drop_table_for_test(TableNames.NO_TABLE)
def test_cannot_insert_missing_table_no_create(engine, schema):
    """
    Check if an error is raised when trying to insert in a missing table
    and `create_table` is False.
    """
    df = pd.DataFrame({'id':[0]}).set_index('id')
    with pytest.raises((OperationalError, ProgrammingError)) as excinfo:
        upsert_or_aupsert(con=engine, schema=schema, df=df, table_name=TableNames.NO_TABLE,
                          if_row_exists='update', create_table=False)
    assert any(s in str(excinfo.value) for s in ('no such table', 'does not exist', "doesn't exist"))


@drop_table_for_test(TableNames.CREATE_SCHEMA_NONE)
def test_create_schema_none(engine, schema):
    """
    If `create_schema` is True in `pangres.upsert_or_aupsert` but the schema is `None`
    we should not raise an error even if it is a database that does not
    support schemas
    """
    df = pd.DataFrame({'id':[0]}).set_index('id')
    upsert_or_aupsert(con=engine, schema=None, df=df, if_row_exists='update', create_schema=True,
                      table_name=TableNames.CREATE_SCHEMA_NONE, create_table=True)


def test_create_schema_not_none(engine, schema):
    # local helpers
    is_postgres = 'postgres' in engine.dialect.dialect_description

    # overwrite schema
    schema = schema_for_testing_creation

    # config
    df = pd.DataFrame({'id':[0]}).set_index('id')
    table_name = TableNames.CREATE_SCHEMA_NOT_NONE

    # drop table before test (could not get my decorator to work with another schema
    # when having an optional arg schema=None due to variable scopes problems)
    if is_postgres:
        drop_table(engine=engine, schema=schema, table_name=table_name)
        if is_postgres:
            drop_schema(engine=engine, schema=schema)

    try:
        upsert_or_aupsert(con=engine, schema=schema, df=df, if_row_exists='update', create_schema=True,
                          table_name=table_name, create_table=True)
        if not is_postgres:
            raise AssertionError('Expected the upsert_or_aupsert to fail when not using a postgres database')
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
            drop_schema(engine=engine, schema=schema)


# see https://github.com/ThibTrip/pangres/issues/56
@drop_table_for_test(TableNames.PK_MYSQL)
def test_mysql_pk_not_auto_incremented(engine, schema):
    if 'mysql' not in engine.dialect.dialect_description:
        pytest.skip('This test is only relevant for MySQL')

    table_name = TableNames.PK_MYSQL

    # upsert first df using pangres which creates the table automatically
    df1 = pd.DataFrame({'id':[0, 1], 'name':['foo', 'bar']}).set_index('id')
    upsert_or_aupsert(con=engine, df=df1, table_name=table_name, if_row_exists='update')

    # upsert second df
    df2 = pd.DataFrame({'id':[100, 200], 'name':['baz', 'qux']}).set_index('id')
    upsert_or_aupsert(con=engine, df=df2, table_name=table_name, if_row_exists='update')

    # read df back
    df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='id')

    # check mysql got that correctly
    pd.testing.assert_frame_equal(df_db.sort_index(), pd.concat((df1, df2)).sort_index())
