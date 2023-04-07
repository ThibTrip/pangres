# +
"""
This module tests different ways of using pangres by providing a connection
instead of an engine and by using transactions
"""
import pandas as pd
import pytest
from sqlalchemy import text, VARCHAR

# local imports
from pangres import aupsert, upsert
from pangres.helpers import _sqla_gt14
from pangres.transaction import TransactionHandler
from pangres.tests.conftest import (aselect_table, adrop_table_between_tests, commit, drop_table_between_tests,
                                    select_table, sync_or_async_test, sync_async_exec_switch, TableNames)


# -

# # Sync and async variants for tests
#
# (`run_test_foo`|`run_test_foo_async`) -> `test_foo`

# ## Reusing a connection

# +
@drop_table_between_tests(table_name=TableNames.REUSE_CONNECTION)
def run_test_connection_usable_after_upsert(engine, schema):
    df = pd.DataFrame(index=pd.Index([0], name='ix'))
    with engine.connect() as con:
        # do some random upsert operation
        upsert(con=con, df=df, schema=schema,
               table_name=TableNames.REUSE_CONNECTION,
               if_row_exists='update')
        # attempt to reuse the connection
        result = con.execute(text('SELECT 1;')).scalar()
        assert result == 1
        commit(con)


@adrop_table_between_tests(table_name=TableNames.REUSE_CONNECTION)
async def run_test_connection_usable_after_upsert_async(engine, schema):
    df = pd.DataFrame(index=pd.Index([0], name='ix'))
    async with engine.connect() as con:
        # do some random upsert operation
        await aupsert(con=con, df=df, schema=schema,
                      table_name=TableNames.REUSE_CONNECTION,
                      if_row_exists='update')
        # attempt to reuse the connection
        proxy = await con.execute(text('SELECT 1;'))
        result = proxy.scalar()
        assert result == 1
        await con.commit()


# -

# ## Using our own transaction

# +
@drop_table_between_tests(table_name=TableNames.COMMIT_OR_ROLLBACK_TRANS)
def run_test_transaction(engine, schema, trans_op):
    df = pd.DataFrame(index=pd.Index(['foo'], name='ix'))
    table_name = TableNames.COMMIT_OR_ROLLBACK_TRANS
    # common keyword arguments for multiple upsert operations below
    common_kwargs = dict(schema=schema, table_name=table_name,
                         if_row_exists='update', dtype={'ix': VARCHAR(3)})

    with engine.connect() as connection:
        trans = connection.begin()
        try:
            # do some random upsert operation
            upsert(con=connection, df=df, **common_kwargs)
            # do some other operation that requires commit
            upsert(con=connection, df=df.rename(index={'foo': 'bar'}), **common_kwargs)
            getattr(trans, trans_op)()  # commit or rollback
        finally:
            trans.close()

    # if trans_op=='commit': make sure we have "bar" and "foo" in the index
    # elif trans_op=='rollback': make sure we don't have any data
    # or that the table was not even created (what is rolled back
    # depends on the database type and other factors)
    if trans_op == 'commit':
        df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='ix')
        # this simple check will be enough since we do not have values, only an index
        assert df_db.index.sort_values().tolist() == ['bar', 'foo']
        assert df_db.index.name == 'ix'

    elif trans_op == 'rollback':
        df_db = select_table(engine=engine, schema=schema, table_name=table_name, error_if_missing=False)
        # no table or an empty table
        assert df_db is None or len(df_db) == 0


@adrop_table_between_tests(table_name=TableNames.COMMIT_OR_ROLLBACK_TRANS)
async def run_test_transaction_async(engine, schema, trans_op):
    df = pd.DataFrame(index=pd.Index(['foo'], name='ix'))
    table_name = TableNames.COMMIT_OR_ROLLBACK_TRANS
    # common keyword arguments for multiple upsert operations below
    common_kwargs = dict(schema=schema, table_name=table_name,
                         if_row_exists='update', dtype={'ix': VARCHAR(3)})

    async with engine.connect() as connection:
        trans = await connection.begin()
        try:
            # do some random upsert operation
            await aupsert(con=connection, df=df, **common_kwargs)
            # do some other operation that requires commit
            await aupsert(con=connection, df=df.rename(index={'foo': 'bar'}), **common_kwargs)
            coro = getattr(trans, trans_op)  # commit or rollback
            await coro()
        finally:
            await trans.close()

    # if trans_op=='commit': make sure we have "bar" and "foo" in the index
    # elif trans_op=='rollback': make sure we don't have any data
    # or that the table was not even created (what is rolled back
    # depends on the database type and other factors)
    if trans_op == 'commit':
        df_db = await aselect_table(engine=engine, schema=schema, table_name=table_name, index_col='ix')
        # this simple check will be enough since we do not have values, only an index
        assert df_db.index.sort_values().tolist() == ['bar', 'foo']
        assert df_db.index.name == 'ix'

    elif trans_op == 'rollback':
        df_db = await aselect_table(engine=engine, schema=schema, table_name=table_name, error_if_missing=False)
        # no table or an empty table
        assert df_db is None or len(df_db) == 0


# -

# ## Commit-as-you-go or ROLLBACK

# +
@drop_table_between_tests(table_name=TableNames.COMMIT_AS_YOU_GO)
def run_test_commit_as_you_go(engine, schema):
    df = pd.DataFrame(index=pd.Index(['foo'], name='ix'))
    table_name = TableNames.COMMIT_AS_YOU_GO
    # common keyword arguments for multiple upsert operations below
    common_kwargs = dict(schema=schema, table_name=table_name,
                         if_row_exists='update', dtype={'ix': VARCHAR(3)})

    with engine.connect() as connection:
        # skip for sqlalchemy < 2.0 or when future=True flag is not passed
        # during engine creation (commit-as-you-go is a new feature)
        # when this is the case there is no attribute commit or rollback for
        # the connection
        if not hasattr(connection, 'commit'):  # pragma: no cover
            pytest.skip('test not possible because there is no attribute "commit" (most likely sqlalchemy < 2)')

        # do some random upsert operation and commit
        upsert(con=connection, df=df, **common_kwargs)
        connection.commit()

        # do some other operation that requires commit and then rollback
        upsert(con=connection, df=df.rename(index={'foo': 'bar'}), **common_kwargs)
        connection.rollback()

    # the table in the db should be equal to the initial df as the second
    # operation was rolled back
    df_db = select_table(engine=engine, schema=schema, table_name=table_name, index_col='ix')
    # this simple check will be enough since we do not have values, only an index
    assert df_db.index.sort_values().tolist() == df.index.sort_values().tolist()
    assert df_db.index.name == 'ix'


@adrop_table_between_tests(table_name=TableNames.COMMIT_AS_YOU_GO)
async def run_test_commit_as_you_go_async(engine, schema):
    df = pd.DataFrame(index=pd.Index(['foo'], name='ix'))
    table_name = TableNames.COMMIT_AS_YOU_GO
    # common keyword arguments for multiple upsert operations below
    common_kwargs = dict(schema=schema, table_name=table_name,
                         if_row_exists='update', dtype={'ix': VARCHAR(3)})

    async with engine.connect() as connection:
        # do some random upsert operation and commit
        await aupsert(con=connection, df=df, **common_kwargs)
        await connection.commit()

        # do some other operation that requires commit and then rollback
        await aupsert(con=connection, df=df.rename(index={'foo': 'bar'}), **common_kwargs)
        await connection.rollback()

    # the table in the db should be equal to the initial df as the second
    # operation was rolled back
    df_db = await aselect_table(engine=engine, schema=schema, table_name=table_name, index_col='ix')
    # this simple check will be enough since we do not have values, only an index
    assert df_db.index.sort_values().tolist() == df.index.sort_values().tolist()
    assert df_db.index.name == 'ix'


# -

# ## Errors

# +
def run_test_non_connectable_transaction_handler(_):
    with pytest.raises(TypeError) as exc_info:
        with TransactionHandler(connectable='abc'):
            pass  # pragma: no cover
    assert 'sqlalchemy connectable' in str(exc_info)


async def run_test_non_connectable_transaction_handler_async(_):
    with pytest.raises(TypeError) as exc_info:
        async with TransactionHandler(connectable='abc'):
            pass  # pragma: no cover
    assert 'sqlalchemy connectable' in str(exc_info)


# -

# # Actual tests

# +
def test_connection_usable_after_upsert(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_connection_usable_after_upsert_async,
                       f_sync=run_test_connection_usable_after_upsert)


@pytest.mark.parametrize("trans_op", ['commit', 'rollback'])
def test_transaction(engine, schema, trans_op):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_transaction_async,
                       f_sync=run_test_transaction,
                       trans_op=trans_op)


def test_commit_as_you_go(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_commit_as_you_go_async,
                       f_sync=run_test_commit_as_you_go)


@pytest.mark.parametrize("async_", [True, False], ids=['async', 'sync'])
def test_non_connectable_transaction_handler(_, async_):
    if async_:
        if not _sqla_gt14():
            pytest.skip('Cannot execute async test with sqlalchemy < 1.4')
        test_func = run_test_non_connectable_transaction_handler_async
    else:
        test_func = run_test_non_connectable_transaction_handler
    sync_async_exec_switch(test_func, _='')
