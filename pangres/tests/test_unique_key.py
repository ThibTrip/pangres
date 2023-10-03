# +
"""
This module tests that we can use a unique key instead of a primary key for upserting.

Thanks to LawrentChen on GitHub for pointing out this is possible and providing me
with an example.
See https://github.com/ThibTrip/pangres/issues/12
"""
import pandas as pd
from sqlalchemy import INTEGER, VARCHAR, Column, UniqueConstraint
# local imports
from pangres import aupsert, upsert
from pangres.helpers import _sqla_gt20
from pangres.tests.conftest import (adrop_table_between_tests, aselect_table, drop_table_between_tests,
                                    select_table, sync_or_async_test, TableNames)


if _sqla_gt20():
    from sqlalchemy.orm import declarative_base
else:
    from sqlalchemy.ext.declarative import declarative_base


# -

# # Helpers

# ## Table model

class TestUniqueKeyBase:
    __tablename__ = TableNames.UNIQUE_KEY

    row_id = Column(INTEGER, primary_key=True, autoincrement=True)
    order_id = Column(VARCHAR(5), nullable=False, server_default='-9999')
    product_id = Column(VARCHAR(5), nullable=False, server_default='-9999')
    qty = Column(INTEGER, nullable=True, comment='purchase_quantity')


# ## Data to be inserted/upserted in tests

# +
# initial data (first upsert)
data_old = {'order_id': ['A0001', 'A0002', 'A0002'],
            'product_id': ['PD100', 'PD200', 'PD201'],
            'qty': [10, 20, 22]}
df_old = pd.DataFrame(data_old).set_index(['order_id', 'product_id'])

# new data (second upsert)
data_new = {'order_id': ['A0001', 'A0002', 'A0002', 'A0003'],
            'product_id': ['PD100', 'PD200', 'PD201', 'PD300'],
            'qty': [10, 20, 77, 30]}
df_new = pd.DataFrame(data_new).set_index(['order_id', 'product_id'])


# -

# # Sync and async variants for tests
#
# (`run_test_foo`|`run_test_foo_async`) -> `test_foo`

# +
@drop_table_between_tests(table_name=TableNames.UNIQUE_KEY)
def run_test_upsert_with_unique_keys(engine, schema):
    # create table model
    Base = declarative_base()

    class TestUniqueKey(Base, TestUniqueKeyBase):
        __table_args__ = (UniqueConstraint('order_id', 'product_id'),
                          {'schema': schema})

    # create table
    Base.metadata.create_all(bind=engine)
    table_name = TestUniqueKey.__tablename__

    # config/local helpers
    # common kwargs for all the upsert commands below
    common_kwargs_upsert = dict(con=engine, schema=schema, table_name=table_name,
                                if_row_exists='update')

    def read_from_db():
        return select_table(engine=engine, schema=schema,
                            table_name=table_name, index_col='row_id')

    # add initial data (df_old)
    upsert(df=df_old, **common_kwargs_upsert)
    df = read_from_db()
    df_expected = df_old.assign(row_id=range(1, 4)).reset_index().set_index('row_id')
    pd.testing.assert_frame_equal(df, df_expected)

    # add new data (df_new)
    upsert(df=df_new, **common_kwargs_upsert)
    df = read_from_db()
    # before creating our expected df we need to implement the special case of postgres
    # where the id of the last row will be 7 instead of 4. I suppose that PG's ON
    # CONFLICT UPDATE clause will run in such a way that it will count 4 (number we
    # would expected) + 3 (three previous rows that were updated)
    last_row_id = 7 if 'postgres' in engine.dialect.dialect_description else 4
    df_expected = (pd.DataFrame([[1, 'A0001', 'PD100', 10],
                                 [2, 'A0002', 'PD200', 20],
                                 [3, 'A0002', 'PD201', 77],
                                 [last_row_id, 'A0003', 'PD300', 30]],
                                columns=['row_id'] + df_old.reset_index().columns.tolist())
                   .set_index('row_id'))
    pd.testing.assert_frame_equal(df, df_expected)


@adrop_table_between_tests(table_name=TableNames.UNIQUE_KEY)
async def run_test_upsert_with_unique_keys_async(engine, schema):
    # create table model
    Base = declarative_base()

    class TestUniqueKey(Base, TestUniqueKeyBase):
        __table_args__ = (UniqueConstraint('order_id', 'product_id'),
                          {'schema': schema})

    # create table
    async with engine.connect() as connection:
        await connection.run_sync(lambda connection: Base.metadata.create_all(bind=connection))
        await connection.commit()
    table_name = TestUniqueKey.__tablename__

    # config/local helpers
    # common kwargs for all the upsert commands below
    common_kwargs_upsert = dict(con=engine, schema=schema, table_name=table_name,
                                if_row_exists='update')

    async def read_from_db():
        return await aselect_table(engine=engine, schema=schema,
                                   table_name=table_name, index_col='row_id')

    # add initial data (df_old)
    await aupsert(df=df_old, **common_kwargs_upsert)
    df = await read_from_db()
    df_expected = df_old.assign(row_id=range(1, 4)).reset_index().set_index('row_id')
    pd.testing.assert_frame_equal(df, df_expected)

    # add new data (df_new)
    await aupsert(df=df_new, **common_kwargs_upsert)
    df = await read_from_db()
    # before creating our expected df we need to implement the special case of postgres
    # where the id of the last row will be 7 instead of 4. I suppose that PG's ON
    # CONFLICT UPDATE clause will run in such a way that it will count 4 (number we
    # would expected) + 3 (three previous rows that were updated)
    last_row_id = 7 if 'postgres' in engine.dialect.dialect_description else 4
    df_expected = (pd.DataFrame([[1, 'A0001', 'PD100', 10],
                                 [2, 'A0002', 'PD200', 20],
                                 [3, 'A0002', 'PD201', 77],
                                 [last_row_id, 'A0003', 'PD300', 30]],
                                columns=['row_id'] + df_old.reset_index().columns.tolist())
                   .set_index('row_id'))
    pd.testing.assert_frame_equal(df, df_expected)


# -

# # Actual tests

def test_upsert_with_unique_keys(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_upsert_with_unique_keys_async,
                       f_sync=run_test_upsert_with_unique_keys)
