"""
Async variant of module test_unique_key. See its docstring.
"""
import pandas as pd
import datetime
import pytest
from pangres import aupsert
from pangres.tests.conftest import adrop_table_if_exists
from sqlalchemy import INTEGER, VARCHAR, MetaData, Column, text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

# # Helpers

# ## Table model

# +
table_name = 'test_unique_key'

async def create_test_table(engine, schema):
    Base = declarative_base(bind=engine)

    class TestUniqueKey(Base):
        __tablename__ = table_name
        __table_args__ = (UniqueConstraint('order_id', 'product_id'),
                          {'schema':schema})

        row_id = Column(INTEGER, primary_key=True, autoincrement=True)
        order_id = Column(VARCHAR(5), nullable=False, server_default='-9999')
        product_id = Column(VARCHAR(5), nullable=False, server_default='-9999')
        qty = Column(INTEGER, nullable=True, comment='purchase_quantity')

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
# -

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

# # Unique key test

@pytest.mark.asyncio
async def test_upsert_with_unique_keys(engine, schema):

    # helpers
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    # local helper
    async def read_from_db():
        async with engine.connect() as connection:
            proxy = await connection.execute(text(f'SELECT * FROM {namespace}'))
            results = [r._asdict() for r in proxy.all()]
            return pd.DataFrame(results).set_index('row_id')

    # create our test table
    await adrop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    await create_test_table(engine=engine, schema=schema)

    # add initial data (df_old)
    await aupsert(engine=engine, df=df_old, schema=schema, table_name='test_unique_key', if_row_exists='update')
    df = await read_from_db()
    df_expected = df_old.assign(row_id=range(1, 4)).reset_index().set_index('row_id')
    pd.testing.assert_frame_equal(df, df_expected)

    # add new data (df_new)
    await aupsert(engine=engine, df=df_new, schema=schema, table_name='test_unique_key', if_row_exists='update')
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
