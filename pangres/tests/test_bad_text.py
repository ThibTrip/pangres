# -*- coding: utf-8 -*-
import pandas as pd
import pytest
import random
from pangres import upsert, fix_psycopg2_bad_cols
from pangres.tests.conftest import AutoDropTableContext

# # Helpers

# list of characters that could potentially cause errors in column names or even values
bad_char_seq = """/_- ?§$&"',:;*()%[]{}|<>=!+#""" + "\\"


# # Add colums with bad names and insert bad text values

# +
def test_bad_text_insert(engine, schema):
    # add bad text in a column named 'text'
    create_random_text = lambda: ''.join([random.choice(bad_char_seq) for i in range(10)])
    df_test = (pd.DataFrame({'text': [create_random_text() for i in range(10)]})
               .rename_axis(['profileid'], axis='index', inplace=False))
    with AutoDropTableContext(engine=engine, schema=schema, table_name='test_bad_text') as ctx:
        upsert(engine=engine, schema=schema, table_name=ctx.table_name, df=df_test,
               if_row_exists='update')

def test_bad_column_names(engine, schema):
    # add columns with bad names
    # don't do this for MySQL which has more strict rules for column names
    if 'mysql' in engine.dialect.dialect_description:
        pytest.skip()

    for i in range(5):
        random_bad_col_name = ''.join([random.choice(bad_char_seq) for i in range(50)])
        df_test = (pd.DataFrame({random_bad_col_name: ['test', None]})
                   .rename_axis(['profileid'], axis='index', inplace=False))

        # psycopg2 can't process columns with "%" or "(" or ")" so we will need `fix_psycopg2_bad_cols`
        df_test = fix_psycopg2_bad_cols(df_test)
        with AutoDropTableContext(engine=engine, schema=schema, table_name=f'test_bad_col_names_{i}') as ctx:
            upsert(engine=engine, schema=schema, df=df_test, table_name=ctx.table_name, if_row_exists='update')


# -

# # Another test with the column name `values` (see issue #34 of pangres)

def test_column_named_values(engine, schema):
    df = pd.DataFrame({'values': range(5, 9)}, index=pd.Index(range(1, 5), name='idx'))
    with AutoDropTableContext(engine=engine, schema=schema, table_name='test_column_values') as ctx:
        upsert(engine=engine, schema=schema, df=df, if_row_exists='update', table_name=ctx.table_name)
