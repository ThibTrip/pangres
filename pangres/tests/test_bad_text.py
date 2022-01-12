# -*- coding: utf-8 -*-
# +
import pandas as pd
import pytest
import random

from pangres import upsert, fix_psycopg2_bad_cols
from pangres.exceptions import BadColumnNamesException
from pangres.tests.conftest import drop_table_for_test, TableNames
# -

# # Helpers

# list of characters that could potentially cause errors in column names or even values
bad_char_seq = """/_- ?§$&"',:;*()%[]{}|<>=!+#""" + "\\"


# # Add colums with bad names and insert bad text values

# +
@drop_table_for_test(TableNames.BAD_TEXT)
def test_bad_text_insert(engine, schema):
    # add bad text in a column named 'text'
    create_random_text = lambda: ''.join(random.choice(bad_char_seq) for i in range(10))
    df_test = (pd.DataFrame({'text': [create_random_text() for i in range(10)]})
               .rename_axis(['profileid'], axis='index', inplace=False))
    upsert(con=engine, schema=schema, table_name=TableNames.BAD_TEXT, df=df_test,
           if_row_exists='update')


# do the next test multiple times to try different combinations of bad characters
@pytest.mark.parametrize('iteration', range(5), ids=[f'iteration{i}' for i in range(5)])
@drop_table_for_test(TableNames.BAD_COLUMN_NAMES)
def test_bad_column_names(engine, schema, iteration):
    # add columns with bad names
    # don't do this for MySQL which has more strict rules for column names
    if 'mysql' in engine.dialect.dialect_description:
        pytest.skip()

    random_bad_col_name = ''.join(random.choice(bad_char_seq) for i in range(50))
    df_test = (pd.DataFrame({random_bad_col_name: ['test', None]})
               .rename_axis(['profileid'], axis='index', inplace=False))

    # psycopg2 can't process columns with "%" or "(" or ")" so we will need `fix_psycopg2_bad_cols`
    if 'postgres' in engine.dialect.dialect_description:
        df_test = fix_psycopg2_bad_cols(df_test)
    upsert(con=engine, schema=schema, df=df_test, table_name=TableNames.BAD_COLUMN_NAMES, if_row_exists='update')


@drop_table_for_test(TableNames.BAD_COLUMN_NAMES)
def test_bad_column_name_postgres_raises(engine, schema):
    if 'postgres' not in engine.dialect.dialect_description:
        pytest.skip()
    df = pd.DataFrame({'id':[0], '(test)':[0]}).set_index('id')
    with pytest.raises(BadColumnNamesException) as exc_info:
        upsert(con=engine, schema=schema, df=df, table_name=TableNames.BAD_COLUMN_NAMES, if_row_exists='update')
    assert 'does not seem to support column names with' in str(exc_info.value)


# -

# # Another test with the column name `values` (see issue #34 of pangres)

@drop_table_for_test(TableNames.COLUMN_NAMED_VALUES)
def test_column_named_values(engine, schema):
    df = pd.DataFrame({'values': range(5, 9)}, index=pd.Index(range(1, 5), name='idx'))
    upsert(con=engine, schema=schema, df=df, if_row_exists='update', table_name=TableNames.COLUMN_NAMED_VALUES)
