import pandas as pd
import pytest
from pangres.utils import fix_psycopg2_bad_cols
from pangres.exceptions import DuplicateLabelsException, UnnamedIndexLevelsException


# # Tests

# +
def test_unnamed_ix_levels_pg_column_names_fix(_):
    # make a df that has no name for its index
    df = pd.DataFrame({'id':[0]})
    with pytest.raises(UnnamedIndexLevelsException) as exc_info:
        fix_psycopg2_bad_cols(df=df)
    assert 'index levels must be named' in str(exc_info.value)


def test_duplicates_before_pg_column_names_fix(_):
    """
    Tests the case when the user passes a df with duplicated labels
    to `fix_psycopg2_bad_cols`
    """
    # make a df where "id" is both a column name and an index label
    df = pd.DataFrame({'id':[0]}).set_index('id')
    df['id'] = 0
    with pytest.raises(DuplicateLabelsException) as exc_info:
        fix_psycopg2_bad_cols(df=df)
    assert 'There cannot be duplicated' in str(exc_info.value)


def test_duplicates_after_pg_column_names_fix(_):
    """
    Tests the case when after cleaning the labels of a df
    with `fix_psycopg2_bad_cols` we end up with duplicated labels
    """
    # "test(" and "test)" will both be cleaned to "test" resulting in duplicate labels
    df = pd.DataFrame({'id':[0], 'test(':[0], 'test)':[0]}).set_index('id')
    with pytest.raises(DuplicateLabelsException) as exc_info:
        fix_psycopg2_bad_cols(df=df)
    assert 'not unique after renaming' in str(exc_info.value)


@pytest.mark.parametrize('replacements', [{'@':''}, False, None, {'(':1, ')':2, '%':3}])
def test_bad_replacements_for_pg_column_names_fix(_, replacements):
    """
    When the user gives a wrong object type or wrong replacements
    for `fix_psycopg2_bad_cols` (e.g. the user tries to replace
    the character "@" although this does not cause any problems with psycopg2)
    """
    df = pd.DataFrame({'id':[0]}).set_index('id')
    with pytest.raises(TypeError) as exc_info:
        fix_psycopg2_bad_cols(df=df, replacements=replacements)
    e_str = str(exc_info.value)
    assert ('must be a dict' in e_str or
            'values of replacements must all be strings' in e_str)