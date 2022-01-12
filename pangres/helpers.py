from distutils.version import LooseVersion


# # Versions checking

# +
def _sqla_gt14() -> bool:
    """
    Checks if sqlalchemy.__version__ is at least 1.4.0, when several
    deprecations were made.

    Stolen from pandas.io.sql (we don't import it as it's private
    and has just 2 lines of code).
    """
    import sqlalchemy
    return LooseVersion(sqlalchemy.__version__) >= LooseVersion("1.4.0")


def _sqlite_gt3_32_0() -> bool:
    """
    Checks if the SQLite version is >= than 3.32.0.
    Starting from this version we can use more SQL parameters.
    See https://github.com/ThibTrip/pangres/issues/43
    """
    import sqlite3
    return LooseVersion(sqlite3.sqlite_version) >= LooseVersion("3.32.0")


# -

# # Parameters checking

def validate_chunksize_param(chunksize:int):
    if not isinstance(chunksize, int):
        raise TypeError(f'Expected chunksize to be an int. Got {type(chunksize)}')
    if chunksize <= 0:
        raise ValueError('chunksize must be strictly above 0')
