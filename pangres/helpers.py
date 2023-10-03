# # Versions checking

# +
def _py_gt3_10() -> bool:
    """
    Returns True if we are running Python >= 3.10 else return False.
    We need that due to different implementations for checking
    versions of other libraries
    """
    import sys
    py_v = sys.version_info
    # first condition is in case Python 4 comes out :P
    return (py_v.major > 3) or ((py_v.major >= 3) and (py_v.minor >= 10))


def _version_equal_or_greater_than(version_string: str, minimal_version_string: str) -> bool:
    """
    Returns True if a library has a version greater or equal than `minimal_version_string`.
    Otherwise returns False.

    To use this function, give the version string of the library in parameter `version_string`.

    Examples
    --------
    >>> _version_equal_or_greater_than('1.1', '2.0')
    False
    >>> _version_equal_or_greater_than('2.1', '2.0')
    True
    """
    if _py_gt3_10():
        from packaging.version import Version
        v = Version(version_string)
        min_v = Version(minimal_version_string)
        return (v.major, v.minor, v.micro) >= (min_v.major, min_v.minor, min_v.micro)
    else:
        from distutils.version import LooseVersion
        return LooseVersion(version_string) >= LooseVersion(minimal_version_string)


def _sqla_gt14() -> bool:
    """
    Checks if sqlalchemy.__version__ is at least 1.4.0, when several
    deprecations were made.

    Stolen from pandas.io.sql (we don't import it as it's private
    and has just 2 lines of code).
    """
    import sqlalchemy
    return _version_equal_or_greater_than(version_string=sqlalchemy.__version__,
                                          minimal_version_string='1.4.0')


def _sqla_gt20() -> bool:
    """
    Same as function `_sqla_gt14` for checking if sqlalchemy>=2.0.0
    """
    import sqlalchemy
    return _version_equal_or_greater_than(version_string=sqlalchemy.__version__,
                                          minimal_version_string='2.0.0')


def _sqlite_gt3_32_0() -> bool:
    """
    Checks if the SQLite version is >= than 3.32.0.
    Starting from this version we can use more SQL parameters.
    See https://github.com/ThibTrip/pangres/issues/43
    """
    import sqlite3
    return _version_equal_or_greater_than(version_string=sqlite3.sqlite_version,
                                          minimal_version_string='3.32.0')


# -

# # Parameters checking

def validate_chunksize_param(chunksize: int) -> None:
    if not isinstance(chunksize, int):
        raise TypeError(f'Expected chunksize to be an int. Got {type(chunksize)}')
    if chunksize <= 0:
        raise ValueError('chunksize must be strictly above 0')
