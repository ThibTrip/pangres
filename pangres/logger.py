# +
import logging
import os

loggers = {}

def log(text, name='pangres', level=logging.INFO):
    """
    Logs given text to stderr (default of the logging library).

    Parameters
    ----------
    text : str
        Text to log
    name : str, default "pangres"
        Name of the logger
    level : int, default logging.INFO

    Notes
    -----
    This is heavily inspired from:
    https://github.com/SergeyPirogov/webdriver_manager/blob/master/webdriver_manager/logger.py

    Examples
    --------
    >>> log('info')

    >>> import logging
    >>> log('warning!', level=logging.WARNING)
    """
    # environment variable so user can customize the logging level of pangres
    log_level = os.getenv('PANGRES_LOG_LEVEL')
    if log_level:
        level = int(log_level)
    # only add a handler if we do not have one (otherwise we'd have duplicated logs)
    if loggers.get(name):
        loggers.get(name).info(text)
    else:
        _logger = logging.getLogger(name)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s | %(levelname)s     '
                                      '| %(name)s    | %(module)s:%(funcName)s:%(lineno)s '
                                      '- %(message)s')
        handler.setFormatter(formatter)
        _logger.addHandler(handler)
        _logger.setLevel(level)
        loggers[name] = _logger
        _logger.info(text)
