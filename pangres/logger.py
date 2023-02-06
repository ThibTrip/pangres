# +
"""
Module heavily inspired from:

https://github.com/SergeyPirogov/webdriver_manager/blob/master/webdriver_manager/logger.py
"""
import logging
import os

loggers = {}
log_method_switch = {logging.CRITICAL: 'critical',  # same level as fatal
                     logging.ERROR: 'error',
                     logging.WARNING: 'warning',
                     logging.INFO: 'info',
                     logging.DEBUG: 'debug'}


def log(text, name: str = 'pangres', level: int = logging.INFO):
    """
    Logs given text to stderr (default of the logging library).
    Parameters
    ----------
    text
        Text to log
    name
        The name of the logger
    level
        default logging.INFO

    Notes
    -----
    This is heavily inspired from:
    https://github.com/SergeyPirogov/webdriver_manager/blob/master/webdriver_manager/logger.py

    I wanted to do it with two functions as well (_init_handler and log) but could not get
    it to work somehow

    Examples
    --------
    * setting the log level of pangres via an environment variable
    >>> import os, logging
    >>> from pangres.logger import log
    >>> os.environ['PANGRES_LOG_LEVEL'] = str(logging.WARNING) # doctest: +SKIP
    >>>
    >>> # this won't log anything (INFO level < WARNING level)
    >>> log('info', level=logging.INFO)  # doctest: +SKIP
    >>>
    >>> # this will log something
    >>> log('warn', level=logging.WARNING)  # doctest: +SKIP
    """
    # get the appropriate log method (info, warning etc.)
    try:
        log_method = log_method_switch[level]
    except KeyError:
        raise ValueError(f'{level} is not a valid log level. See https://docs.python.org/3/library/logging.html')

    # environment variable so user can customize the logging level of pangres
    logger_level = os.getenv('PANGRES_LOG_LEVEL', logging.INFO)
    logger_level = int(logger_level) if isinstance(logger_level, str) else logger_level

    # init logger
    if name not in loggers:
        _logger = logging.getLogger(name)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s | %(levelname)s     '
                                      '| %(name)s    | %(module)s:%(funcName)s:%(lineno)s '
                                      '- %(message)s')
        handler.setFormatter(formatter)
        _logger.addHandler(handler)
        _logger.setLevel(logger_level)
        loggers[name] = _logger

    # log
    getattr(loggers[name], log_method)(text)
