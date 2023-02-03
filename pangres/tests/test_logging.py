import logging
import os
import pytest
from contextlib import contextmanager
from pangres.logger import log, loggers


# # Helpers

# +
log_level_names = ('CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG')
log_level_values_to_names = {getattr(logging, k): k for k in log_level_names}


@contextmanager
def custom_logger(logger_level):
    # kick out any already initalized logger before setting the environment variable
    # not only from the `loggers` dict but also from the logging module as the handlers
    # will still be present and cause duplicates
    loggers.pop('pangres', None)
    logger = logging.getLogger('pangres')
    while logger.hasHandlers():
        try:
            logger.removeHandler(logger.handlers[0])
        except IndexError:
            break

    os.environ['PANGRES_LOG_LEVEL'] = str(logger_level)
    # make sure a logger is available (initialized if not present by the log function)
    log('DUMMY LOG', level=logging.DEBUG)
    try:
        yield
    finally:
        os.environ.pop('PANGRES_LOG_LEVEL', None)


# -

# # Tests

# +
# "_" is a dummy passed by metafunc when generating tests automatically
# for each engine and schema (see module conftests.py)
def test_bad_log_level(_):
    with pytest.raises(ValueError) as exc_info:
        log('test', level=5)
    assert 'not a valid log level' in str(exc_info.value)


# by default DEBUG is not shown
@pytest.mark.parametrize("test_level", [k for k, v in log_level_values_to_names.items() if v != 'DEBUG'],
                         scope='session')
def test_log_output(_, caplog, test_level):
    txt = 'TESTING LOG OUTPUT AND LEVEL'
    with caplog.at_level(logging.INFO):
        log(txt, level=test_level)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == log_level_values_to_names[test_level]
    assert txt in caplog.text


def test_custom_level(_, caplog):
    base_text = 'TESTING CUSTOM LOGGER LEVEL'
    with custom_logger(logger_level=logging.WARNING):
        # test lower log level -> should be hidden
        with caplog.at_level(logging.INFO):
            log(base_text + ' | THIS LOG SHOULD BE HIDDEN', level=logging.INFO)
        assert caplog.text == ''

        # test greater than or equal level
        expected = base_text + ' | THIS LOG SHOULD BE DISPLAYED'
        with caplog.at_level(logging.INFO):
            log(expected, level=logging.WARNING)
        assert len(caplog.records) == 1
        assert expected in caplog.text
