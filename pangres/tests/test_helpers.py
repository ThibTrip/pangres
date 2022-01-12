# -*- coding: utf-8 -*-
import pytest
from pangres.helpers import validate_chunksize_param


# # Tests

@pytest.mark.parametrize('value', [-1, 0, 10, 'abc'])
def test_valid_chunksize_values(_, value):
    # 10 is the only valid value here
    if value == 10:
        validate_chunksize_param(value)
        return
    # all the other values should fail
    with pytest.raises((TypeError, ValueError)):
        validate_chunksize_param(value)
