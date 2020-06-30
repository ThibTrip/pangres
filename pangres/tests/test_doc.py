import pytest
from pangres.docs import generate_documentation


# # Tests
#
# How do I prevent pytest from parameterizing this test with engine and schema?
# Those arguments are useless and the function runs three times which is quite stupid.

# +
@pytest.fixture()
def wiki_path(pytestconfig):
    return pytestconfig.getoption("wiki_path")

def test_generate_doc(engine, schema, pytestconfig):
    wiki_path = pytestconfig.getoption('wiki_path')
    if wiki_path is None:
        pytest.skip("Skipping doc generation test as a path to a cloned pangres' "
                    "wiki was not provided (argument --wiki_path)")
    generate_documentation.generate_doc(wiki_path)
