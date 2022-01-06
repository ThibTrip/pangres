# +
import argparse
import os
import pkgutil
import sys

script_description = """
This script generates the documentation of pangres using the library npdoc_to_md (https://github.com/ThibTrip/npdoc_to_md)
to render markdown files (located in the same folder as this notebook) with Jinja like placeholders (docstrings of functions,
methods or classes are pulled and converted to pretty markdown).
"""


# -

# # Helpers

def resource_filename(package, resource):
    """
    Rewrite of pkgutil.get_data() that returns the file path.
    This replaces resource_filename from pkg_resources which is deprecated.

    Source: https://github.com/DLR-RY/pando-core/blob/master/pando/pkg.py
    (via https://dev.deluge-torrent.org/ticket/3252)
    """
    loader = pkgutil.get_loader(package)
    if loader is None or not hasattr(loader, 'get_data'):
        return None
    mod = sys.modules.get(package) or loader.load_module(package)
    if mod is None or not hasattr(mod, '__file__'):
        return None

    # Modify the resource name to be compatible with the loader.get_data
    # signature - an os.path format "filename" starting with the dirname of
    # the package's __file__
    parts = resource.split('/')
    parts.insert(0, os.path.dirname(mod.__file__))
    return os.path.normpath(os.path.join(*parts))


# # Parse arguments

def parse_arguments():
    parser = argparse.ArgumentParser(description=script_description)
    parser.add_argument('wiki_path', metavar='wiki_path', type=str, help="Path to pangres' cloned wiki folder on your computer")
    args = parser.parse_args()
    wiki_path = args.wiki_path
    return wiki_path


# # Render markdown files
#
# Each Markdown file corresponds to a wiki page except README.md

def generate_doc(wiki_path):
    from npdoc_to_md import render_md_file, get_markdown_files_in_dir

    script_dir = resource_filename('pangres.docs.generate_documentation', '')
    files_and_names = get_markdown_files_in_dir(script_dir)
    # do not render README.md this is the README for the docs folder on GitHub
    # so it is not a page for the Wiki
    files_and_names = {k:v for k,v in files_and_names.items() if v != 'README.md'}

    # simple verification that the page "Home" is present
    has_home_md = any(['home.md' in v.lower() for v in files_and_names.values()])
    if not has_home_md:
        raise AssertionError('Expected a Home.md page (case insensitive match) '
                             'and did not find any. The script searched for '
                             f'markdown files in the path "{script_dir}"')

    # render Markdowns
    for file, name in files_and_names.items():
        # render the file and put it a given destination
        destination = os.path.join(wiki_path, name)
        print(f'Rendering file "{file}"')
        render_md_file(source=file, destination=destination)
    print('Done')


# # Execute main function

if __name__ == '__main__':
    wiki_path = parse_arguments()
    generate_doc(wiki_path)
