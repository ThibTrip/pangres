# +
import os
import argparse

script_description = """
This script generates the documentation of pangres using the library [npdoc_to_md](https://github.com/ThibTrip/npdoc_to_md)
to render markdown files (located in the same folder as this notebook) with Jinja like placeholders (docstrings of functions,
methods or classes are pulled and converted to pretty markdown).
"""
# -

# # Parse arguments

parser = argparse.ArgumentParser(description=script_description)
parser.add_argument('wiki_path', metavar='wiki_path', type=str, help="Path to pangres' cloned wiki folder on your computer")
args = parser.parse_args()
wiki_path = args.wiki_path


# # Render markdown files
#
# Each Markdown file corresponds to a wiki page except README.md

# +
def main():
    from npdoc_to_md import render_md_file, get_markdown_files_in_dir

    files_and_names = get_markdown_files_in_dir('.')
    # do not render README.md this is the README for the docs folder on GitHub
    # so it is not a page for the Wiki
    files_and_names = {k:v for k,v in files_and_names.items() if v != 'README.md'}

    for file, name in files_and_names.items():
        # render the file and put it a given destination
        destination = os.path.join(wiki_path, name)
        print(f'Rendering file "{file}"')
        render_md_file(source=file, destination=destination)
    print('Done')

if __name__ == '__main__':
    main()

