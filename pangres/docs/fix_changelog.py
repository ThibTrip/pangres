"""
Script for adding release notes to a changelog. Assumes the changelog has been generated using
`github-changelog-generator` (https://github.com/github-changelog-generator/github-changelog-generator).

Execute the command below first. Make sure to replace the following variables:
* $PATH_TO_PANGRES -> path to pangres repo on your computer (you have to clone it)
* `-t $GITHUB_TOKEN` -> optionally give a github token (for much higher API quota)
* $OUTPUT_PATH -> where to put the CHANGELOG.md file

sudo docker run -it --rm -v "$(pwd)":$PATH_TO_PANGRES githubchangeloggenerator/github-changelog-generator -u ThibTrip -p pangres -t $GITHUB_TOKEN -o $OUTPUT_PATH --release-url https://github.com/ThibTrip/pangres/releases/tag/%s

Usage:

python fix_changelog.py $PATH_TO_CHANGELOG
"""
import argparse
import re
import requests  # pip install requests
import sys
from loguru import logger  # pip install loguru
from pathlib import Path

# # Helpers

# +
re_section_release_notes = re.compile(r'^# [A-Z]{1,}')  # e.g. "# New Features" -> "# N"
re_release_title_md = re.compile(r'## \[(?P<version>v[\d\.]+)\]')  # see https://regex101.com/r/g6yRM8/1


def adjust_levels_release_notes(body):
    """
    Lowers the levels (markdown levels e.g. # Title, ## Sub-title )
    of the sections in the release notes.
    """
    new_body = []
    for line in body.splitlines():
        new_line = (line.replace('# ', '**_') + '_**' + '\n') if re_section_release_notes.search(line) else line
        new_body.append(new_line)
    return '\n'.join(new_body)


def get_release_notes(github_token=None):
    kwargs = dict(headers={'Authorization': f'token {github_token}'}) if github_token else {}
    response = requests.get('https://api.github.com/repos/ThibTrip/pangres/releases', **kwargs)
    response.raise_for_status()
    return {d['tag_name']:adjust_levels_release_notes(d['body']) for d in response.json()}


def add_release_notes_to_changelog(filepath, github_token=None, dryrun=False):
    # open original file
    with open(filepath, mode='r', encoding='utf-8') as fh:
        ch = fh.read()

    # get release notes in the form {'v4.1':...}
    release_notes = get_release_notes(github_token)

    # go through the lines, look for release titles (e.g. "## [v4.1]")
    # if we find release title, look for release notes and add them
    new_ch = []
    for line in ch.splitlines():
        match_version = re_release_title_md.search(line)
        # case of "normal" lines
        if not match_version:
            new_ch.append(line)
            continue
        # case of release title lines
        version = match_version['version']
        try:
            notes = release_notes[version]
            logger.info(f'Adding release notes for version {version}')
            new_ch.extend([line, '\n', '**Release Notes**', '\n', '___', release_notes[version], '___', '\n'])
        except KeyError:
            logger.warning(f'No release notes found for version {version}!')
            continue
    new_ch = '\n'.join(new_ch)
    if not dryrun:
        with open(filepath, mode='w', encoding='utf-8') as fh:
            fh.write(new_ch)
    else:
        print(new_ch)
    return new_ch


# -

# # Main

# +
def main():
    # parse arguments
    parser = argparse.ArgumentParser(description=sys.modules['__main__'].__doc__)
    parser.add_argument('filepath_change_log', metavar='filepath_change_log', type=str, help="Path to the changelog")
    parser.add_argument('--github_token', action="store", type=str, default=None, help='Optional github token for higher API quota')
    parser.add_argument('--dryrun', action="store_true", default=False, help='If True, simply prints what we would save otherwise overwrites the changelog')
    args = parser.parse_args()
    add_release_notes_to_changelog(filepath=Path(args.filepath_change_log).resolve(), github_token=args.github_token,
                                   dryrun=args.dryrun)

if __name__ == '__main__':
    main()
