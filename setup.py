import setuptools
import os

here = os.path.abspath(os.path.dirname(__file__))
description = 'Postgres insert update with pandas DataFrames.'

# Import the README and use it as the long-description.
try:
    with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = description


with open(os.path.join(here, "requirements.txt"),"r") as f:
    requirements = [line.strip() for line in f.readlines()]


setuptools.setup(
    name="pangres",
    version="4.0.1",
    license = 'The Unlicense',
    author="Thibault Bétrémieux",
    author_email="thibault.betremieux@gmail.com",
    url = 'https://github.com/ThibTrip/pangres',
    download_url = 'https://github.com/ThibTrip/pangres/archive/v4.0.1.tar.gz',
    keywords = ['pandas','postgres', 'mysql', 'sqlite'],
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires= requirements,
    classifiers=["Development Status :: 5 - Production/Stable",
                 "Programming Language :: Python :: 3",
                 "Programming Language :: Python :: 3.6",
                 "Programming Language :: Python :: 3.7",
                 "Programming Language :: Python :: 3.8",
                 "License :: Public Domain",
                 "Intended Audience :: Developers",
                 "Operating System :: OS Independent"])
