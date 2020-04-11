# Generating the documentation

1. Clone pangres' wiki: <code>git clone https://github.com/ThibTrip/pangres.wiki.git</code>

2. Install [npdoc_to_md](https://github.com/ThibTrip/npdoc_to_md) which will pull docstrings, convert them to markdown and render that in new markdown files (see [instructions for rendering a markdown file with placeholders](https://github.com/ThibTrip/npdoc_to_md/wiki/Render-file)) <code>pip install npdoc_to_md</code>

3. If necessary modify the unrendered markdown files in this folder (each file corresponds to a page in the wiki).

4. Run the notebook **generate_documentation.ipynb** in the same folder.