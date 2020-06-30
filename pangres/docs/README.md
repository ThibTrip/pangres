# Generating the documentation

Pangres uses a special syntax in markdown files so that the script **generate_documentation.py** can create parts of pangres' documentation using docstrings of functions/classes/methods. This is done via the library **npdoc_to_md**. Head over to [npdoc_to_md's wiki] (https://github.com/ThibTrip/npdoc_to_md/wiki) to understand how the syntax work.

Each Markdown file in this folder corresponds to a page in the wiki except for this README.md.

1. Install npdoc_to_md with <code>pip install npdoc_to_md</code> (in a virtual environment if you wish)

2. Clone pangres' wiki: <code>git clone https://github.com/ThibTrip/pangres.wiki.git</code>

3. Modify unrendered markdown files if needed (e.g. Upsert.md)

4. Run the script **generate_documentation.py**. It must be launched from the folder it is located in and it takes one argument which is the path to the folder where you cloned pangres' wiki: <code>python generate_documentation.py $wiki_path</code> e.g. <code>python generate_documentation.py "/home/thibtrip/pangres.wiki"</code>. This script requires the library npdoc_to_md which you should have installed by now.

5. Commit changes (if there are any) in the folder of the cloned pangres wiki.