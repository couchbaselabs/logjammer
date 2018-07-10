logmerge.py
===========

logmerge merges log files by timestamp to stdout or to a file

Command line help: logmerge.py -h

Examples...

> logmerge.py --out=out.log cbcollect*

logan.py
========

logan analyzes and plots multiple log files into a PNG image

Command line help: logan.py -h

Examples...

> logan.py cbcollect*

The above emits analysis files, and an out-logan-000.png image.

To start a web server on port 9999, using data from the previous
logan.py run...

> logan.py --http=9999 --repo=~/path/to/source/repo cbcollect*

Then, visit http://localhost:9999 in your web browser.

If you use the option --out-prefix=PREFIX, such as...

> logan.py --http=9999 --repo=~/path/to/source/repo --out-prefix=zzz cbcollect*

Then you also need to provide an outPrefix in your URL...

http://localhost:9999/?outPrefix=zzz


