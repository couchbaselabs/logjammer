logmerge.py
===========

logmerge merges log files by timestamp to stdout or to a file

command line help: logmerge.py -h

examples...

> logmerge.py --out=out.log cbcollect*

logan.py
========

logan analyzes and plots multiple log files into a PNG image

command line help: logan.py -h

examples...

> logan.py cbcollect*

> open out-000.png

to start a web server on port 9999, using info from the previous logan run...

> logan.py --http=9999 --repo=~/path/to/source/repo cbcollect*

