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

the above emits analysis files, and an out-logan-000.png image

to start a web server on port 9999, using info from the previous logan run...

> logan.py --http=9999 --repo=~/path/to/source/repo cbcollect*

then, visit http://localhost:9999 in your web browser
