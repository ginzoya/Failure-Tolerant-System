This directory contains sample code for the final project for the
final project in CMPT 474, Spring 2015.  The project is
described at http://474.cmpt.sfu.ca/public/failtol.html

Files:

* README.md: This description

Demonstration of tools for final project:

* cmpt474-build-readme.txt: Instructions for installing libraries.
* frontend.py*: Sample frontend, demonstrating use of libraries.
* gen_ports.py: Function to compute port numbers for publish/subscribe.
* proxy.py*: Proxy process to (optionally) run, duplicating and reordering pub/sub messages.
* retrieve.py: Mock database retrieve function. Only for testing.
* table.py: Mock database table. Only for testing.

CounterLast---Extended kazoo.recipe.counter.Counter:

* counterlast.py: Extended Counter that includes `last_set` property.
* kazooclientlast.py: Kazoo client including CounterLast support.

Test code for counterlast.py:

* testcounterlast*: Script to run test. Single argument is number of trials.
* testcounterlast.py*: Client to drive test.
* checktest.py*: Utility that checks test results. No output means successful test.
