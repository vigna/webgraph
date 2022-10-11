Welcome to WebGraph!
--------------------

[WebGraph](http://webgraph.di.unimi.it/) is a framework for graph
compression aimed at studying web graphs. It provides simple ways to
manage very large graphs, exploiting modern compression techniques.

This version of WebGraph is limited to graphs with at most 2^31 nodes. For
larger graphs, have a look at the [big
version](https://github.com/vigna/webgraph-big).

Building
--------

You need [Ant](https://ant.apache.org/) and [Ivy](https://ant.apache.org/ivy/).

Then, run `ant ivy-setupjars jar` and set your `CLASSPATH` by `source setcp.sh`

seba (<mailto:sebastiano.vigna@unimi.it>)
