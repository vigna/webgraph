# Welcome to WebGraph!

## Introduction

WebGraph is a framework for graph compression aimed at studying web
graphs. It provides simple ways to manage very large graphs, exploiting
modern compression techniques. More precisely, it is currently made of:

1.  A set of flat codes, called _ζ codes_, which are particularly
suitable for storing web graphs (or, in general, integers with power-law
distribution in a certain exponent range). The fact that these codes work
well can be easily tested empirically, but we also try to provide a
[detailed mathematical
analysis](http://vigna.di.unimi.it/papers.php#BoVCWWW).

2.  Algorithms for compressing web graphs that exploit gap compression and
referentiation (à la [LINK](https://ieeexplore.ieee.org/document/999950)),
intervalisation and ζ codes to provide a high compression ratio (see [our
datasets](http://law.di.unimi.it/datasets.php)). The algorithms are
controlled by several parameters, which provide different tradeoffs
between access speed and compression ratio.

3.  Algorithms for accessing a compressed graph without actually
decompressing it, using lazy techniques that delay the decompression until
it is actually necessary.

4.  Algorithms for analysing very large graphs, such as
[HyperBall](http://vigna.di.unimi.it/papers.php#BoVHB)
which has been used to show that Facebook has just [four degrees of
separation](http://vigna.di.unimi.it/papers.php#BBRFDS).

5.  An implementation of the algorithms above in Java and
[Rust](https://www.rust-lang.org/) distributed under either the [GNU
Lesser General Public License
2.1+](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html) or the
[Apache Software License
2.0](https://www.apache.org/licenses/LICENSE-2.0). Besides a clearly
defined API, we also provide several classes tha modify (e.g., transpose)
or recompress a graph, so to experiment with various settings.

6.  [Datasets](http://law.di.unimi.it/) for large graph. These are either
gathered from public sources (such as
[WebBase](http://www-diglib.stanford.edu/~testbed/doc2/WebBase/)), or
produced by [UbiCrawler](http://law.di.unimi.it/ubicrawler) and BUbiNG.

In the end, with WebGraph you can access and analyse very large web
graphs. Using WebGraph is as easy as installing a few jar files and
downloading a dataset. This makes studying phenomena such as PageRank,
distribution of graph properties of the web graph, etc. very easy.

You are welcome to use and improve WebGraph! If you find our software
useful for your research, please quote [this
paper](http://vigna.di.unimi.it/papers.php#BoVWFI).

This version of WebGraph is limited to graphs with at most 2³¹ nodes. For
larger graphs, have a look at the [big
version](https://github.com/vigna/webgraph-big).

---

## Hadoop

[Helge Holzmann](http://www.helgeholzmann.de/) has developed an [input
format for Hadoop](https://github.com/helgeho/HadoopWebGraph/) for graphs
in [BVGraph](docs/it/unimi/dsi/webgraph/BVGraph.html) format.

---

## WebGraph++

Jacob Ratkievicz has developed a [C++ version of
WebGraph](http://cnets.indiana.edu/groups/nan/webgraph/) that you might
want to try. The library exposes a
[BVGraph](docs/it/unimi/dsi/webgraph/BVGraph.html) as an object of the
[Boost Graph Library](http://www.boost.org/libs/graph/doc/index.html), so
it is easily integrable with other code.

---

## pyWebgraph

[Massimo Santini](http://santini.di.unimi.it/) has developed a [front-end
that interfaces Jython with
WebGraph](http://code.google.com/p/py-web-graph/). It makes exploring
small portions of very large graphs very easy and interactive.

---

# Papers

* A [detailed description](http://vigna.di.unimi.it/papers.php#BoVWFI) of
the compression algorithms used in WebGraph, published in the proceedings
of the [Thirteenth International World–Wide Web
Conference](http://www2004.org).

* A [mathematical analysis](http://vigna.di.unimi.it/papers.php#BoVCWWW)
of the performance of γ, δ and ζ codes against power-law distributions.

* Some [quite surprising
experiments](http://vigna.di.unimi.it/papers.php#BSVPWSG) showing that the
transpose graph reacts very peculiarly to compression after
lexicographical or Gray-code sorting.

* A [paper](http://vigna.di.unimi.it/papers.php#BRVH) about
[HyperBall](http://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/algo/HyperBall.html)
(then named HyperANF), our tool for computing an approximation of the
neighbourhood function, reachable nodes and geometric centralities of
massive graphs. More information can be found in this
[preprint](http://vigna.di.unimi.it/papers.php#BoVHB).

* [HyperBall](docs/it/unimi/dsi/webgraph/algo/HyperBall.html) was used to
find out that on average there are just [four degrees of
separation](http://vigna.di.unimi.it/papers.php#BBRFDS) on
[Facebook](http://facebook.com/), and the experiment was reported by the
[New York
Times](http://nytimes.com/2011/11/22/technology/between-you-and-me-4-74-degrees.html).
Alas, the degrees were actually 3.74 (one less than the [average
distance](http://law.di.unimi.it/webdata/fb-current/)), but the off-by-one
between graph theory (“distance”) and sociology (“degrees of separation”)
generated a lot of confusion.

* A [paper](http://vigna.di.unimi.it/papers.php#BPVULCRAGC) were we
describe our efforts to compress one of the largest social graphs
available—the graph of commits of the Software Heritage archive.

* A [paper](http://vigna.di.unimi.it/papers.php#FVZWNG) about our effort
to bring WebGraph to [Rust](https://www.rust-lang.org/).
