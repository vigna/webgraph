/*
 * Copyright (C) 2016-2023 Sebastiano Vigna
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.webgraph.algo;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.lang.EnumStringParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

/**
 * Computes the <var>k</var> most central vertices according to a <em>positive</em> {@linkplain Centrality geometric centrality}.
 * A survey about geometric centralities can be found
 * &ldquo;<a href="http://vigna.di.unimi.it/BoVAC">Axioms for centrality</a>&rdquo;,
 * by Paolo Boldi and Sebastiano Vigna, <i>Internet Math.</i>, 10(3-4):222&minus;262, 2014.
 *
 * <p>Note that usually one is interested in the <em>negative</em> version of a centrality measure, that is, the version
 * that depends on the <em>incoming</em> arcs. This class can compute only <em>positive</em> centralities: if you are
 * interested (as it usually happens) in the negative version, you must pass to this class the <em>transpose</em> of the graph.
 *
 * <p>In more detail, this class can compute the top <var>k</var> nodes for a centrality
 * out of {@link Centrality}. You must build a suitable instance using one of the
 * static factory method (i.e., {@link #newHarmonicCentrality(ImmutableGraph, int, int)}) and
 * then invoke {@link #compute()} on the instance. After the computation, the results will be available
 * in the public arrays {@link #centrality} and {@link #topK}.
 *
 * <p>The algorithm implemented in this class is the CutClos algorithm proposed by Michele
 * Borassi, Pierluigi Crescenzi and Andrea Marino in &ldquo;Fast and Simple
 * Computation of Top-<var>k</var> Closeness Centralities&rdquo;,
 * <i>CoRR</i>, abs/1507.01490, 2015.
 * The implementation performs a number of parallel breadth-first visits.
 *
 * <p>If <var>k</var> is small, the algorithm is much faster than the standard algorithm
 * which {@linkplain GeometricCentralities computes all centralities}. For example, if <var>k</var> is 1
 * the difference can
 * be several orders of magnitude. For bigger values of <var>k</var>, the performance
 * improvement decreases, and for <var>k</var> equal to the number of nodes the performance is the same as the
 * trivial algorithm that computes all centralities. In that case, you might consider using
 * an approximate algorithm like {@link HyperBall}.
 *
 * @author Michele Borassi
 */

public class TopKGeometricCentrality {
	private final static Logger LOGGER = LoggerFactory.getLogger(TopKGeometricCentrality.class);
	private static final boolean DEBUG = false;

	/**
	 * A thread that performs BFSes from some nodes, in order to compute their
	 * centralities, or to prove that these vertices are not in the top-k.
	 */
	@SuppressWarnings("hiding")
	private class GeometricCentralityThread extends Thread {
		private final int nn = TopKGeometricCentrality.this.nn;
		private final Centrality centralityType = TopKGeometricCentrality.this.centralityType;

		// These variables are used in a BFS, and we have to recycle them for
		// performance reasons.
		private final int dist[];
		private final int queue[];
		private final ImmutableGraph graph = TopKGeometricCentrality.this.graph.copy();
		private int neVis;
		private int nnVis;

		GeometricCentralityThread() {
			dist = new int[nn];
			queue = new int[nn];
			Arrays.fill(dist, -1);
		}

		/**
		 * A BFS from a vertex v, that is cut as soon as we can prove that v is
		 * not one of the <var>k</var> most central vertices.
		 *
		 * @param v
		 *            the starting vertex.
		 * @return the Lin/harmonic/exponential centrality of v if v can
		 *         be in the top-k, 0 otherwise.
		 */
		private double BFSCut(final int v) {
			if (this.graph.outdegree(v) == 0) {
				if (centralityType == Centrality.LIN) return 1;
				else return 0;
			}
			int x, y;
			int startQ = 0;
			int d = -1, gamma = this.graph.outdegree(v);
			double sumDist = 0, tildefL = 0, tildefU = 0;
			final double reachL = TopKGeometricCentrality.this.reachL[v],
					reachU = TopKGeometricCentrality.this.reachU[v];
			final int[] queue = this.queue;
			final int[] dist = this.dist;
			final int[] degs = TopKGeometricCentrality.this.degs;
			final Centrality centrality = this.centralityType;
			final double kth = TopKGeometricCentrality.this.kth;
			final double alpha = TopKGeometricCentrality.this.alpha;

			LazyIntIterator iter;

			// We reset variables that were modified in previous BFSes.
			for (int i = 0; i < nnVis; i++) dist[queue[i]] = -1;
			nnVis = 0;

			dist[v] = 0;
			queue[nnVis++] = v;

			while (startQ < nnVis) {
				x = queue[startQ++];
				iter = this.graph.successors(x);
				if (dist[x] > d) {
					d++;
					if (centrality == Centrality.LIN) {
						tildefL = ((sumDist - gamma + (d + 2) * (reachL - nnVis))) / (reachL * reachL);
						tildefU = ((sumDist - gamma + (d + 2) * (reachU - nnVis))) / (reachU * reachU);
						if (kth > 0 && tildefL >= 1.0 / kth && tildefU >= 1.0 / kth) {
							return -1;
						}
					} else if (centrality == Centrality.HARMONIC) {
						tildefL = sumDist + ((double) gamma) / (d + 1) + (reachU - gamma - nnVis) / (d + 2);
						if (tildefL <= kth) {
							return -1;
						}
					} else {
						tildefL = sumDist + gamma * Math.pow(alpha, d + 1) + (reachU - gamma - nnVis) * Math.pow(alpha, d + 2);
						if (tildefL <= kth) {
							return -1;
						}
					}
					gamma = 0;
				}
				while ((y = iter.nextInt()) != -1) {
					neVis++;
					if (dist[y] == -1) {
						dist[y] = dist[x] + 1;
						if (centrality == Centrality.LIN) {
							sumDist += dist[y];
						} else if (centrality == Centrality.HARMONIC) {
							sumDist += 1.0 / dist[y];
						} else {
							sumDist += Math.pow(alpha, dist[y]);
						}
						queue[nnVis++] = y;
						gamma += degs[y];
					} else {
						if (centrality == Centrality.LIN) {
							tildefL += 1 / (reachL * reachL);
							tildefU += 1 / (reachU * reachU);
							if (kth > 0 && tildefL >= 1.0 / kth && tildefU >= 1.0 / kth) {
								return -1;
							}
						} else if (centrality == Centrality.HARMONIC) {
							tildefL += 1.0 / (d + 2) - 1.0 / (d + 1);
							if (tildefL <= kth) {
								return -1;
							}
						} else {
							tildefL += Math.pow(alpha, d + 2) - Math.pow(alpha, d + 1);
							if (tildefL <= kth) {
								return -1;
							}
						}
					}
				}
			}
			if (centrality == Centrality.LIN) {
				return ((double) nnVis) * nnVis / sumDist;
			}
			return sumDist;
		}

		/*
		 * The main function run by each thread: it performs a BFSCut from each
		 * vertex, until no more new vertex is available.
		 */
		@Override
		public void run() {
			int v = nextVert();
			double centrality;
			while (v != -1) {
				neVis = 0;
				centrality = this.BFSCut(v);
				endBFS(v, centrality, neVis);
				v = nextVert();
			}
		}
	}

	/**
	 * The centralities with respect to which it is possible to find the top <var>k</var> nodes.
	 */
	public enum Centrality {
		/**
		 * Lin's Centrality: &ell;(<var>x</var>) =
		 * |<var>R</var>(<var>x</var>)|<sup>2</sup>&nbsp;&frasl;&nbsp;&nbsp;&sum;<sub><var>y</var>&isin;<var>R</var>(<var>x</var>)</sub>
		 * d(<var>x</var>,<var>y</var>), where <var>R</var>(<var>x</var>) is the set of nodes reachable from
		 * <var>x</var>. Note that for a strongly connected graph Lin's centrality is exactly Bavelas's
		 * closeness centrality, that is,
		 * 1&nbsp;&frasl;&nbsp;&nbsp;&sum;<sub><var>y</var></sub>d(<var>x</var>, <var>y</var>), multiplied
		 * by the square of the number of nodes.
		 */
		LIN,
		/**
		 * Harmonic Centrality: <var>h</var>(<var>x</var>) = &sum;<sub><var>y</var> &ne; <var>x</var></sub>
		 * 1&nbsp;&frasl;&nbsp;d(<var>x</var>,<var>y</var>).
		 */
		HARMONIC,
		/**
		 * Exponential Centrality: <var>e</var><sub>&alpha;</sub>(<var>x</var>) =
		 * &sum;<sub><var>y</var></sub> &alpha;<sup>d(<var>x</var>,<var>y</var>)</sup> for some real number
		 * &alpha; &isin; (0..1).
		 */
		EXPONENTIAL
	}

	/** The graph under examination. */
	private final ImmutableGraph graph;
	/** The number of nodes. */
	private final int nn;
	/** The global progress logger. */
	private final ProgressLogger pl;
	/** The number of vertices to be analyzed. */
	private final int k;
	/** The number of threads. */
	private final int threads;
	/** The kind of centrality to be computed. */
	private final Centrality centralityType;
	/** The exponent used (only if centrality==Centrality.EXPONENTIAL). */
	private final double alpha;
	/** Lower and upper bound on the number of reachable vertices. */
	private final int reachL[], reachU[];
	/** The degree of all vertices. */
	private final int degs[];
	/** List of all vertices sorted by degree. */
	private final int sortedVertDeg[];
	/** Number of vertices already processed. */
	private int finishedVisits;
	/** Vertex which is currently processed. */
	private int currentV;
	/** K-th biggest centrality found until now. */
	private double kth;
	/** The number of visited edges. */
	private long neVis;
	/**
	 * If <var>x</var> is one of the <var>k</var> most central vertices, {@code centrality[x]}
	 * will contain its centrality. On all other nodes, this array contains either -1 or
	 * the centrality of the node.
	 */
	public final double centrality[];
	/** The <var>k</var> most central vertices, from the most central to the least central. */
	public int topK[];

	/** The <var>k</var> most central vertices, from the less central to the most central. */
	private final IntHeapPriorityQueue topKQueue;

	/**
	 * Creates a new instance to compute the <var>k</var> most central vertices according
	 * to {@linkplain Centrality#LIN positive Lin's centrality}, logging every 10 seconds.
	 *
	 * @param g
	 *            the input graph.
	 * @param k
	 *            the number of vertices to be output.
	 * @param threads
	 *            the number of threads, or 0 for {@link Runtime#availableProcessors()}.
	 * @return the new instance.
	 */
	public static TopKGeometricCentrality newLinCentrality(final ImmutableGraph g, final int k, final int threads) throws IllegalArgumentException {
		return new TopKGeometricCentrality(g, k, Centrality.LIN, threads, 0.5, new ProgressLogger());
	}

	/**
	 * Creates a new instance to compute the <var>k</var> most central vertices according
	 * to {@linkplain Centrality#HARMONIC positive harmonic centrality}, logging every 10 seconds.
	 *
	 * @param g
	 *            the input graph.
	 * @param k
	 *            the number of vertices to be output.
	 * @param threads
	 *            the number of threads, or 0 for {@link Runtime#availableProcessors()}.
	 * @return the new instance.
	 */
	public static TopKGeometricCentrality newHarmonicCentrality(final ImmutableGraph g, final int k, final int threads) {
		return new TopKGeometricCentrality(g, k, Centrality.HARMONIC, threads, 0.5, new ProgressLogger());
	}

	/**
	 * Creates a new instance to compute the <var>k</var> most central vertices according
	 * to {@linkplain Centrality#EXPONENTIAL positive exponential centrality}, logging every 10 seconds.
	 *
	 * @param g
	 *            the input graph
	 * @param k
	 *            the number of vertices to be output.
	 * @param threads
	 *            the number of threads, or 0 for {@link Runtime#availableProcessors()}.
	 * @param alpha
	 *            the base used for the exponential centrality.
	 * @return the new instance.
	 */
	public static TopKGeometricCentrality newExponentialCentrality(final ImmutableGraph g, final int k, final double alpha, final int threads) {
		return new TopKGeometricCentrality(g, k, Centrality.EXPONENTIAL, threads, alpha, new ProgressLogger());
	}

	/**
	 * Creates a new instance.
	 *
	 * @param g
	 *            the input graph.
	 * @param k
	 *            the number of vertices required.
	 * @param centralityType
	 *            the type of centrality.
	 * @param threads
	 *            the number of threads, or 0 for {@link Runtime#availableProcessors()}.
	 * @param alpha
	 *            the exponent (used only if {@code centrality} is {@link Centrality#EXPONENTIAL}).
	 * @param pl
	 *            a progress logger, or {@code null}.
	 */
	public TopKGeometricCentrality(final ImmutableGraph g, final int k, final Centrality centralityType,
			final int threads, final double alpha, final ProgressLogger pl) {
		this.alpha = alpha;
		this.centralityType = centralityType;
		this.neVis = 0;
		this.finishedVisits = 0;
		this.kth = 0;
		this.pl = pl;
		graph = g;
		nn = graph.numNodes();
		this.k = Math.min(k, nn);
		centrality = new double[nn];
		topKQueue = new IntHeapPriorityQueue((x, y) -> (int) Math.signum(centrality[x] - centrality[y]));
		reachL = new int[nn];
		reachU = new int[nn];

		if (centralityType == Centrality.EXPONENTIAL && (alpha <= 0 || alpha >= 1))
			throw new IllegalArgumentException("The value alpha must be strictly between 0 and 1.");

		if (k <= 0) throw new IllegalArgumentException("k must be positive.");
		if (threads < 0) throw new IllegalArgumentException("The number of threads must not be negative.");
		else if (threads == 0) this.threads = Runtime.getRuntime().availableProcessors();
		else this.threads = threads;

		LOGGER.debug("Nodes: " + nn);
		LOGGER.debug("Arcs: " + graph.numArcs());

		computeReach();
		degs = new int[nn];

		for (int v = 0; v < nn; v++)
			degs[v] = graph.outdegree(v);

		sortedVertDeg = countingSort(degs);
		currentV = nn - 1;
	}

	/**
	 * Uses counting sort to sort the first n integers, according to their
	 * values.
	 *
	 * @param values
	 *            an array containing in position i the value of vertex i (which
	 *            is assumed to be between 0 and values.length)
	 * @return a permutation sorted[] of {0,...,values.length-1}, such that
	 *         values[sorted[i]] is non-decreasing with respect to i.
	 */
	private static int[] countingSort(final int[] values) {
		final int[] sorted = new int[values.length];
		final int numValues[] = new int[values.length + 1];
		for (final int i : values)
			numValues[i + 1]++;

		for (int i = 1; i < numValues.length; i++)
			numValues[i] += numValues[i - 1];

		for (int i = 0; i < values.length; i++)
			sorted[numValues[values[i]]++] = i;

		return sorted;
	}

	/**
	 * Computes a lower and an upper bound on the number of vertices reachable
	 * from each vertex v.
	 */
	private void computeReach() {
		final StronglyConnectedComponents scc = StronglyConnectedComponents.compute(graph, false, pl);
		final int nscc = scc.numberOfComponents;
		int sortedVerts[] = new int[nn];
		int v, vComp, w, wComp, i, maxSCC = 0;
		final int sccSizes[] = new int[nscc];
		final boolean visited[] = new boolean[nscc];
		final boolean reachMaxSCC[] = new boolean[nscc];
		final long lReachSCC[] = new long[nscc];
		final long uReachSCC[] = new long[nscc];
		final long uReachSCCWithoutMax[] = new long[nscc];
		LazyIntIterator iter;

		LOGGER.debug("There are " + nscc + " strongly connected components.");
		final IntArrayList sccGraph[] = new IntArrayList[nscc];

		for (i = 0; i < nscc; i++)
			sccGraph[i] = new IntArrayList();

		sortedVerts = countingSort(scc.component);
		i = 0;
		v = sortedVerts[i++];
		for (int contSCC = 0; contSCC < nscc; contSCC++) {
			while (scc.component[v] == contSCC) {
				sccSizes[contSCC]++;
				iter = graph.successors(v);
				while ((w = iter.nextInt()) != -1) {
					wComp = scc.component[w];
					if (!visited[wComp] && contSCC != wComp) {
						visited[wComp] = true;
						sccGraph[contSCC].add(wComp);
					}
				}
				if (i >= nn) break;
				v = sortedVerts[i++];
			}
			for (final int xComp : sccGraph[contSCC])
				visited[xComp] = false;

			if (sccSizes[contSCC] > sccSizes[maxSCC])
				maxSCC = contSCC;
		}

		// BFS from maxSCC to compute reachL[maxSCC], reachU[maxSCC] exactly
		final int[] queue = new int[nscc];
		int startQ = 0, endQ = 0;
		queue[endQ++] = maxSCC;
		visited[maxSCC] = true;
		while (startQ < endQ) {
			wComp = queue[startQ++];
			lReachSCC[maxSCC] += sccSizes[wComp];
			for (final int xComp : sccGraph[wComp]) {
				if (!visited[xComp]) {
					visited[xComp] = true;
					queue[endQ++] = xComp;
				}
			}
		}
		uReachSCC[maxSCC] = lReachSCC[maxSCC];
		reachMaxSCC[maxSCC] = true;

		// Dynamic programming to compute number of reachable vertices
		for (vComp = 0; vComp < nscc; vComp++) {
			if (vComp != maxSCC) {
				for (final int xComp : sccGraph[vComp]) {
					lReachSCC[vComp] = Math.max(lReachSCC[vComp], lReachSCC[xComp]);
					if (!visited[xComp]) uReachSCCWithoutMax[vComp] += uReachSCCWithoutMax[xComp];
					uReachSCC[vComp] += uReachSCC[xComp];
					uReachSCC[vComp] = Math.min(uReachSCC[vComp], nn);
					reachMaxSCC[vComp] = reachMaxSCC[vComp] || reachMaxSCC[xComp];
				}
				lReachSCC[vComp] += sccSizes[vComp];
				uReachSCC[vComp] += sccSizes[vComp];
				if (!visited[vComp]) uReachSCCWithoutMax[vComp] += sccSizes[vComp];
				if (reachMaxSCC[vComp]) uReachSCC[vComp] = uReachSCC[maxSCC] + uReachSCCWithoutMax[vComp];
				uReachSCC[vComp] = Math.min(uReachSCC[vComp], nn);
			}
		}

		// Store all results obtained in reachL, reachU.
		for (v = 0; v < nn; v++) {
			vComp = scc.component[v];
			reachL[v] = (int) Math.min(lReachSCC[vComp], nn);
			reachU[v] = (int) Math.min(uReachSCC[vComp], nn);
		}
	}

	/**
	 * Checks that the bounds reachL and reachU are correct.
	 */
	void checkReachLU() {
		final int queue[] = new int[nn];
		for (int v = 0; v < nn; v++) {
			int startQ = 0, endQ = 0, x, y;
			LazyIntIterator iter;
			final boolean visited[] = new boolean[nn];
			visited[v] = true;
			queue[endQ++] = v;

			while (startQ < endQ) {
				x = queue[startQ++];
				iter = this.graph.successors(x);

				while ((y = iter.nextInt()) != -1) {
					if (!visited[y]) {
						visited[y] = true;
						queue[endQ++] = y;
					}
				}
			}
			assert reachL[v] <= startQ;
			assert reachU[v] >= startQ;
		}
	}

	/**
	 * When a thread asks, it returns the next vertex to be analyzed.
	 *
	 * @return a vertex
	 */
	private synchronized int nextVert() {
		if (currentV >= 0) {
			return this.sortedVertDeg[currentV--];
		}
		return -1;
	}

	/**
	 * Updates the values when a BFS is terminated, and if requested it outputs
	 * some data.
	 *
	 * @param v
	 *            the starting vertex
	 * @param centrality
	 *            the centrality of v, or 0 if v is not in the top-k.
	 */
	private synchronized void endBFS(final int v, final double centrality, final int neVis) {
		this.neVis += neVis;
		if (pl != null) pl.update();
		this.centrality[v] = centrality;
		if (centrality >= 0) {
			topKQueue.enqueue(v);
			if (topKQueue.size() > k) topKQueue.dequeueInt();
			if (topKQueue.size() == k) kth = this.centrality[topKQueue.firstInt()];
		}

		if (DEBUG) {
			LOGGER.debug("Finished visit " + ++finishedVisits + "/" + nn + ":");
			LOGGER.debug("Vertex: " + v + " (degree: " + degs[v] + ")");
			LOGGER.debug("Current " + k + "-th centrality: " + kth);
			LOGGER.debug("Current vertex centrality: " + centrality);
			LOGGER.debug("Current improvement (approx): " + (((double) graph.numArcs()) * finishedVisits / neVis));
		}
	}

	/**
	 * Compute top-<var>k</var> geometric centralities.
	 */
	public void compute() {
		if (pl != null) {
			pl.start("Starting visits...");
			pl.itemsName = "nodes";
			pl.displayLocalSpeed = true;
		}

		final GeometricCentralityThread[] threads = new GeometricCentralityThread[this.threads];

		for (int i = 0; i < this.threads; i++) {
			threads[i] = new GeometricCentralityThread();
			threads[i].start();
		}
		for (int i = 0; i < this.threads; i++) {
			try {
				threads[i].join();
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}
		topK = new int[this.topKQueue.size()];

		for (int i = this.topKQueue.size()-1; i >= 0; i--)
			topK[i] = this.topKQueue.dequeueInt();

		if (pl != null) pl.done();
	}



	public static void main(final String args[]) throws JSAPException, IOException {

		final SimpleJSAP jsap = new SimpleJSAP(TopKGeometricCentrality.class.getName(), "Computes top-k central vertices according to different positive geometric centrality measures. Outputs a file with extension .nodes containing the top k nodes (most central nodes first), and a file with extension .values containing the corresponding centralities.\nPlease note that to compute negative centralities on directed graphs (which is usually what you want) you have to compute positive centralities on the transpose.",
		new Parameter[] {
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new Switch("text", 't', "text", "If true, a human-readable text file is produced, otherwise two binary files containing nodes and centralities."),
			new FlaggedOption("k", JSAP.INTSIZE_PARSER, "1", JSAP.NOT_REQUIRED, 'k', "k", "The number of vertices to be output"),
			new FlaggedOption("centrality", EnumStringParser.getParser(Centrality.class, true), Centrality.HARMONIC.name(), JSAP.REQUIRED, 'c', "centrality", Arrays.toString(Centrality.values())),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
 			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, "0.5", JSAP.NOT_REQUIRED, 'a', "alpha", "The value of alpha for exponential centrality (ignored, otherwise)."),
			new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("outputBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The output basename."),
		});

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("graphBasename");
		final int k = jsapResult.getInt("k");
		final String outputBasename = jsapResult.getString("outputBasename");
		final Centrality centrality = Enum.valueOf(Centrality.class, jsapResult.getObject("centrality").toString().toUpperCase());
		int threads = jsapResult.getInt("threads");
		final long logInterval = jsapResult.getLong("logInterval");
		final double alpha = jsapResult.getDouble("alpha");

		if (centrality == Centrality.EXPONENTIAL && (alpha <= 0 || alpha >= 1))
			throw new IllegalArgumentException("The value alpha must be strictly between 0 and 1.");

		if (threads == 0) threads = Runtime.getRuntime().availableProcessors();

		TopKGeometricCentrality c;
		ImmutableGraph g = ImmutableGraph.load(basename);
		if (jsapResult.userSpecified("expand")) g = new ArrayListMutableGraph(g).immutableView();

		final ProgressLogger pl = new ProgressLogger(LOGGER, logInterval, TimeUnit.MILLISECONDS, "nodes");

		c = new TopKGeometricCentrality(g, k, centrality, threads, alpha, pl);
		c.compute();

		if (jsapResult.getBoolean("text")) {
			final PrintStream outputNodes = new PrintStream(outputBasename + ".nodes");
			final PrintStream outputValues = new PrintStream(outputBasename + ".values");
			for (final int v : c.topK) {
				outputNodes.println(v);
				outputValues.println(c.centrality[v]);
			}
			outputNodes.close();
			outputValues.close();
		} else {
			final DataOutputStream outputNodes = new DataOutputStream(new FileOutputStream(outputBasename + ".nodes"));
			final DataOutputStream outputValues = new DataOutputStream(new FileOutputStream(outputBasename + ".values"));
			for (final int v : c.topK) {
				outputNodes.writeInt(v);
				outputValues.writeDouble(c.centrality[v]);
			}
			outputNodes.close();
			outputValues.close();
		}

		LOGGER.info("\nFinal improvement: " + ((double) c.nn) * c.graph.numArcs() / c.neVis);
	}
}
