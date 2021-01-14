/*
 * Copyright (C) 2016-2020 Sebastiano Vigna
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

import java.io.IOException;
import java.util.Arrays;

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
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.EnumStringParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.Transform;

/**
 * Computes the radius and/or the diameter and/or all eccentricities of a graph,
 * using the SumSweep algorithm described by Michele Borassi, Pierluigi
 * Crescenzi, Michel Habib, Walter A. Kosters, Andrea Marino, and Frank W. Takes
 * in &ldquo;Fast diameter and radius BFS-based computation in (weakly
 * connected) real-world graphs&mdash;With an application to the six degrees of
 * separation games &rdquo;, <i>Theoretical Computer Science</i>,
 * 586:59&minus;80, 2015.
 *
 * <p>
 * We define the <em>positive</em>, or <em>forward</em> (resp.,
 * <em>negative</em>, or <em>backward</em>) <em>eccentricity</em> of a node
 * <var>v</var> in a graph <var>G</var>=(<var>V</var>,<var>E</var>) as ecc
 * <sup>+</sup>(<var>v</var>)=max<sub><var>w</var> reachable from
 * <var>v</var></sub> d(<var>v</var>,<var>w</var>) (resp., ecc<sup>&minus;</sup>(
 * <var>v</var>)=max<sub><var>w</var> reaches <var>v</var></sub> <var>d</var>(
 * <var>w</var>,<var>v</var>)), where <var>d</var>(<var>v</var>,<var>w</var>) is
 * the number of edges in a shortest path from <var>v</var> to <var>w</var>. The
 * diameter is max <sub><var>v</var>&isin;<var>V</var></sub> ecc<sup>+</sup>(
 * <var>v</var>), which is also equal to max <sub><var>v</var>&isin;
 * <var>V</var></sub> ecc<sup>&minus;</sup>(<var>v</var>), while the radius is min
 * <sub><var>v</var>&isin;<var>V</var>'</sub> ecc(<var>v</var>), where
 * <var>V</var>' is a set of vertices specified by the user. These definitions
 * are slightly different from the standard ones due to the restriction to
 * reachable nodes. In particular, if we simply define the radius as the minimum
 * eccentricity, the radius of a graph containing a vertex with out-degree 0
 * would be 0, and this does not make much sense. For this reason, we restrict
 * our attention only to a subset <var>V</var>' of the set of all vertices: by
 * choosing a suitable <var>V</var>', we can specialize this definition to all
 * definitions proposed in the literature. If <var>V</var>' is not specified, we
 * include in <var>V</var>' all vertices from which it is possible to reach the
 * largest strongly connected component, as suggested in the aforementioned
 * paper.
 *
 * <p>
 * Our algorithm performs some BFSs from "clever" vertices, and uses these BFSs
 * to bound the eccentricity of all vertices. More specifically, for each vertex
 * <var>v</var>, this algorithm keeps a lower and an upper bound on the forward
 * and backward eccentricity of <var>v</var>, named <var>lF</var>[<var>v</var>],
 * <var>lB</var>[<var>v</var>], <var>uF</var>[<var>v</var>], and <var>uB</var>[
 * <var>v</var>]. Furthermore, it keeps a lower bound <var>dL</var> on the
 * diameter and an upper bound <var>rU</var> on the radius. At each step, the
 * algorithm performs a BFS, and it updates all these bounds: the radius is
 * found as soon as <var>rU</var> is smaller than the minimum value of
 * <var>lF</var>, and the diameter is found as soon as <var>dL</var> is bigger
 * than <var>uF</var>[<var>v</var>] for each <var>v</var>, or <var>dL</var> is
 * bigger than <var>uB</var>[<var>v</var>] for each <var>v</var>.
 *
 * <p>
 * More specifically, the upper bound on the radius (resp., lower bound on the
 * diameter) is defined as the minimum forward (resp., maximum forward or
 * backward) eccentricity of a vertex from which we performed a BFS. Moreover,
 * if we perform a forward (resp., backward) BFS from a vertex <var>s</var>, we
 * update <var>lB</var>[<var>v</var>]=max(<var>lB</var>[<var>v</var>], d(
 * <var>s</var>, <var>v</var>)) (resp., <var>lF</var>[<var>v</var>]=max(
 * <var>lF</var>[<var>v</var>], d(<var>v</var>, <var>s</var>)). Finally, for the
 * upper bounds, we use a more complicated procedure that handles different
 * strongly connected components separately.
 *
 * <p>
 * To use this class, it is enough to create an instance, and then invoke
 * {@link #compute()}. It is possible to choose between the following stopping
 * conditions:
 * <ul>
 * <li>only the radius is found;</li>
 * <li>only the diameter is found;</li>
 * <li>radius and diameter are found;</li>
 * <li>all forward eccentricities are found;</li>
 * <li>all eccentricities are found.</li>
 * </ul>
 *
 * <p>
 * After the method {@link #compute()} is run, the output can be obtained
 * through the methods {@link #getRadius()} for the radius, {@link #getRadialVertex()} for a
 * radial vertex, {@link #getDiameter()} for the diameter, {@link #getDiametralVertex()} for a
 * vertex whose (forward or backward) eccentricity equals the diameter,
 * {@link #getEccentricity(int, boolean)} for the forward or backward eccentricities.
 * Similarly, one can use the methods {@link #getRadiusIterations()} An exception is raised
 * if the field has not been computed.
 *
 * <h2>Performance issues</h2>
 *
 * <p>
 * Although the running-time is <var>O</var>(<var>mn</var>) in the worst-case,
 * the algorithm is usually much more efficient on real-world networks, when
 * only radius and diameter are needed. If all eccentricities are needed, the
 * algorithm could be faster than <var>O</var>(<var>mn</var>), but in many
 * networks it achieves performances similar to the textbook algorithm, that
 * performs a breadth-first search from each node.
 *
 * @author Michele Borassi
 */

public class SumSweepDirectedDiameterRadius {
	private static final boolean DEBUG = true;

	/**
	 * TODO: find better way to do it
	 * Returns the index <var>i</var> such that
	 * <var>vec</var>[<var>i</var>] is maximum.
	 *
	 * @param vec
	 *            the vector of which we want to compute the argMax
	 * @return the value <var>i</var> such that <var>vec</var>[<var>i</var>] is
	 *         maximum
	 */
	public static int argMax(final double[] vec) {
		double max = -Double.MAX_VALUE;
		int argMax = -1;
		for (int i = 0; i < vec.length; i++) {
			if (vec[i] > max) {
				argMax = i;
				max = vec[i];
			}
		}
		return argMax;
	}

	/**
	 * Returns the index <var>i</var> such that <var>vec</var>[<var>i</var>] is
	 * maximum.
	 *
	 * @param vec
	 *            the vector of which we want to compute the argMax
	 * @return the value <var>i</var> such that <var>vec</var>[<var>i</var>] is
	 *         maximum
	 */
	public static int argMax(final int[] vec) {
		int max = Integer.MIN_VALUE;
		int argMax = -1;
		for (int i = 0; i < vec.length; i++) {
			if (vec[i] > max) {
				argMax = i;
				max = vec[i];
			}
		}
		return argMax;
	}

	/**
	 * Returns the index <var>i</var> such that <var>vec</var>[<var>i</var>] is
	 * maximum, among all indices such that <var>acc</var>[<var>i</var>] is
	 * true. In case of tie, the index maximizing <var>tieBreak</var> is chosen.
	 *
	 * @param vec
	 *            the vector of which we want to compute the argMax
	 * @param tieBreak
	 *            the tiebreak vector
	 * @param acc
	 *            the vector used to decide if an index is acceptable: a
	 *            negative value means that the vertex is acceptable
	 * @return the value <var>i</var> such that <var>vec</var>[<var>i</var>] is
	 *         maximum
	 */
	public static int argMax(final int[] vec, final int[] tieBreak, final boolean acc[]) {

		int max = Integer.MIN_VALUE, maxTieBreak = Integer.MIN_VALUE, argMax = -1;
		for (int i = 0; i < vec.length; i++) {
			if (acc[i] && (vec[i] > max || (vec[i] == max && tieBreak[i] > maxTieBreak))) {
				argMax = i;
				max = vec[i];
				maxTieBreak = tieBreak[i];
			}
		}
		return argMax;
	}

	/**
	 * Returns the index <var>i</var> such that <var>vec</var>[<var>i</var>] is
	 * minimum, among all indices such that <var>acc</var>[<var>i</var>] is
	 * true. In case of tie, the index minimizing <var>tieBreak</var> is chosen.
	 *
	 * @param vec
	 *            the vector of which we want to compute the argMax
	 * @param tieBreak
	 *            the tiebreak vector
	 * @param acc
	 *            the vector used to decide if an index is acceptable: a
	 *            negative value means that the vertex is acceptable
	 * @return the value <var>i</var> such that <var>vec</var>[<var>i</var>] is
	 *         maximum
	 */
	public static int argMin(final int[] vec, final int[] tieBreak, final boolean acc[]) {

		int min = Integer.MAX_VALUE, minTieBreak = Integer.MAX_VALUE, argMin = -1;
		for (int i = 0; i < vec.length; i++) {
			if (acc[i] && (vec[i] < min || (vec[i] == min && tieBreak[i] < minTieBreak))) {
				argMin = i;
				min = vec[i];
				minTieBreak = tieBreak[i];
			}
		}
		return argMin;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(SumSweepDirectedDiameterRadius.class);

	/**
	 * The type of output requested: radius, diameter, radius and diameter, all
	 * forward eccentricities, or all (forward and backward) eccentricities.
	 */
	public enum OutputLevel {
		/**
		 * Computes only the radius of the graph.
		 */
		RADIUS,
		/**
		 * Computes only the diameter of the graph.
		 */
		DIAMETER,
		/**
		 * Computes both radius and diameter.
		 */
		RADIUS_DIAMETER,
		/**
		 * Computes the radius, the diameter, and all the forward
		 * eccentricities.
		 */
		ALL_FORWARD,
		/**
		 * Computes the radius, the diameter, and all the (forward and backward)
		 * eccentricities.
		 */
		ALL
	}

	/** The graph under examination. */
	private final ImmutableGraph graph;
	/** The reversed graph. */
	private final ImmutableGraph revgraph;
	/** The number of nodes. */
	private final int nn;
	/** The global progress logger. */
	private final ProgressLogger pl;
	/** The kind of output requested. */
	private final OutputLevel output;
	/** The array of forward eccentricity value. */
	private final int[] eccF;
	/** The array of backward eccentricity value. */
	private final int[] eccB;
	/**
	 * <var>toCompleteF</var>[<var>v</var>] is <var>True</var> if and only if
	 * the forward eccentricity of <var>v</var> is not guaranteed, yet.
	 */
	private final boolean[] toCompleteF;
	/**
	 * <var>toCompleteB</var>[<var>v</var>] is <var>True</var> if and only if
	 * the backward eccentricity of <var>v</var> is not guaranteed, yet.
	 */
	private final boolean[] toCompleteB;
	/** The set of vertices that can be radial vertices. */
	private final boolean[] accRadial;
	/** The queue used for each BFS (it is recycled to save some time). */
	private final int[] queue;
	/**
	 * The array of distances, used in each BFS (it is recycled to save some
	 * time).
	 */
	private final int[] dist;
	/** Upper bound on the radius of the graph. */
	private int dL;
	/** Upper bound on the radius of the graph. */
	private int rU;
	/** A vertex whose eccentricity equals the diameter. */
	private int dV;
	/** A vertex whose eccentricity equals the radius. */
	private int rV;
	/** Number of iterations performed until now. */
	private int iter;
	/** Lower bound on the forward eccentricity. */
	protected int lF[];
	/** Upper bound on the forward eccentricity. */
	protected int uF[];
	/** Lower bound on the backward eccentricity. */
	protected int lB[];
	/** Upper bound on the backward eccentricity. */
	protected int uB[];
	/** Number of iteration before the radius is found. */
	private int iterR;
	/** Number of iteration before the diameter is found. */
	private int iterD;
	/** Number of iteration before all forward eccentricities are found. */
	private int iterAllF;
	/** Number of iteration before all eccentricities are found. */
	private int iterAll;
	/** Strongly connected components of the graph. */
	private final StronglyConnectedComponents scc;
	/** The strongly connected components digraph. */
	private final int[][] sccGraph;
	/**
	 * For each edge in the SCC graph, the start vertex of a corresponding edge
	 * in the graph
	 */
	private final int[][] startBridges;
	/**
	 * For each edge in the SCC graph, the end vertex of a corresponding edge in
	 * the graph
	 */
	private final int[][] endBridges;

	/**
	 * Total forward distance from already processed vertices (used as tie-break
	 * for the choice of the next vertex to process).
	 */
	private final int totDistF[];
	/**
	 * Total backward distance from already processed vertices (used as
	 * tie-break for the choice of the next vertex to process).
	 */
	private final int totDistB[];

	/**
	 * Creates a new class for computing diameter and/or radius and/or all
	 * eccentricities.
	 *
	 * @param graph
	 *            a graph.
	 * @param pl
	 *            a progress logger, or {@code null}.
	 * @param output
	 *            which output is requested: radius, diameter, radius and
	 *            diameter, or all eccentricities.
	 * @param accRadial
	 *            the set of vertices that can be considered radial vertices. If
	 *            null, the set is automatically chosen as the set of vertices
	 *            that are in the biggest strongly connected component, or that
	 *            are able to reach the biggest strongly connected component.
	 */
	public SumSweepDirectedDiameterRadius(final ImmutableGraph graph, final OutputLevel output,
			final boolean[] accRadial, final ProgressLogger pl) {
		this.pl = pl;
		this.graph = graph;
		this.revgraph = Transform.transpose(graph);
		this.nn = graph.numNodes();
		this.eccF = new int[nn];
		this.eccB = new int[nn];
		totDistF = new int[nn];
		totDistB = new int[nn];
		lF = new int[nn];
		lB = new int[nn];
		uF = new int[nn];
		uB = new int[nn];
		toCompleteF = new boolean[nn];
		toCompleteB = new boolean[nn];
		queue = new int[nn];
		dist = new int[nn];
		scc = StronglyConnectedComponents.compute(graph, false, null);
		startBridges = new int[scc.numberOfComponents][];
		endBridges = new int[scc.numberOfComponents][];
		sccGraph = new int[scc.numberOfComponents][];

		Arrays.fill(eccF, -1);
		Arrays.fill(eccB, -1);
		Arrays.fill(uF, nn + 1);
		Arrays.fill(uB, nn + 1);
		Arrays.fill(toCompleteF, true);
		Arrays.fill(toCompleteB, true);
		this.dL = 0;
		this.rU = Integer.MAX_VALUE;
		this.output = output;
		iterR = -1;
		iterD = -1;
		iterAllF = -1;
		iterAll = -1;

		if (accRadial == null) {
			this.accRadial = new boolean[nn];
			computeAccRadial();
		} else if (accRadial.length != nn)
			throw new IllegalArgumentException(
					"The size of the array of acceptable vertices must be equal to the number of nodes in the graph.");
		else {
			this.accRadial = accRadial;
		}

		findEdgesThroughSCC();
	}

	/**
	 * Returns the radius of the graph, if it has already been computed
	 * (otherwise, an exception is raised).
	 *
	 * @return the radius
	 */
	public int getRadius() {
		if (iterR == -1) {
			throw new UnsupportedOperationException("The radius has not been"
					+ "computed, yet. Please, run the compute method with" + "the correct output.");
		}
		return rU;
	}

	/**
	 * Returns the diameter, if it has already been computed (otherwise, an
	 * exception is raised).
	 *
	 * @return the diameter
	 */
	public int getDiameter() {
		if (iterD == -1) {
			throw new UnsupportedOperationException("The diameter has not been"
					+ "computed, yet. Please, run the compute method with" + "the correct output.");
		}
		return dL;
	}

	/**
	 * Returns a radial vertex, if it has already been computed (otherwise, an
	 * exception is raised).
	 *
	 * @return a radial vertex
	 */
	public int getRadialVertex() {
		if (iterR == -1) {
			throw new UnsupportedOperationException("The radius has not been"
					+ "computed, yet. Please, run the compute method with" + "the correct output.");
		}
		return rV;
	}

	/**
	 * Returns a diametral vertex, if it has already been computed (otherwise,
	 * an exception is raised).
	 *
	 * @return a diametral vertex
	 */
	public int getDiametralVertex() {
		if (iterD == -1) {
			throw new UnsupportedOperationException("The radius has not been"
					+ "computed, yet. Please, run the compute method with" + "the correct output.");
		}
		return dV;
	}

	/**
	 * Returns the eccentricity of a vertex, if it has already been computed
	 * (otherwise, an exception is raised).
	 *
	 * @param v
	 *            the vertex
	 * @param forward
	 *            if <var>True</var>, the forward eccentricity is returned,
	 *            otherwise the backward eccentricity
	 * @return the eccentricity of <var>v</var>
	 */
	public int getEccentricity(final int v, final boolean forward) {
		final int ecc = forward ? eccF[v] : eccB[v];

		if (ecc == -1) {
			throw new UnsupportedOperationException("The eccentricity of v has not been"
					+ "computed, yet. Please, use the compute method with" + "the correct output.");
		}
		return ecc;
	}

	/**
	 * Returns the number of iteration needed to compute the radius, if it has
	 * already been computed (otherwise, an exception is raised).
	 *
	 * @return the number of iterations before the radius is found
	 */
	public int getRadiusIterations() {
		if (iterR == -1) {
			throw new UnsupportedOperationException("The radius has not been "
					+ "computed, yet. Please, run the compute method with " + "the correct output.");
		}
		return iterR;
	}

	/**
	 * Returns the number of iteration needed to compute the diameter, if it has
	 * already been computed (otherwise, an exception is raised).
	 *
	 * @return the number of iterations before the diameter is found
	 */
	public int getDiameterIterations() {
		if (iterD == -1) {
			throw new UnsupportedOperationException("The diameter has not been "
					+ "computed, yet. Please, run the compute method with the correct output.");
		}
		return iterD;
	}

	/**
	 * Returns the number of iteration needed to compute all forward
	 * eccentricities, if they have already been computed (otherwise, an
	 * exception is raised).
	 *
	 * @return the number of iterations before all forward eccentricities are
	 *         found
	 */
	public int getAllForwardIterations() {
		if (iterAllF == -1) {
			throw new UnsupportedOperationException("All forward eccentricities have not been "
					+ " computed, yet. Please, run the compute method with the correct output.");
		}
		return iterAllF;
	}

	/**
	 * Returns the number of iteration needed to compute all eccentricities, if
	 * they have already been computed (otherwise, an exception is raised).
	 *
	 * @return the number of iterations before all eccentricities are found
	 */
	public int getAllIterations() {
		if (iterAll == -1) {
			throw new UnsupportedOperationException("All eccentricities have not been "
					+ " computed, yet. Please, run the compute method with the correct output.");
		}
		return iterAll;
	}

	/**
	 * Uses a heuristic to decide which is the best pivot to choose in each
	 * strongly connected component, in order to perform the
	 * {@link #allCCUpperBound(int[])} function.
	 *
	 * @return an array containing in position <var>i</var> the pivot of the
	 *         <var>i</var>th component.
	 */
	private int[] findBestPivot() {
		final int lF[] = this.lF;
		final int lB[] = this.lB;
		final int totDistF[] = this.totDistF;
		final int totDistB[] = this.totDistB;
		final int nn = this.nn;
		final boolean toCompleteF[] = this.toCompleteF;
		final boolean toCompleteB[] = this.toCompleteB;
		final int pivot[] = new int[this.scc.numberOfComponents];
		Arrays.fill(pivot, -1);
		final int sccs[] = scc.component;
		int p;
		long best, current;

		for (int v = nn - 1; v >= 0; v--) {
			p = pivot[sccs[v]];
			if (p == -1) {
				pivot[sccs[v]] = v;
				continue;
			}
			current = (long) lF[v] + lB[v] + (toCompleteF[v] ? 0 : 1) * nn + (toCompleteB[v] ? 0 : 1) * nn;
			best = (long) lF[p] + lB[p] + (toCompleteF[p] ? 0 : 1) * nn + (toCompleteB[p] ? 0 : 1) * nn;

			if (current < best || (current == best && totDistF[v] + totDistB[v] <= totDistF[p] + totDistB[p])) {
				pivot[sccs[v]] = v;
			}
		}
		return pivot;
	}

	/**
	 * Computes and stores in variable <var>accRadial</var> the set of vertices
	 * that are either in the biggest strongly connected component, or that are
	 * able to reach vertices in the biggest strongly connected component.
	 */
	private void computeAccRadial() {
		if (nn == 0) {
			return;
		}
		final boolean accRadial[] = this.accRadial;
		final int sccs[] = scc.component;

		final int sccSizes[] = scc.computeSizes();
		final int maxSizeSCC = argMax(sccSizes);
		int v = 0;

		for (v = nn; v-- > 0;) {
			if (sccs[v] == maxSizeSCC) {
				break;
			}
		}
		final ParallelBreadthFirstVisit bfs = new ParallelBreadthFirstVisit(revgraph, 0, false, null);
		bfs.visit(v);
		for (int i = nn; i-- > 0;) {
			accRadial[i] = bfs.marker.get(i) >= 0;
		}
	}

	/**
	 * Performs a (forward or backward) BFS, updating lower bounds on the
	 * eccentricities of all visited vertices.
	 *
	 * @param start
	 *            the starting vertex of the BFS
	 * @param forward
	 *            if <var>True</var>, the BFS is performed following the
	 *            direction of edges, otherwise it is performed in the opposite
	 *            direction
	 */
	private void stepSumSweep(final int start, final boolean forward) {
		if (start == -1) {
			return;
		}
		final int queue[] = this.queue;
		final int dist[] = this.dist;
		int startQ = 0, endQ = 0;
		int v, w, eccStart;
		int[] l, lOther, u, uOther, totDistOther, ecc, eccOther;
		boolean[] toComplete, toCompleteOther;

		Arrays.fill(dist, -1);

		ImmutableGraph g;

		if (forward) {
			l = lF;
			lOther = lB;
			u = uF;
			uOther = uB;
			totDistOther = totDistB;
			g = graph;
			ecc = eccF;
			eccOther = eccB;
			toComplete = toCompleteF;
			toCompleteOther = toCompleteB;
		} else {
			l = lB;
			lOther = lF;
			u = uB;
			uOther = uF;
			totDistOther = totDistF;
			g = revgraph;
			ecc = eccB;
			eccOther = eccF;
			toComplete = toCompleteB;
			toCompleteOther = toCompleteF;
		}

		LazyIntIterator iter;

		queue[endQ++] = start;
		dist[start] = 0;

		while (startQ < endQ) {
			v = queue[startQ++];
			iter = g.successors(v);

			while ((w = iter.nextInt()) != -1) {
				if (dist[w] == -1) {
					dist[w] = dist[v] + 1;
					queue[endQ++] = w;
				}
			}
		}

		eccStart = dist[queue[endQ - 1]];

		l[start] = eccStart;
		u[start] = eccStart;
		ecc[start] = eccStart;
		toComplete[start] = false;

		if (dL < eccStart) {
			dL = eccStart;
			dV = start;
		}
		if (forward) {
			if (this.accRadial[start] && rU > eccStart) {
				rU = eccStart;
				rV = start;
			}
		}

		for (v = nn - 1; v >= 0; v--) {

			if (dist[v] == -1)
				continue;

			totDistOther[v] += dist[v];

			if (toCompleteOther[v]) {
				if (lOther[v] < dist[v]) {
					lOther[v] = dist[v];
					if (lOther[v] == uOther[v]) {
						toCompleteOther[v] = false;
						eccOther[v] = lOther[v];

						if (!forward && this.accRadial[v] && eccOther[v] < rU) {
							rU = eccOther[v];
							rV = v;
						}
					}
				}
			}
		}
		this.iter++;
		if (pl != null)
			pl.update();
	}

	/**
	 * Performs <var>iter</var> steps of the SumSweep heuristic, starting from
	 * vertex <var>start</var>.
	 *
	 * @param start
	 *            the starting vertex
	 * @param iter
	 *            the number of iterations
	 */
	public void sumSweepHeuristic(final int start, final int iter) {

		if (DEBUG)
			LOGGER.debug("Performing initial SumSweep visit from " + start + ".");
		this.stepSumSweep(start, true);

		for (int i = 2; i < iter; i++) {
			if (i % 2 == 0) {
				final int v = argMax(totDistB, lB, toCompleteB);
				if (DEBUG)
					LOGGER.debug("Performing initial SumSweep visit from " + v + ".");
				this.stepSumSweep(v, false);
			} else {
				final int v = argMax(totDistF, lF, toCompleteF);
				if (DEBUG)
					LOGGER.debug("Performing initial SumSweep visit from " + v + ".");
				this.stepSumSweep(v, true);
			}
		}
	}

	/**
	 * For each edge in the DAG of strongly connected components, finds a
	 * corresponding edge in the graph. This edge is used in the
	 * {@link #allCCUpperBound(int[])} function.
	 */
	private void findEdgesThroughSCC() {
		final int sccs[] = scc.component;
		final int nscc = scc.numberOfComponents;
		final int bestStart[] = new int[nscc];
		final int bestEnd[] = new int[nscc];
		int nSons;
		final ImmutableGraph graph = this.graph;
		final ImmutableGraph revgraph = this.revgraph;
		final int[][] sccGraph = this.sccGraph;
		final int[][] startBridges = this.startBridges;
		final int[][] endBridges = this.endBridges;

		IntArrayList childComponents = new IntArrayList();
		LazyIntIterator iter;
		int w, cw;

		Arrays.fill(bestStart, -1);
		Arrays.fill(bestEnd, -1);

		final IntArrayList vertInSCC[] = new IntArrayList[nscc];
		for (int i = vertInSCC.length; i-- > 0;) {
			vertInSCC[i] = new IntArrayList();
		}

		for (int v = 0; v < nn; v++) {
			vertInSCC[sccs[v]].add(v);
		}

		for (int c = 0; c < nscc; c++) {
			final IntArrayList component = vertInSCC[c];
			childComponents = new IntArrayList();
			for (final int v : component) {
				iter = graph.successors(v);
				while ((w = iter.nextInt()) != -1) {
					cw = sccs[w];
					if (sccs[v] != sccs[w]) {
						if (bestStart[cw] == -1) {
							bestStart[cw] = v;
							bestEnd[cw] = w;
							childComponents.add(cw);
						} else if (graph.outdegree(v) + revgraph.outdegree(w) > graph.outdegree(bestEnd[cw])
								+ revgraph.outdegree(bestStart[cw])) {
							bestStart[cw] = v;
							bestEnd[cw] = w;
						}
					}
				}
			}
			nSons = childComponents.size();
			sccGraph[c] = new int[nSons];
			startBridges[c] = new int[nSons];
			endBridges[c] = new int[nSons];
			for (int i = 0; i < nSons; i++) {
				cw = childComponents.getInt(i);
				sccGraph[c][i] = cw;
				startBridges[c][i] = bestStart[cw];
				endBridges[c][i] = bestEnd[cw];
				bestStart[cw] = -1;
			}
		}
	}

	/**
	 * Performs a (forward or backward) BFS inside each strongly connected
	 * component, starting from the pivot
	 *
	 * @param pivot
	 *            an array containing in position <var>i</var> the pivot of
	 *            the <var>i</var>th strongly connected component
	 * @param forward
	 *            if <var>True</var>, a forward visit is performed, otherwise a
	 *            backward visit
	 * @return two arrays of <var>int</var>, implemented as a bidimensional
	 *         array <var>a</var>[][]. The array <var>a</var>[1] contains the
	 *         distance of each vertex from the pivot of its strongly connected
	 *         component, while <var>a</var>[2] contains in position
	 *         <var>i</var> the eccentricity of the pivot of the <var>i</var>th
	 *         strongly connected component.
	 */
	private int[][] computeDistPivot(final int[] pivot, final boolean forward) {
		final int nn = this.nn;
		final int scc[] = this.scc.component;
		final int eccPivot[] = new int[this.scc.numberOfComponents];
		final int queue[] = this.queue;
		int startQ, endQ, v, w;
		LazyIntIterator iter;

		final int distPivot[] = new int[nn];
		Arrays.fill(distPivot, -1);

		ImmutableGraph g;

		if (forward)
			g = graph;
		else
			g = revgraph;

		for (final int p : pivot) {
			startQ = 0;
			endQ = 0;
			queue[endQ++] = p;
			distPivot[p] = 0;

			while (startQ < endQ) {
				v = queue[startQ++];
				iter = g.successors(v);

				while ((w = iter.nextInt()) != -1) {
					if (scc[w] == scc[p] && distPivot[w] == -1) {
						distPivot[w] = distPivot[v] + 1;
						eccPivot[scc[p]] = distPivot[w];
						queue[endQ++] = w;
					}
				}
			}
		}
		return new int[][] { distPivot, eccPivot };
	}

	/**
	 * Performs a step of the ExactSumSweep algorithm, by performing the
	 * {@link #allCCUpperBound(int[])} function (see the paper for more
	 * details).
	 *
	 * @param pivot
	 *            an array containing in position <var>i</var> the pivot of the
	 *            <var>i</var>th strongly connected component.
	 */
	private void allCCUpperBound(final int[] pivot) {
		final int[][] distEccF = computeDistPivot(pivot, true);
		final int[][] distEccB = computeDistPivot(pivot, false);
		final int distPivotF[] = distEccF[0];
		final int eccPivotF[] = distEccF[1];
		final int distPivotB[] = distEccB[0];
		final int eccPivotB[] = distEccB[1];
		final int[][] sccGraph = this.sccGraph, startBridges = this.startBridges, endBridges = this.endBridges;
		final int[] uF = this.uF;
		final int[] uB = this.uB;
		final int[] lF = this.lF;
		final int[] lB = this.lB;
		final int[] eccF = this.eccF;
		final int[] eccB = this.eccB;
		final int[] sccs = scc.component;
		final boolean[] accRadial = this.accRadial;
		final boolean[] toCompleteB = this.toCompleteB;
		final boolean[] toCompleteF = this.toCompleteF;
		final int nscc = scc.numberOfComponents;

		int p;

		for (int c = 0; c < nscc; c++) {
			p = pivot[c];
			for (int i = 0; i < sccGraph[c].length; i++) {
				final int nextC = sccGraph[c][i];
				final int start = startBridges[c][i];
				final int end = endBridges[c][i];
				eccPivotF[c] = Math.max(eccPivotF[c], distPivotF[start] + 1 + distPivotB[end] + eccPivotF[nextC]);
				if (eccPivotF[c] >= uF[p]) {
					eccPivotF[c] = uF[p];
					break;
				}
			}
		}
		for (int c = nscc; c-- > 0;) {
			for (int i = 0; i < sccGraph[c].length; i++) {
				final int nextC = sccGraph[c][i];
				final int start = startBridges[c][i];
				final int end = endBridges[c][i];
				eccPivotB[nextC] = Math.max(eccPivotB[nextC], distPivotF[start] + 1 + distPivotB[end] + eccPivotB[c]);
				if (eccPivotB[nextC] >= uB[pivot[nextC]]) {
					eccPivotB[nextC] = uB[pivot[nextC]];
				}
			}
		}
		for (int v = 0; v < nn; v++) {
			uF[v] = Math.min(uF[v], distPivotB[v] + eccPivotF[sccs[v]]);
			if (uF[v] == lF[v]) {
				// We do not have to check whether eccF(v)=D, because
				// lF[v]=d(w,v)
				// for some w from which we have already performed a BFS.
				toCompleteF[v] = false;
				eccF[v] = uF[v];
				if (accRadial[v]) {
					if (uF[v] < rU) {
						rU = uF[v];
						rV = v;
					}
				}
			}

			uB[v] = Math.min(uB[v], distPivotF[v] + eccPivotB[sccs[v]]);
			if (uB[v] == lB[v]) {
				toCompleteB[v] = false;
				eccB[v] = uB[v];
				// We do not have to check whether eccB(v)=D, because
				// lB[v]=d(v,w)
				// for some w from which we have already performed a BFS.
			}
		}
		this.iter += 3;
	}

	/**
	 * Computes how many nodes are still to be processed, before outputting the
	 * result
	 *
	 * @return the number of nodes to be processed
	 */
	private int findMissingNodes() {
		int missingR = 0, missingDF = 0, missingDB = 0, missingAllF = 0, missingAllB = 0;
		final boolean toCompleteF[] = this.toCompleteF;
		final boolean toCompleteB[] = this.toCompleteB;
		final boolean accRadial[] = this.accRadial;
		final int[] uF = this.uF;
		final int[] uB = this.uB;
		final int[] lF = this.lF;
		final int dL = this.dL;
		final int rU = this.rU;

		for (int v = nn; v-- > 0;) {
			if (toCompleteF[v]) {
				missingAllF++;
				if (uF[v] > dL) {
					missingDF++;
				}
				if (accRadial[v] && lF[v] < rU) {
					missingR++;
				}
			}
			if (toCompleteB[v]) {
				missingAllB++;
				if (uB[v] > dL) {
					missingDB++;
				}
			}
		}
		if (missingR == 0 && iterR == -1) {
			iterR = iter;
		}
		if ((missingDF == 0 || missingDB == 0) && iterD == -1) {
			iterD = iter;
		}
		if (missingAllF == 0 && iterAllF == -1)
			iterAllF = iter;
		if (missingAllF == 0 && missingAllB == 0)
			iterAll = iter;

		switch (output) {
		case RADIUS:
			return missingR;
		case DIAMETER:
			return Math.min(missingDF, missingDB);
		case RADIUS_DIAMETER:
			return missingR + Math.min(missingDF, missingDB);
		case ALL_FORWARD:
			return missingAllF;
		default:
			return missingAllF + missingAllB;
		}
	}

	/**
	 * Computes diameter, radius, and/or all eccentricities. Results can be
	 * accessed by methods such as {@link #getDiameter()},
	 * {@link #getRadialVertex()} and
	 * {@link #getEccentricity(int, boolean)}.
	 */
	public void compute() {
		if (pl != null) {
			pl.start("Starting visits...");
			pl.itemsName = "nodes";
			pl.displayLocalSpeed = true;
		}
		int maxDeg = Integer.MIN_VALUE, maxDegVert = -1;
		for (int v = 0; v < nn; v++) {
			if (graph.outdegree(v) > maxDeg) {
				maxDeg = graph.outdegree(v);
				maxDegVert = v;
			}
		}

		sumSweepHeuristic(maxDegVert, 6);

		final double points[] = new double[6];
		int missingNodes = findMissingNodes(), oldMissingNodes = missingNodes;

		Arrays.fill(points, graph.numNodes());

		while (missingNodes > 0) {

			final int stepToPerform = argMax(points);

			switch (stepToPerform) {
			case 0:
				if (DEBUG)
					LOGGER.debug("Performing AllCCUpperBound.");
				this.allCCUpperBound(findBestPivot());
				break;
			case 1:
				if (DEBUG)
					LOGGER.debug("Performing a forward BFS, from a vertex maximizing the upper bound.");
				this.stepSumSweep(argMax(uF, totDistF, toCompleteF), true);
				break;
			case 2:
				if (DEBUG)
					LOGGER.debug("Performing a forward BFS, from a vertex minimizing the lower bound.");
				this.stepSumSweep(argMin(lF, totDistF, accRadial), true);
				break;
			case 3:
				if (DEBUG)
					LOGGER.debug("Performing a backward BFS, from a vertex maximizing the upper bound.");
				this.stepSumSweep(argMax(uB, totDistB, toCompleteB), false);
				break;
			case 4:
				if (DEBUG)
					LOGGER.debug("Performing a backward BFS, from a vertex maximizing the distance sum.");
				this.stepSumSweep(argMax(totDistB, uB, toCompleteB), false);
				break;
			case 5:
				if (DEBUG)
					LOGGER.debug("Performing a forward BFS, from a vertex maximizing the distance sum.");
				this.stepSumSweep(argMax(totDistF, uF, toCompleteF), false);
				break;
			}
			oldMissingNodes = missingNodes;
			missingNodes = this.findMissingNodes();
			points[stepToPerform] = oldMissingNodes - missingNodes;

			for (int j = 0; j < points.length; j++) {
				if (j != stepToPerform && points[j] >= 0) {
					points[j] = points[j] + 2.0 / iter;
				}
			}
			if (DEBUG)
				LOGGER.debug("    Missing nodes: " + missingNodes + "/" + 2 * nn + ".");
		}
		if (DEBUG) {
			if (this.output == OutputLevel.RADIUS || this.output == OutputLevel.RADIUS_DIAMETER)
				LOGGER.debug("Radius: " + rU + " (" + iterR + " iterations).");
			if (this.output == OutputLevel.DIAMETER || this.output == OutputLevel.RADIUS_DIAMETER)
				LOGGER.debug("Diameter: " + dL + " (" + iterD + " iterations).");
		}
		if (pl != null)
			pl.done();
	}

	public static void main(final String[] arg) throws IOException, JSAPException {

		final SimpleJSAP jsap = new SimpleJSAP(SumSweepDirectedDiameterRadius.class.getName(),
				"Computes the diameter, radius, diameter and radius, or all eccentricities in a graph, using the ExactSumSweep algorithm.",
				new Parameter[] {
						new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
						new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
						new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
								JSAP.NOT_GREEDY, "The basename of the graph."),
						new UnflaggedOption("forwardOutputFilename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT,
								JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY,
								"The filename where the resulting forward eccentricities (integers in binary form) are stored. If not available, the output file is not produced."),
						new UnflaggedOption("backwardOutputFilename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT,
								JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY,
								"The filename where the resulting backward eccentricities (integers in binary form) are stored. If not available, the output file is not produced."),
						new FlaggedOption("level", EnumStringParser.getParser(OutputLevel.class),
								OutputLevel.ALL.name(), JSAP.REQUIRED, 'l', "level",
								Arrays.toString(OutputLevel.values())) });

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted())
			System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final String graphBasename = jsapResult.getString("graphBasename");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");
		final OutputLevel level = Enum.valueOf(OutputLevel.class,
				jsapResult.getObject("level").toString().toUpperCase());
		final String forwardOutputFilename = jsapResult.getString("forwardOutputFilename");
		final String backwardOutputFilename = jsapResult.getString("backwardOutputFilename");

		progressLogger.displayFreeMemory = true;
		progressLogger.displayLocalSpeed = true;

		ImmutableGraph graph = mapped ? ImmutableGraph.loadMapped(graphBasename, progressLogger)
				: ImmutableGraph.load(graphBasename, progressLogger);
		if (jsapResult.userSpecified("expand"))
			graph = new ArrayListMutableGraph(graph).immutableView();

		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph, level, null, progressLogger);
		ss.compute();
		if (level != OutputLevel.DIAMETER)
			System.out.println("Radius: " + ss.rU + " (" + ss.iterR + " iterations).");
		if (level != OutputLevel.RADIUS)
			System.out.println("Diameter: " + ss.dL + " (" + ss.iterD + " iterations).");

		if (forwardOutputFilename != null && (level == OutputLevel.ALL || level == OutputLevel.ALL_FORWARD)) {
			BinIO.storeInts(ss.eccF, forwardOutputFilename);
		}
		if (backwardOutputFilename != null && level == OutputLevel.ALL) {
			BinIO.storeInts(ss.eccB, backwardOutputFilename);
		}
	}
}
