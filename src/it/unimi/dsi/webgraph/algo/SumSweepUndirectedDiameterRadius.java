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

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.EnumStringParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.Check;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.Transform;

/**
 *
 * Computes the radius and/or the diameter and/or all eccentricities of an
 * undirected graph, using the SumSweep algorithm described by Michele Borassi,
 * Pierluigi Crescenzi, Michel Habib, Walter A. Kosters, Andrea Marino, and
 * Frank W. Takes in &ldquo;Fast diameter and radius BFS-based computation in
 * (weakly connected) real-world graphs&mdash;With an application to the six
 * degrees of separation games &rdquo;, <i>Theoretical Computer Science</i>,
 * 586:59&minus;80, 2015.
 *
 * <p>
 * We define the <em>eccentricity</em> of a node <var>v</var> in a graph
 * <var>G</var>=(<var>V</var>,<var>E</var>) as <var>ecc</var>(<var>v</var>)=max
 * <sub><var>w</var> reachable from <var>v</var></sub> <var>d</var>(<var>v</var>
 * ,<var>w</var>), where <var>d</var>(<var>v</var>,<var>w</var>) is the number
 * of edges in a shortest path from <var>v</var> to <var>w</var>. The
 * <em>diameter</em> is max<sub><var>v</var>&isin;<var>V</var></sub>
 * <var>ecc</var>(<var>v</var>), and the <em>radius</em> is min <sub>v&isin;
 * <var>V</var></sub> <var>ecc</var>(<var>v</var>).
 *
 * <p>
 * This algorithm performs some BFSs from "clever" vertices, and uses these BFSs
 * to bound the eccentricity of all vertices. More specifically, for each vertex
 * <var>v</var>, this algorithm keeps a lower and an upper bound on the
 * eccentricity of <var>v</var>, named <var>l</var>[<var>v</var>], <var>u</var>[
 * <var>v</var>]. Furthermore, it keeps a lower bound <var>dL</var> on the
 * diameter and an upper bound <var>rU</var> on the radius. At each step, the
 * algorithm performs a BFS, and it updates all these bounds: the radius is
 * found as soon as <var>rU</var> is smaller than the minimum value of
 * <var>l</var>, and the diameter is found as soon as <var>dL</var> is bigger
 * than the maximum value of <var>u</var>.
 *
 * <p>
 * More specifically, the upper bound on the radius (resp., lower bound on the
 * diameter) is defined as the minimum (resp., maximum) eccentricity of a vertex
 * from which we performed a BFS. Moreover, if we perform a BFS from a vertex
 * <var>s</var>, we update <var>l</var>[<var>v</var>]=max(<var>l</var>[
 * <var>v</var>], d(<var>s</var>, <var>v</var>)), and <var>u</var>[<var>v</var>
 *]=max(<var>u</var>[<var>v</var>], d(<var>v</var>, <var>s</var>) +
 * <var>ecc</var>(<var>s</var>).
 *
 * <p>
 * To use this class, it is enough to create an instance, and then invoke
 * {@link #compute()}. It is possible to choose between the following stopping
 * conditions:
 * <ul>
 * <li>only the radius is found;</li>
 * <li>only the diameter is found;</li>
 * <li>radius and diameter are found;</li>
 * <li>all eccentricities are found.</li>
 * </ul>
 *
 * <p>
 * After the method {@link #compute()} is run, the output can be obtained
 * through the methods {@link #getRadius()} for the radius, {@link #getRadialVertex()} for a
 * radial vertex, {@link #getDiameter()} for the diameter, {@link #getDiametralVertex()} for a
 * vertex whose (forward or backward) eccentricity equals the diameter,
 * {@link #getEccentricity(int)} for the eccentricity of a vertex. An exception is raised
 * if the field has not been computed.
 *
 * <h2>Performance issues</h2>
 *
 * <p>
 * The algorithm is exact and, although the running-time is <var>O</var>(
 * <var>mn</var>) in the worst-case, it is usually much faster on real-world
 * networks.
 *
 *
 * @author Michele Borassi
 */

public class SumSweepUndirectedDiameterRadius {
	private static final boolean DEBUG = true;
	private final static Logger LOGGER = LoggerFactory.getLogger(SumSweepUndirectedDiameterRadius.class);

	/**
	 * The type of output requested: radius, diameter, radius and diameter, or
	 * all eccentricities.
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
		RADIUSDIAMETER,
		/**
		 * Computes the radius, the diameter, and all the eccentricities.
		 */
		ALL
	}

	/** The graph under examination. */
	private final ImmutableGraph graph;
	/** The number of nodes. */
	private final int nn;
	/** The global progress logger. */
	private final ProgressLogger pl;
	/** The kind of output requested. */
	private final OutputLevel output;
	/** The array of eccentricity values. */
	protected final int[] ecc;
	/**
	 * <var>toComplete</var>[<var>v</var>] is true if and only if the
	 * eccentricity of <var>v</var> has not been exactly computed, yet.
	 */
	private final boolean[] toComplete;
	/** Saves which vertices are in the first branch of a BFS. */
	private final boolean[] firstBranch;
	/** The queue used for each BFS (it is recycled to save some time). */
	private final int[] queue;
	/**
	 * The array of distances, used in each BFS (it is recycled to save some
	 * time).
	 */
	private final int[] dist;
	/** Lower bound on the diameter of the graph. */
	private int dL;
	/** Lower bound on the radius of the graph. */
	private int rU;
	/** A vertex whose eccentricity equals the diameter. */
	private int dV;
	/** A vertex whose eccentricity equals the radius. */
	private int rV;
	/** Number of iterations performed until now. */
	private int iter;
	/** Lower bound on the eccentricity. */
	protected int l[];
	/** Upper bound on the eccentricity. */
	protected int u[];
	/** Number of iterations before the radius is found. */
	private int iterR;
	/** Number of iterations before the diameter is found. */
	private int iterD;
	/** Number of iterations before all eccentricities are found. */
	private int iterAll;
	/**
	 * Total forward distance from already processed vertices (used as tie-break
	 * for the choice of the next vertex to process).
	 */
	private final int totDist[];

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
	 */
	public SumSweepUndirectedDiameterRadius(final ImmutableGraph graph, final OutputLevel output,
			final ProgressLogger pl) {
		if (!Check.symmetry(graph)) {
			throw new IllegalArgumentException("The graph is not undirected.");
		}
		this.pl = pl;
		this.graph = graph;
		this.nn = graph.numNodes();
		this.ecc = new int[nn];
		totDist = new int[nn];
		l = new int[nn];
		u = new int[nn];
		toComplete = new boolean[nn];
		queue = new int[nn];
		dist = new int[nn];
		firstBranch = new boolean[nn];

		Arrays.fill(ecc, -1);
		Arrays.fill(u, nn + 1);
		Arrays.fill(l, 0);
		Arrays.fill(toComplete, true);
		this.dL = 0;
		this.rU = Integer.MAX_VALUE;
		this.output = output;
		iterR = -1;
		iterD = -1;
		iterAll = -1;
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
	 */
	public int getEccentricity(final int v) {
		if (ecc[v] == -1) {
			throw new UnsupportedOperationException("The eccentricity of v has not been"
					+ "computed, yet. Please, use the compute method with" + "the correct output.");
		}
		return ecc[v];
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
	 * Performs a (forward or backward) BFS, updating upper and lower bounds on
	 * the eccentricities of all visited vertices.
	 *
	 * @param start
	 *            the starting vertex of the BFS
	 */
	private void stepSumSweep(final int start) {
		if (start == -1) {
			return;
		}
		if (graph.outdegree(start) == 0) {
			rU = 0;
			rV = start;
			ecc[start] = 0;
			toComplete[start] = false;
			return;
		}
		final ImmutableGraph g = graph;
		final int queue[] = this.queue;
		final int dist[] = this.dist;
		int startQ = 0, endQ = 0;
		int v = start, w, eccStart, eccNotFirstBranch = 0;
		final int[] l = this.l, u = this.u, totDist = this.totDist, ecc = this.ecc;
		final boolean[] firstBranch = this.firstBranch, toComplete = this.toComplete;
		int startingPathL = 0;

		Arrays.fill(dist, -1);
		dist[start] = 0;

		// If the BFS tree starts with a path, we consider the path separately.
		if (g.outdegree(start) == 1) {
			int old = start;
			w = start;
			v = g.successors(start).nextInt();
			dist[v] = 1;
			startingPathL++;
			while (g.outdegree(v) == 2) {
				final int successors[] = g.successorArray(v);
				old = w;
				w = v;
				if (successors[0] == old)
					v = successors[1];
				else
					v = successors[0];
				dist[v] = dist[w] + 1;
				startingPathL++;
			}
		}

		Arrays.fill(firstBranch, false);
		LazyIntIterator iter;

		queue[endQ++] = v;
		final int successors[] = g.successorArray(v);

		// We want to compute which is the first branch of the BFS tree.
		// Obviously, we want to exclude the initial path.
		if (g.outdegree(v) != 1) {
			if (dist[successors[0]] == -1) {
				firstBranch[successors[0]] = true;
			} else {
				firstBranch[successors[1]] = true;
			}
		}

		// We run the BFS.
		while (startQ < endQ) {
			v = queue[startQ++];

			iter = g.successors(v);

			if (!firstBranch[v]) {
				eccNotFirstBranch = dist[v];
			}

			while ((w = iter.nextInt()) != -1) {
				if (dist[w] == -1) {
					dist[w] = dist[v] + 1;
					queue[endQ++] = w;
					firstBranch[w] = firstBranch[w] || firstBranch[v];
				}
			}
		}

		eccStart = dist[queue[endQ - 1]];

		// We update all bounds.
		for (v = nn; v-- > 0;) {
			if (dist[v] == -1) {
				continue;
			}
			totDist[v] += dist[v];
			if (toComplete[v]) {
				final int distv = dist[v];

				l[v] = Math.max(l[v], Math.max(eccStart - distv, distv));

				if (firstBranch[v]) {
					u[v] = Math.min(u[v], Math.max(eccStart - 2 - 2 * (startingPathL) + distv,
							distv + Math.max(0, eccNotFirstBranch - 2 * startingPathL)));
				} else if (distv < startingPathL) {
					u[v] = Math.min(u[v], Math.max(distv, eccStart - distv));
				} else {
					u[v] = Math.min(u[v], Math.max(eccStart - 2 * startingPathL + distv, eccStart));
				}

				if (l[v] == u[v]) {
					toComplete[v] = false;
					ecc[v] = l[v];
					if (dL < ecc[v]) {
						dL = ecc[v];
						dV = v;
					}
					if (rU > ecc[v]) {
						rU = ecc[v];
						rV = v;
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
	 * vertex <var>start</var>. The SumSweep heuristic performs BFSes from
	 * vertices maximizing the sum of the distance from the starting vertices of
	 * previous BFSes, and should be considered "peripheral". This way, after
	 * few iterations, usually most lower bounds on the eccentricities are
	 * tight.
	 *
	 * @param start
	 *            the starting vertex
	 * @param iter
	 *            the number of iterations
	 */
	public void sumSweepHeuristic(final int start, final int iter) {

		this.stepSumSweep(start);

		for (int i = 2; i < iter; i++) {
			this.stepSumSweep(SumSweepDirectedDiameterRadius.argMax(totDist, l, toComplete));
		}
	}

	/**
	 * Computes how many nodes are still to be processed, before outputting the
	 * result
	 *
	 * @return the number of nodes to be processed
	 */
	private int findMissingNodes() {
		int missingR = 0, missingD = 0, missingAll = 0;
		final boolean toComplete[] = this.toComplete;
		final int u[] = this.u;
		final int l[] = this.l;

		for (int v = nn; v-- > 0;) {
			if (toComplete[v]) {
				missingAll++;
				if (u[v] > dL) {
					missingD++;
				}
				if (l[v] < rU) {
					missingR++;
				}
			}
		}
		if (missingR == 0 && iterR == -1) {
			iterR = iter;
		}
		if ((missingD == 0) && iterD == -1) {
			iterD = iter;
		}
		if (missingAll == 0)
			iterAll = iter;

		switch (output) {
		case RADIUS:
			return missingR;
		case DIAMETER:
			return missingD;
		case RADIUSDIAMETER:
			return missingR + missingD;
		default:
			return missingAll;
		}
	}

	/**
	 * Computes diameter, radius, and/or all eccentricities. Results can be
	 * accessed by methods such as {@link #getDiameter()},
	 * {@link #getRadialVertex()} and
	 * {@link #getEccentricity(int)}.
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
		sumSweepHeuristic(maxDegVert, 3);

		final double points[] = new double[3];
		int missingNodes = findMissingNodes(), oldMissingNodes = missingNodes;

		Arrays.fill(points, graph.numNodes());

		while (missingNodes > 0) {

			final int stepToPerform = SumSweepDirectedDiameterRadius.argMax(points);

			switch (stepToPerform) {
			case 0:
				if (DEBUG)
					LOGGER.debug("Performing a BFS from a vertex maximizing the upper bound.");
				this.stepSumSweep(SumSweepDirectedDiameterRadius.argMax(u, totDist, toComplete));
				break;
			case 1:
				if (DEBUG)
					LOGGER.debug("Performing a BFS from a vertex minimizing the lower bound.");
				this.stepSumSweep(SumSweepDirectedDiameterRadius.argMin(l, totDist, toComplete));
				break;
			case 2:
				if (DEBUG)
					LOGGER.debug("Performing a BFS from a vertex maximizing the distance sum.");
				this.stepSumSweep(SumSweepDirectedDiameterRadius.argMax(totDist, u, toComplete));
				break;
			}
			oldMissingNodes = missingNodes;
			missingNodes = this.findMissingNodes();
			points[stepToPerform] = oldMissingNodes - missingNodes;

			for (int j = 0; j < points.length; j++) {
				if (j != stepToPerform) {
					points[j] = points[j] + 2.0 / iter;
				}
			}
			if (DEBUG)
				LOGGER.debug("    Missing nodes: " + missingNodes + "/" + 2 * nn + ".");
		}
		if (DEBUG) {
			if (this.output == OutputLevel.RADIUS || this.output == OutputLevel.RADIUSDIAMETER)
				LOGGER.debug("Radius: " + rU + " (" + iterR + " iterations).");
			if (this.output == OutputLevel.DIAMETER || this.output == OutputLevel.RADIUSDIAMETER)
				LOGGER.debug("Diameter: " + dL + " (" + iterD + " iterations).");
		}
		if (pl != null)
			pl.done();
	}

	public static void main(final String[] arg) throws IOException, JSAPException {

		final SimpleJSAP jsap = new SimpleJSAP(SumSweepUndirectedDiameterRadius.class.getName(),
				"Computes the diameter, radius, diameter and radius, or all eccentricities in a graph, using the ExactSumSweep algorithm.",
				new Parameter[] {
						new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
						new Switch("onlyGiant", 'g', "onlyGiant",
								"Performs the computation only for the biggest component of the input graph."),
						new Switch("symmetrize", 's', "symmetrize",
								"Symmetrizes the graph (so that also directed graphs can be input)."),
						new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
						new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
								JSAP.NOT_GREEDY, "The basename of the graph."),
						new UnflaggedOption("outputFilename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED,
								JSAP.NOT_GREEDY,
								"The filename where the resulting backward eccentricities (integers in binary form) are stored. If not available, the output file is not produced."),
						new FlaggedOption("level", EnumStringParser.getParser(OutputLevel.class, true),
								OutputLevel.ALL.name(), JSAP.NOT_REQUIRED, 'l', "level",
								Arrays.toString(OutputLevel.values())), });

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted())
			System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final boolean onlyGiant = jsapResult.getBoolean("onlyGiant", false);
		final boolean symmetrize = jsapResult.getBoolean("symmetrize", false);
		final String graphBasename = jsapResult.getString("graphBasename");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");
		final OutputLevel level = Enum.valueOf(OutputLevel.class,
				jsapResult.getObject("level").toString().toUpperCase());
		final String forwardOutputFilename = jsapResult.getString("outputFilename");

		progressLogger.displayFreeMemory = true;
		progressLogger.displayLocalSpeed = true;

		ImmutableGraph graph = mapped ? ImmutableGraph.loadMapped(graphBasename, progressLogger)
				: ImmutableGraph.load(graphBasename, progressLogger);
		if (jsapResult.userSpecified("expand"))
			graph = new ArrayListMutableGraph(graph).immutableView();
		if (symmetrize)
			graph = Transform.symmetrize(graph);
		if (onlyGiant)
			graph = ConnectedComponents.getLargestComponent(graph, 0, null);

		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(graph, level, progressLogger);
		ss.compute();
		if (level != OutputLevel.DIAMETER)
			System.out.println("Radius: " + ss.rU + " (" + ss.iterR + " iterations).");
		if (level != OutputLevel.RADIUS)
			System.out.println("Diameter: " + ss.dL + " (" + ss.iterD + " iterations).");
		System.out.println("Total number of iterations: " + ss.iter + ".");

		if (forwardOutputFilename != null && (level == OutputLevel.ALL)) {
			BinIO.storeInts(ss.ecc, forwardOutputFilename);
		}
	}
}
