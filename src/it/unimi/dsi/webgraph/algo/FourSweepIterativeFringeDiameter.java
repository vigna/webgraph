/*
 * Copyright (C) 2011-2021 Sebastiano Vigna
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
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;

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

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph.LoadMethod;

/** Computes the diameter of a <em>symmetric</em> (a.k.a&#46; <em>undirected</em>) graph.
 *
 * <p>This class implements a variant of the heuristic algorithm proposed by Pierluigi Crescenzi, Roberto Grossi, Michel Habib,
 * Leonardo Lanzi and Andrea Marino in &ldquo;On computing the diameter of real-world undirected graphs&rdquo;, presented
 * at the <i>Workshop on Graph Algorithms and Applications</i> (Zurich, July 3,2011-2014), which extends
 * the double-sweep heuristic for bounding the diameter suggested by Cl&eacute;mence Magnien,
 * Matthieu Latapy and Michel Habib in &ldquo;Fast computation of empirically tight bounds for the diameter of massive graphs&rdquo;,
 * <i>J. Exp. Algorithmics</i>, 13:1.10:1&minus;1.10:9, ACM, 2009.
 *
 * <p>To understand why the following algorithm works, recall that the <em>eccentricity</em> of a node <var>x</var> is the
 * maximum distance <i>d</i>(<var>x</var>, <var>y</var>). The minimum eccentricity over all nodes is called the <em>radius</em> of the graph, and
 * a node with minimum eccentricity is called a <em>center</em>. The diameter is just the maximum eccentricity, so
 * the diameter is bounded by twice the radius (but it might not be equal: a line with an even number of nodes is a counterexample).
 * The following two observations are obvious:
 * <ul>
 * <li>the eccentricity of a node is a lower bound for the diameter;
 * <li>given a node <var>x</var> and an integer <var>h</var>, 2<var>h</var> maximised with the
 * eccentricities of all nodes at distance greater than <var>h</var> from <var>x</var> is an
 * upper bound for the diameter.
 * </ul>
 *
 * <p>The <em>double-sweep</em> algorithm is the standard algorithm to compute the diameter of a tree:
 * we take a random node and locate using a breadth-first visit a
 * farthest node <var>x</var>. Then, we perform a second breadth-first visit, computing the
 * eccentricity of <var>x</var>, which turns out to be the diameter of the tree.
 * When applied to a general graph, the double-sweep algorithm provides a good lower bound (in general, whenever we perform
 * a breadth-first visit we use the resulting eccentricity to improve the current lower bound).
 * With some (usually few) additional visits, the <em>iterative
 * fringe</em> algorithm often makes it possible to make the bounds match.
 *
 * <p>More precisely, after the second visit we find a node <var>c</var> that is
 * halfway between <var>x</var> and a node farthest from <var>x</var>. The
 * node <var>c</var> is a tentative center of the graph,
 * and it certainly is if the graph is a tree.
 *
 * <p>We then perform a breadth-first visit from <var>c</var> and compute its eccentricity <var>h</var>, obtaining an upper bound
 * 2<var>h</var> for the diameter.
 *
 * <p>In case our upper bound does not match the lower bound, we compute the eccentricities of the <em>fringe</em>, that is, the set
 * of nodes at distance <var>h</var> from <var>c</var>, by performing a breadth-first visit from each node in the fringe. At each
 * eccentricity computed, we update our lower bound, and stop if it matches our current upper bound. Finally, when the fringe is exhausted,
 * assuming <var>M</var> is the maximum of the eccentricities computed, max(2<var>(h</var>&nbsp;&minus;&nbsp;1),&nbsp;<var>M</var>)
 * is an improved upper bound for the diameter. We iterate the procedure with the new fringe
 * (nodes at distance <var>h</var>&nbsp;&minus;&nbsp;1), and so on, until the lower and upper bounds do match.
 *
 * <p>The description above is a bit simplified: after finding <var>c</var>, we actually
 * do a double sweep again starting from <var>c</var> and update <var>c</var> accordingly. This
 * four-sweep procedure often improves the quality (e.g., reduces the eccentricity) of <var>c</var>.
 *
 * <h2>Performance issues</h2>
 *
 * <p>This class uses an instance of {@link ParallelBreadthFirstVisit} to ensure a high degree of parallelism (see its
 * documentation for memory requirements).
 *
 * @deprecated Superseded by {@link SumSweepDirectedDiameterRadius}/{@link SumSweepUndirectedDiameterRadius}.
 */

@Deprecated
public class FourSweepIterativeFringeDiameter {
	private static final Logger LOGGER = LoggerFactory.getLogger(FourSweepIterativeFringeDiameter.class);

	/** Checks that we are always visiting the same component of the same size and possibly logs a warning or throws an exception.
	 *
	 * @param visit the current visit.
	 * @param componentSize the size of the visited component, or 0 if unknown.
	 * @return the size of the visited component.
	 */

	private static int componentSize(final ParallelBreadthFirstVisit visit, int componentSize) {
		if (visit.queue.size() != visit.graph.numNodes()) {
			if (componentSize == -1) {
				componentSize = visit.queue.size();
				LOGGER.warn("The graph is not connected: computing the diameter of a component of " + componentSize + " < " + visit.graph.numNodes() + " nodes");
			}
			else if (componentSize != visit.queue.size()) throw new IllegalStateException("Queue size (" + visit.queue.size() + ") is different from component size (" + componentSize + "): maybe the graph is not symmetric.");
		}

		return componentSize;
	}

	/** Computes the diameter of a symmetric graph.
	 *
	 * @param symGraph a symmetric graph.
	 * @param threads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param pl a progress logger, or <code>null</code>.
	 * @param seed a seed for generating random starting points.
	 * @return the diameter.
	 */
	public static int run(final ImmutableGraph symGraph, final int threads, final ProgressLogger pl, final long seed) {
		final ParallelBreadthFirstVisit visit = new ParallelBreadthFirstVisit(symGraph, threads, true, pl);
		final AtomicIntegerArray parent = visit.marker;
		final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(seed);
		final int n = symGraph.numNodes();
		int lowerBound = 0, upperBound = n - 1, componentSize = -1;

		while(lowerBound < upperBound) {
			if (pl != null) pl.logger().info("New round of bound refinement... [" + lowerBound + ".." + upperBound + "]");

			// After the first iteration, we pick a node from the visit queue
			visit.clear();
			visit.visit(visit.queue.isEmpty() ? random.nextInt(n) : visit.queue.getInt(random.nextInt(visit.queue.size())), componentSize);
			int border = visit.nodeAtMaxDistance();
			componentSize = componentSize(visit, componentSize);
			lowerBound = Math.max(visit.maxDistance(), lowerBound);
			upperBound = Math.min(upperBound, 2 * visit.maxDistance());

			if (pl != null) pl.logger().info("After visit from random node: [" + lowerBound + ".." + upperBound + "]");
			if (lowerBound == upperBound) break;

			visit.clear();
			visit.visit(border, componentSize);
			border = visit.nodeAtMaxDistance();
			componentSize = componentSize(visit, componentSize);
			lowerBound = Math.max(visit.maxDistance(), lowerBound);
			upperBound = Math.min(upperBound, 2 * visit.maxDistance());

			if (pl != null) pl.logger().info("After first double sweep: [" + lowerBound + ".." + upperBound + "]");
			if (lowerBound == upperBound) break;

			// Find first tentative center of the graph (certainly the center if it is a tree).
			int center = border;
			for(int i = visit.maxDistance() / 2; i-- != 0;) center = parent.get(center);

			// We now visit from the tentative center.
			visit.clear();
			visit.visit(center, componentSize);
			border = visit.nodeAtMaxDistance();
			componentSize = componentSize(visit, componentSize);
			lowerBound = Math.max(visit.maxDistance(), lowerBound);
			upperBound = Math.min(upperBound, 2 * visit.maxDistance());

			if (pl != null) pl.logger().info("After visit from first tentative center (node " + center + "): [" + lowerBound + ".." + upperBound + "]");
			if (lowerBound == upperBound) break;

			// Last sweep
			visit.clear();
			visit.visit(border);
			border = visit.nodeAtMaxDistance();
			componentSize = componentSize(visit, componentSize);
			lowerBound = Math.max(visit.maxDistance(), lowerBound);
			upperBound = Math.min(upperBound, 2 * visit.maxDistance());

			if (pl != null) pl.logger().info("After second double sweep: [" + lowerBound + ".." + upperBound + "]");
			if (lowerBound == upperBound) break;

			// Find new (and hopefully improved) center.
			center = border;
			for(int i = visit.maxDistance() / 2; i-- != 0;) center = parent.get(center);

			// We now visit from the new center.
			visit.clear();
			visit.visit(center, componentSize);
			componentSize = componentSize(visit, componentSize);
			lowerBound = Math.max(visit.maxDistance(), lowerBound);
			upperBound = Math.min(upperBound, 2 * visit.maxDistance());

			if (pl != null) pl.logger().info("After visit from new center (node " + center + "): [" + lowerBound + ".." + upperBound + "]");
			if (lowerBound == upperBound) break;

			// Copy cutpoints and queue as they are needed to visit incrementally the fringe (this stuff could go on disk, actually).
			final IntArrayList cutPoints = visit.cutPoints.clone();
			final IntArrayList queue = visit.queue.clone();

			final ProgressLogger globalProgressLogger = pl == null ? null : new ProgressLogger(pl.logger(), pl.logInterval, TimeUnit.MILLISECONDS, "visits");
			if (pl != null) {
				pl.logger().debug("Cutpoints: " + cutPoints);
				globalProgressLogger.start("Starting visits...");
			}

			/* We now incrementally remove nodes at decreasing distance d from the center,
			 * keeping track of the maximum eccentricity maxEcc of the removed nodes.
			 * max(maxEcc, 2(d - 1)) is obviously an upper bound for the diameter. */
			int maxEcc = 0;
			for(int d = visit.maxDistance(); d > 0 && lowerBound < upperBound; d--) {
				if (pl != null) {
					globalProgressLogger.expectedUpdates = pl.count + cutPoints.getInt(d + 1) - cutPoints.getInt(lowerBound / 2 + 1);
					pl.logger().info("Examining " + (cutPoints.getInt(d + 1) - cutPoints.getInt(d)) + " nodes at distance " + d + " (at most " + globalProgressLogger.expectedUpdates + " visits to go)...");
				}
				for(int pos = cutPoints.getInt(d); pos < cutPoints.getInt(d + 1); pos++) {
					final int x = queue.getInt(pos);
					visit.clear();
					visit.visit(x);
					componentSize = componentSize(visit, componentSize);
					maxEcc = Math.max(maxEcc, visit.maxDistance());
					lowerBound = Math.max(lowerBound, maxEcc);
					if (lowerBound == upperBound) return lowerBound;
				}

				upperBound = Math.max(maxEcc, 2 * (d - 1));
				if (pl != null) {
					globalProgressLogger.updateAndDisplay(cutPoints.getInt(d + 1) - cutPoints.getInt(d));
					pl.logger().info("After enlarging fringe: [" + lowerBound + ".." + upperBound + "]");
				}
			}

			if (globalProgressLogger != null) globalProgressLogger.done();
		}
		return lowerBound;
	}

	static public void main(final String arg[]) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, IOException, ClassNotFoundException, InstantiationException {
		final SimpleJSAP jsap = new SimpleJSAP(FourSweepIterativeFringeDiameter.class.getName(), "Computes the diamater of a symmetric graph using Magnien-Latay-Habib's technique.",
				new Parameter[] {
						new FlaggedOption("graphClass", GraphClassParser.getParser(), null, JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java class for the source graph."),
						new Switch("spec", 's', "spec", "The basename is rather a specification of the form <ImmutableGraphImplementation>(arg,arg,...)."),
						new Switch("mapped", 'm', "mapped", "Do not load the graph in main memory, but rather memory-map it."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		final String basename = jsapResult.getString("basename");
		final Class<?> graphClass = jsapResult.getClass("graphClass");
		final boolean spec = jsapResult.getBoolean("spec");
		final boolean mapped = jsapResult.getBoolean("mapped");
		final int threads = jsapResult.getInt("threads");
		final ImmutableGraph graph;

		if (graphClass != null) {
			if (spec) {
				System.err.println("Options --graph-class and --spec are incompatible");
				System.exit(1);
				return; // Just to avoid spurious errors about graph not being initialised.
			}
			else graph = (ImmutableGraph)graphClass.getMethod(mapped ? LoadMethod.MAPPED.toMethod() : LoadMethod.STANDARD.toMethod(), CharSequence.class).invoke(null, basename);
		}
		else {
			if (!spec) graph = mapped ? ImmutableGraph.loadMapped(basename, pl) : ImmutableGraph.load(basename, pl);
			else graph = ObjectParser.fromSpec(basename, ImmutableGraph.class, GraphClassParser.PACKAGE);
		}

		System.out.println(run(graph, threads, new ProgressLogger(LOGGER), Util.randomSeed()));
	}
}
