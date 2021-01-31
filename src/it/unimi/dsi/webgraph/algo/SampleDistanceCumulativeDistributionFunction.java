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
import java.math.RoundingMode;

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

import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.stat.Jackknife;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.ImmutableGraph;

/** Samples a graph via breadth-first visits.
 *
 * <h2>Performance issues</h2>
 *
 * <p>This class uses an instance of {@link ParallelBreadthFirstVisit} to ensure a high degree of parallelism (see its
 * documentation for memory requirements).
 */

public class SampleDistanceCumulativeDistributionFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(SampleDistanceCumulativeDistributionFunction.class);

	/** Checks that we are always visiting the same number of nodes, warning if it is less than the number of nodes of the graph, and throwing an exception otherwise.
	 *
	 * @param visit the current visit.
	 * @param visitedNodes the number of the visited nodes, or 0 if unknown.
	 * @return the number of visited nodes in <code>visit</code>.
	 */
	private static int visitedNodes(final ParallelBreadthFirstVisit visit, int visitedNodes) {
		if (visit.queue.size() != visit.graph.numNodes()) {
			if (visitedNodes == -1) {
				visitedNodes = visit.queue.size();
				LOGGER.warn("The graph is not strongly connected: visiting " + visitedNodes + " < " + visit.graph.numNodes() + " nodes");
			}
			else if (visitedNodes != visit.queue.size()) throw new IllegalStateException("Queue size (" + visit.queue.size() + ") is different from the number of previously visited nodes (" + visitedNodes + "): maybe the graph is not symmetric.");
		}

		return visitedNodes;
	}

	/** Samples a graph via breadth-first visits.
	 *
	 * <p>This method will estimate the cumulative distribution function of distances of
	 * a strongly connected graph. to which a randomly extracted node belongs.
	 * If there is more than one connected component, a warning will be given, specifying the size of the component. An {@link IllegalStateException}
	 * will be thrown if the algorithm detects that the graph is not strongly connected, but this is not guaranteed to happen.
	 *
	 * @param graph a graph.
	 * @param k a number of samples.
	 * @param threads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @return an array of samples.
	 */
	protected static int[][] sample(final ImmutableGraph graph, final int k, final int threads) {
		return sample(graph, k, false, threads);
	}

	/** Samples a graph via breadth-first visits.
	 *
	 * <p>This method will estimate the cumulative distribution function of distances of
	 * a strongly connected graph. If there is more than one connected component, a warning will be given, specifying the size of the component. An {@link IllegalStateException}
	 * will be thrown if the algorithm detects that the graph is not strongly connected, but this is not guaranteed to happen.
	 *
	 * @param graph a graph.
	 * @param k a number of samples.
	 * @param naive sample naively: do not stop sampling even when detecting the lack of strong connection.
	 * @param threads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @return an array of samples.
	 */
	protected static int[][] sample(final ImmutableGraph graph, final int k, final boolean naive, final int threads) {
		final ParallelBreadthFirstVisit visit = new ParallelBreadthFirstVisit(graph, threads, false, new ProgressLogger(LOGGER, "nodes"));

		final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom();

		int componentSize = -1;
		final int[][] result = new int[k][];
		for(int i = k; i-- != 0;) {
			// After the first iteration, we pick a node from the visit queue, unless we are sampling naively.
			visit.clear();
			visit.visit(visit.queue.isEmpty() || naive ? random.nextInt(visit.graph.numNodes()) : visit.queue.getInt(random.nextInt(visit.queue.size())), componentSize);
			if (!naive) componentSize = visitedNodes(visit, componentSize);
			final int maxDistance = visit.maxDistance();
			result[i] = new int[maxDistance + 1];
			for(int d = 0; d <= maxDistance; d++) result[i][d] = visit.cutPoints.getInt(d + 1);
		}

		return result;
	}

	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(SampleDistanceCumulativeDistributionFunction.class.getName(),
				"Estimates the neighbourhood function, the distance cumulative distribution function and the distance probability mass function by sampling." +
				"The output files contains nine columns: for each function, we give the value, the standard error and the relative" +
				"standard error as a percentage (all estimated by the jacknife).",
				new Parameter[] {
			new Switch("mapped", 'm', "mapped", "Do not load the graph in main memory, but rather memory-map it."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new FlaggedOption("samples", JSAP.INTSIZE_PARSER, "1000", JSAP.NOT_REQUIRED, 's', "samples", "The number of samples (breadth-first visits)."),
			new Switch("naive", 'n', "naive", "Sample naively: pick nodes at random and do not stop sampling even when detecting the lack of strong connection."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final ImmutableGraph graph = jsapResult.userSpecified("mapped") ? ImmutableGraph.loadMapped(basename) :ImmutableGraph.load(basename);
		final int[][] sample = sample(graph, jsapResult.getInt("samples"), jsapResult.userSpecified("naive"), jsapResult.getInt("threads"));
		int l = 0;
		for(final int[] s: sample) l = Math.max(l, s.length);
		final int length = l;
		final AbstractObjectList<double[]> samples = new AbstractObjectList<>() {
			@Override
			public double[] get(final int index) {
				final double[] result = new double[length];
				final int[] s = sample[index];
				final double n = graph.numNodes();
				for(int i = 0; i < length; i++) result[i] = s[Math.min(i, s.length - 1)] * n;
				return result;
			}

			@Override
			public int size() {
				return sample.length;
			}
		};
		final Jackknife nf = Jackknife.compute(samples, Jackknife.IDENTITY);
		final Jackknife cdf = Jackknife.compute(samples, ApproximateNeighbourhoodFunctions.CDF);
		final Jackknife pmf = Jackknife.compute(samples, ApproximateNeighbourhoodFunctions.PMF);

		for(int i = 0; i < pmf.estimate.length; i++) System.out.println(
				nf.bigEstimate[i].setScale(30, RoundingMode.HALF_EVEN) + "\t" + nf.standardError[i] + "\t" + 100 * nf.standardError[i] / nf.estimate[i] + "\t" +
				cdf.bigEstimate[i].setScale(30, RoundingMode.HALF_EVEN) + "\t" + cdf.standardError[i] + "\t" + 100 * cdf.standardError[i] / cdf.estimate[i] + "\t" +
				pmf.bigEstimate[i].setScale(30, RoundingMode.HALF_EVEN) + "\t" + pmf.standardError[i] + "\t" + 100 * pmf.standardError[i] / pmf.estimate[i]);

	}
}
