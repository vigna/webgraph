package it.unimi.dsi.webgraph.scratch;

/*
 * Copyright (C) 2019 Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import java.io.IOException;
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

import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.algo.ParallelBreadthFirstVisit;

/** Computes the neighbourhood function of a node of graph by a {@linkplain ParallelBreadthFirstVisit parallel breadth-first visit}.
 *
 * <h2>Performance issues</h2>
 *
 * <p>This class uses an instance of {@link ParallelBreadthFirstVisit} to ensure a high degree of parallelism (see its
 * documentation for memory requirements). Note that if the graph is small a large number of thread will slow down the computation because of synchronization costs.
 *
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 */
public class NodeNeighbourhoodFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(NodeNeighbourhoodFunction.class);

	/**
	 * Computes and returns the neighbourhood function of the specified graph by multiple breadth-first
	 * visits.
	 *
	 * <p>
	 * This method returns an array of doubles. When some values of the function are near
	 * 2<sup>63</sup>, it might lose some least-significant digits. If you need exact values, use
	 * {@link #computeExact(ImmutableGraph, int, int, ProgressLogger)} instead.
	 *
	 * @param g a graph.
	 * @param node the starting node.
	 * @return the neighbourhood function of the specified graph.
	 */
	public static double[] compute(final ImmutableGraph g, final int node) {
		return compute(g, node, null);
	}

	/**
	 * Computes and returns the neighbourhood function of the specified graph by multiple breadth-first
	 * visits.
	 *
	 * <p>
	 * This method returns an array of doubles. When some values of the function are near
	 * 2<sup>63</sup>, it might lose some least-significant digits. If you need exact values, use
	 * {@link #computeExact(ImmutableGraph, int, int, ProgressLogger)} instead.
	 *
	 * @param g a graph.
	 * @param node the starting node.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the neighbourhood function of the specified graph.
	 */
	public static double[] compute(final ImmutableGraph g, final int node, final ProgressLogger pl) {
		return compute(g, node, 0, pl);
	}

	/**
	 * Computes and returns the neighbourhood function of the specified graph by multiple breadth-first
	 * visits.
	 *
	 * <p>
	 * This method returns an array of doubles. When some values of the function are near
	 * 2<sup>63</sup>, it might lose some least-significant digits. If you need exact values, use
	 * {@link #computeExact(ImmutableGraph, int, int, ProgressLogger)} instead.
	 *
	 * @param g a graph.
	 * @param node the starting node.
	 * @param threads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 *            Note that if the graph is small a large number of thread will slow down the
	 *            computation because of synchronization costs.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the neighbourhood function of the specified graph.
	 */
	public static double[] compute(final ImmutableGraph g, final int node, final int threads, final ProgressLogger pl) {
		final long[] computeExact = computeExact(g, node, threads, pl);
		final double[] result = new double[computeExact.length];
		for(int i = result.length; i-- != 0;) result[i] = computeExact[i];
		return result;
	}

	/** Computes and returns the neighbourhood function of the specified graph by multiple breadth-first visits.
	 *
	 * <p>This method returns an array of longs. When some values of the function are near 2<sup>63</sup>, it
	 * provides an exact value, as opposed to {@link #compute(ImmutableGraph, int, ProgressLogger)}.
	 *
	 * @param g a graph.
	 * @param threads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * Note that if the graph is small a large number of thread will slow down the computation because of synchronization costs.
	 * @param node the starting node.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the neighbourhood function of the specified graph as an array of longs.
	 */
	public static long[] computeExact(final ImmutableGraph g, final int threads, final int node, final ProgressLogger pl) {
		final ParallelBreadthFirstVisit visit = new ParallelBreadthFirstVisit(g, threads, true, pl);

		visit.clear();
		visit.visit(node);
		final int maxDistance = visit.maxDistance();
		final long[] result = new long[maxDistance + 1];
		result[0] = visit.cutPoints.getInt(1);
		for(int d = 1; d <= maxDistance; d++) result[d] = visit.cutPoints.getInt(d + 1) - visit.cutPoints.getInt(d) + result[d - 1];
		return result;
	}

	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(NodeNeighbourhoodFunction.class.getName(),
				"Prints the neighbourhood function of a node a graph, computing it via a parallel breadth-first visit.",
				new Parameter[] {
			new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically. Note that if the graph is small a large number of thread will slow down the computation because of synchronization costs."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("node", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The node."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final int node = jsapResult.getInt("node");
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		ImmutableGraph g =ImmutableGraph.load(basename);
		if (jsapResult.userSpecified("expand")) g = new ArrayListMutableGraph(g).immutableView();
		TextIO.storeLongs(computeExact(g, node, threads, pl), System.out);
	}
}
