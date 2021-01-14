/*
 * Copyright (C) 2010-2020 Paolo Boldi and Sebastiano Vigna
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
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;

/** Computes the neighbourhood function of a graph by multiple {@linkplain ParallelBreadthFirstVisit parallel breadth-first visits}.
 *
 * <p>Note that performing all breadth-first visits requires time <i>O</i>(<var>n</var><var>m</var>), so this class
 * is usable only on very small graphs.
 *
 * <p>Additionally, this class provides several useful static methods such as {@link #distanceCumulativeDistributionFunction(double[])},
 * {@link #distanceProbabilityMassFunction(double[])}, {@link #averageDistance(double[])}, {@link #medianDistance(int, double[])}
 * and {@link #spid(double[])} that act on neighbourhood functions.
 *
 * <h2>Performance issues</h2>
 *
 * <p>This class uses an instance of {@link ParallelBreadthFirstVisit} to ensure a high degree of parallelism (see its
 * documentation for memory requirements). Note that if the graph is small a large number of thread will slow down the computation because of synchronization costs.
 *
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 */
public class NeighbourhoodFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(NeighbourhoodFunction.class);

	/** Computes and returns the neighbourhood function of the specified graph by multiple breadth-first visits.
	 *
	 * <p>This method returns an array of doubles. When some values of the function are near 2<sup>63</sup>, it
	 * might lose some least-significant digits. If you need exact values,
	 * use {@link #computeExact(ImmutableGraph, int, ProgressLogger)} instead.
	 *
	 * @param g a graph.
	 * @return the neighbourhood function of the specified graph.
	 */
	public static double[] compute(final ImmutableGraph g) {
		return compute(g, null);
	}

	/** Computes and returns the neighbourhood function of the specified graph by multiple breadth-first visits.
	 *
	 * <p>This method returns an array of doubles. When some values of the function are near 2<sup>63</sup>, it
	 * might lose some least-significant digits. If you need exact values,
	 * use {@link #computeExact(ImmutableGraph, int, ProgressLogger)} instead.
	 *
	 * @param g a graph.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the neighbourhood function of the specified graph.
	 */
	public static double[] compute(final ImmutableGraph g, final ProgressLogger pl) {
		return compute(g, 0, pl);
	}

	/** Computes and returns the neighbourhood function of the specified graph by multiple breadth-first visits.
	 *
	 * <p>This method returns an array of doubles. When some values of the function are near 2<sup>63</sup>, it
	 * might lose some least-significant digits. If you need exact values,
	 * use {@link #computeExact(ImmutableGraph, int, ProgressLogger)} instead.
	 *
	 * @param g a graph.
	 * @param threads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * Note that if the graph is small a large number of thread will slow down the computation because of synchronization costs.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the neighbourhood function of the specified graph.
	 */
	public static double[] compute(final ImmutableGraph g, final int threads, final ProgressLogger pl) {
		final long[] computeExact = computeExact(g, threads, pl);
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
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the neighbourhood function of the specified graph as an array of longs.
	 */
	public static long[] computeExact(final ImmutableGraph g, final int threads, final ProgressLogger pl) {
		final int n = g.numNodes();
		long count[] = LongArrays.EMPTY_ARRAY;

		final ParallelBreadthFirstVisit visit = new ParallelBreadthFirstVisit(g, threads, true, null);

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start();
		}

		for(int i = 0; i < n; i++) {
			visit.clear();
			visit.visit(i);
			final int maxDistance = visit.maxDistance();
			if (count.length <= maxDistance) count = LongArrays.grow(count, maxDistance + 1);
			for(int d = maxDistance + 1; d-- != 0;) count[d] += visit.cutPoints.getInt(d + 1) - visit.cutPoints.getInt(d);
			if (pl != null) pl.update();
		}

		if (pl != null) pl.done();

		int last;
		for(last = count.length; last-- != 0 && count[last] == 0;);
		last++;
		final long[] result = new long[last];
		result[0] = count[0];
		for(int i = 1; i < last; i++) result[i] = result[i - 1] + count[i];
		return result;
	}

	/** Returns the distance cumulative distribution function.
	 *
	 * @param neighbourhoodFunction a neighbourhood function or distance cumulative distribution function.
	 * @return the distance cumulative distribution function.
	 */
	public static double[] distanceCumulativeDistributionFunction(final double[] neighbourhoodFunction) {
		final double[] result = neighbourhoodFunction.clone();
		final double lastValue = result[result.length - 1];
		for(int d = result.length; d-- != 0;) result[d] /= lastValue;
		return result;
	}

	/** Returns the probability mass function of the distance distribution.
	 *
	 * @param neighbourhoodFunction a neighbourhood function or distance cumulative distribution function.
	 * @return the probability mass function of the distance distribution.
	 */
	public static double[] distanceProbabilityMassFunction(final double[] neighbourhoodFunction) {
		final double[] result = neighbourhoodFunction.clone();
		final double lastValue = result[result.length - 1];
		// Not necessary, but not harmful.
		for(int d = result.length; d-- != 0;) result[d] /= lastValue;
		for(int d = result.length; d-- != 1;) result[d] -= result[d - 1];
		return result;
	}



	/** Returns the effective diameter at a specified fraction.
	 *
	 * @param alpha the desired fraction of reachable pairs of nodes (usually, 0.9).
	 * @param neighbourhoodFunction a neighbourhood function or distance cumulative distribution function.
	 * @return the effective diameter at <code>fraction</code>.
	 */
	public static double effectiveDiameter(final double alpha, final double[] neighbourhoodFunction) {
		final double finalFraction = neighbourhoodFunction[neighbourhoodFunction.length - 1];
		int d;
		for (d = 0; neighbourhoodFunction[d] / finalFraction < alpha; d++);

		if (d == 0) // In this case we assume the previous ordinate to be zero
			return d + (alpha * finalFraction - neighbourhoodFunction[d]) / (neighbourhoodFunction[d]);
		else
			return d + (alpha * finalFraction - neighbourhoodFunction[d]) / (neighbourhoodFunction[d] - neighbourhoodFunction[d - 1]);
	}

	/** Returns the effective diameter at 0.9.
	 *
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the effective diameter at 0.9.
	 */
	public static double effectiveDiameter(final double[] neighbourhoodFunction) {
		return effectiveDiameter(.9, neighbourhoodFunction);
	}

	/** Returns the median of distances between all pairs of nodes.
	 *
	 * @param neighbourhoodFunction a neighbourhood function.
	 * @return the median distance, which might be {@link Double#POSITIVE_INFINITY} if less than half
	 * of the pairs of nodes are reachable.
	 */
	public static double medianDistance(final double[] neighbourhoodFunction) {
		return medianDistance((int)Math.round(neighbourhoodFunction[0]),  neighbourhoodFunction);
	}

	/** Returns the median of distances between all pairs of nodes.
	 *
	 * <p>Note that if you have an actual neighbourhood function, you can safely pass its first value
	 * as first argument; however, having the number of nodes as a separate input
	 * makes it possible passing this method a distance cumulative distribution
	 * function, too.
	 *
	 * @param n the number of nodes in the graph.
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the median distance, which might be {@link Double#POSITIVE_INFINITY} if less than half
	 * of the pairs of nodes are reachable.
	 */
	public static double medianDistance(final int n, final double[] neighbourhoodFunction) {
		final double halfPairs = .5 * n * n;
		int d;
		for (d = neighbourhoodFunction.length; d-- != 0 && neighbourhoodFunction[d] > halfPairs;);
		return d == neighbourhoodFunction.length - 1 ? Double.POSITIVE_INFINITY : d + 1;
	}

	/** Returns the spid (shortest-paths index of dispersion).
	 *
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the spid.
	 */
	public static double spid(final double[] neighbourhoodFunction) {
		final double[] distanceProbabilityMassFunction = NeighbourhoodFunction.distanceProbabilityMassFunction(neighbourhoodFunction);
		double mean = 0,  meanOfSquares = 0;
		for(int i = 0; i < distanceProbabilityMassFunction.length; i++) {
			mean += distanceProbabilityMassFunction[i] * i;
			meanOfSquares += distanceProbabilityMassFunction[i] * i * i;
		}

		return (meanOfSquares - mean * mean) / mean;
	}

	/** Returns the average of the distances between reachable pairs of nodes.
	 *
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the average of the distances between reachable pairs of nodes.
	 */
	public static double averageDistance(final double[] neighbourhoodFunction) {
		final double[] distanceProbabilityMassFunction = NeighbourhoodFunction.distanceProbabilityMassFunction(neighbourhoodFunction);
		double mean = 0;
		for(int i = 0; i < distanceProbabilityMassFunction.length; i++) mean += distanceProbabilityMassFunction[i] * i;
		return mean;
	}

	/** Returns the harmonic diameter, that is, the harmonic mean of all distances.
	 *
	 * @param neighbourhoodFunction a neighbourhood function.
	 * @return the harmonic diameter.
	 */
	public static double harmonicDiameter(final double[] neighbourhoodFunction) {
		return harmonicDiameter((int)Math.round(neighbourhoodFunction[0]), neighbourhoodFunction);
	}

	/** Returns the harmonic diameter, that is, the harmonic mean of all distances.
	 *
	 * <p>Note that if you have an actual neighbourhood function, you can safely pass its first value
	 * as first argument; however, having the number of nodes as a separate input
	 * makes it possible passing this method a distance cumulative distribution
	 * function, too.
	 *
	 * @param n the number of nodes in the graph.
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the harmonic diameter.
	 */
	public static double harmonicDiameter(final int n, final double[] neighbourhoodFunction) {
		double t = 0;
		for(int i = 1; i < neighbourhoodFunction.length; i++) t += (neighbourhoodFunction[i] - neighbourhoodFunction[i - 1]) / i;
		return (double)n * (n - 1) / t;
	}

	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(NeighbourhoodFunction.class.getName(),
				"Prints the neighbourhood function of a graph, computing it via parallel breadth-first visits.",
				new Parameter[] {
			new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically. Note that if the graph is small a large number of thread will slow down the computation because of synchronization costs."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		ImmutableGraph g =ImmutableGraph.load(basename);
		if (jsapResult.userSpecified("expand")) g = new ArrayListMutableGraph(g).immutableView();
		TextIO.storeLongs(computeExact(g, threads, pl), System.out);
	}
}

