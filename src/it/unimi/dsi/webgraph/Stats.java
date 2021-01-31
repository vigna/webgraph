/*
 * Copyright (C) 2007-2021 Sebastiano Vigna
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

package it.unimi.dsi.webgraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.algo.StronglyConnectedComponents;

/** Computes basic statistical data about a given graph.
 *
 * <p>This class loads a graph of given basename, and computes the following data:
 * <ol>
 * <li>an ASCII file containing the <em>outdegree distribution</em>; line <var>n</var> contains the number of nodes with outdegree <var>n</var> (starting from 0);
 * <li>an ASCII file containing the <em>indegree distribution</em>; line <var>n</var> contains the number of nodes with indegree <var>n</var> (starting from 0);
 * <li>a property file containing several self-descriptive data, such as the average indegree/outdegree (which should be identical), sample nodes with minimum
 * or maximum indegree/outdegree, and so on; additional data will be computed if files produced by {@link StronglyConnectedComponents} are present
 * with the same basename (in particular, buckets and component sizes);
 * <li>if files produced by {@link StronglyConnectedComponents} are present with the same basename, an ASCII file containing the <em>distribution
 * of strongly connected components</em>, specified as a sequence of lines each containing a pair of integer &lt;<var>size</var>, <var>count</var>&gt;.
 * </ol>
 *
 * <p>The graph is loaded {@linkplain ImmutableGraph#loadOffline(CharSequence) offline}: the only memory allocated is for indegree count (one integer
 * per node) and for storing the actual counts (one integer per indegree/outdegree value).
 */

public class Stats {

	private Stats() {}

	/** Computes stats for the given graph using a single traversal, storing the results in files with given basename.
	 *
	 * @param graph the graph to be examined.
	 * @param buckets the set of buckets of this graph, or <code>null</code> if this information is not available.
	 * @param sccsize the sizes of strongly connected components, or <code>null</code> if this information is not available.
	 * @param resultsBasename the basename for result files (see the {@linkplain Stats class description}).
	 * @param pl a progress logger.
	 */

	public static void run(final ImmutableGraph graph, final LongArrayBitVector buckets, final int[] sccsize, final CharSequence resultsBasename, final ProgressLogger pl) throws IOException {
		run(graph, buckets, sccsize, resultsBasename, false, pl);
	}

	/** Computes stats for the given graph using a single traversal, storing the results in files with given basename.
	 *
	 * @param graph the graph to be examined.
	 * @param buckets the set of buckets of this graph, or <code>null</code> if this information is not available.
	 * @param sccsize the sizes of strongly connected components, or <code>null</code> if this information is not available.
	 * @param resultsBasename the basename for result files (see the {@linkplain Stats class description}).
	 * @param saveDegrees if true, indegrees and outdegrees will be saved.
	 * @param pl a progress logger.
	 */

	public static void run(final ImmutableGraph graph, final LongArrayBitVector buckets, final int[] sccsize, final CharSequence resultsBasename, final boolean saveDegrees, final ProgressLogger pl) throws IOException {
		final NodeIterator nodeIterator = graph.nodeIterator();
		int[] count = IntArrays.EMPTY_ARRAY;
		final int[] indegree = new int[graph.numNodes()];
		int[] successor;
		int curr, maxd = 0, maxNode = 0, mind = Integer.MAX_VALUE, minNode = 0;
		long dangling = 0, terminal = 0, loops = 0, numArcs = 0, numGaps = 0;
		BigInteger totLoc = BigInteger.ZERO, totGap = BigInteger.ZERO;


		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = graph.numNodes();
			pl.start("Scanning...");
		}

		final PrintWriter outdegreesPrintWriter = saveDegrees ? new PrintWriter(new BufferedWriter(new FileWriter(resultsBasename + ".outdegrees"))) : null;

		/** Statistics for the gap width of successor lists (exponentially binned). */
		final long[] successorDeltaStats = new long[32];

		for(int i = graph.numNodes(); i-- != 0;) {
			curr = nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			if (saveDegrees) outdegreesPrintWriter.println(d);
			successor = nodeIterator.successorArray();

			if (d > 1) {
				totGap = totGap.add(BigInteger.valueOf(successor[d - 1] - successor[0]));
				totGap = totGap.add(BigInteger.valueOf(Fast.int2nat(successor[0] - curr)));
				numGaps += d ;
			}
			for(int s = d; s-- != 0;) {
				totLoc = totLoc.add(BigInteger.valueOf(Math.abs(successor[s] - curr)));

				if (successor[s] != curr) successorDeltaStats[Fast.mostSignificantBit(Math.abs(curr - successor[s]))]++;
				else loops++;

				indegree[successor[s]]++;
			}

			if (d == 0) {
				dangling++;
				terminal++;
			}

			if (d == 1 && successor[0] == curr) terminal++;

			if (d < mind) {
				mind = d;
				minNode = curr;
			}

			if (d > maxd){
				maxd = d;
				maxNode = curr;
			}

			numArcs += d;

			if (d >= count.length) count = IntArrays.grow(count, d + 1);
			count[d]++;

			if (pl != null) pl.lightUpdate();
		}

		if (pl != null) pl.done();

		if (saveDegrees) {
			outdegreesPrintWriter.close();
			TextIO.storeInts(indegree, resultsBasename + ".indegrees");
		}

		@SuppressWarnings("resource")
		final PrintWriter properties = new PrintWriter(new FileWriter(resultsBasename + ".stats"));
		properties.println("nodes=" + graph.numNodes());
		properties.println("arcs=" + numArcs);
		properties.println("loops=" + loops);
		properties.println("successoravggap=" + new BigDecimal(totGap).divide(BigDecimal.valueOf(Math.max(1, numGaps)), 3, RoundingMode.HALF_EVEN));
		properties.println("avglocality=" + new BigDecimal(totLoc).divide(BigDecimal.valueOf(Math.max(1, numArcs)), 3, RoundingMode.HALF_EVEN));
		properties.println("minoutdegree=" + mind);
		properties.println("maxoutdegree=" + maxd);
		properties.println("minoutdegreenode=" + minNode);
		properties.println("maxoutdegreenode=" + maxNode);
		properties.println("dangling=" + dangling);
		properties.println("terminal=" + terminal);
		properties.println("percdangling=" + 100.0 * dangling / graph.numNodes());
		properties.println("avgoutdegree=" + (double)numArcs/graph.numNodes());

		int l;
		for(l = successorDeltaStats.length; l-- != 0;) if (successorDeltaStats[l] != 0) break;
		final StringBuilder s = new StringBuilder();
		double totLogDelta = 0;
		long numDelta = 0;

		long g = 1;
		for(int i = 0; i <= l; i++) {
			if (i != 0) s.append(',');
			s.append(successorDeltaStats[i]);
			numDelta += successorDeltaStats[i];
			totLogDelta += (Fast.log2(g * 2 + g + 1) - 1) * successorDeltaStats[i];
			g *= 2;
		}

		properties.println("successorlogdeltastats=" + s.toString());
		properties.println("successoravglogdelta=" + (numDelta == 0 ? "0" : new BigDecimal(totLogDelta).divide(BigDecimal.valueOf(Math.max(1, numDelta * 2)), 3, RoundingMode.HALF_EVEN).toString()));

		TextIO.storeInts(count, 0, maxd + 1, resultsBasename + ".outdegree");

		Arrays.fill(count, 0);

		maxd = maxNode = minNode = 0;
		mind = Integer.MAX_VALUE;
		for(int i = indegree.length; i-- != 0;) {
			final int d = indegree[i];
			if (d >= count.length) count = IntArrays.grow(count, d + 1);
			if (d < mind) {
				mind = d;
				minNode = i;
			}

			if (d > maxd){
				maxd = d;
				maxNode = i;
			}

			count[d]++;
		}

		TextIO.storeInts(count, 0, maxd + 1, resultsBasename + ".indegree");

		properties.println("minindegree=" + mind);
		properties.println("maxindegree=" + maxd);
		properties.println("minindegreenode=" + minNode);
		properties.println("maxindegreenode=" + maxNode);
		properties.println("avgindegree=" + (double)numArcs/graph.numNodes());

		if (buckets != null) {
			final long numBuckets = buckets.count();
			properties.println("buckets=" + numBuckets);
			properties.println("percbuckets=" + 100.0 * numBuckets / graph.numNodes());
		}

		if (sccsize != null) {
			IntArrays.parallelQuickSort(sccsize);
			final int m = sccsize.length;
			final int maxSize = sccsize[m - 1];
			final int minSize = sccsize[0];

			properties.println("sccs=" + m);
			properties.println("maxsccsize=" + maxSize);
			properties.println("percmaxscc=" + 100.0 * maxSize / graph.numNodes());
			properties.println("minsccsize=" + minSize);
			properties.println("percminscc=" + 100.0 * minSize / graph.numNodes());

			final PrintWriter pw = new PrintWriter(resultsBasename + ".sccdistr");
			int current = maxSize;
			int c = 0;
			for(int i = sccsize.length; i-- != 0;) {
				if(sccsize[i] != current) {
					pw.println(current + "\t" + c);
					current = sccsize[i];
					c = 0;
				}
				c++;
			}
			pw.println(current + "\t" + c);

			pw.flush();
			pw.close();
		}

		properties.close();
	}

	static public void main(final String arg[]) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, IOException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(Stats.class.getName(), "Computes statistical data of a given graph.",
				new Parameter[] {
						new FlaggedOption("graphClass", GraphClassParser.getParser(), null, JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java class for the source graph."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new Switch("saveDegrees", 's', "save-degrees", "Save indegrees and outdegrees in text format."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
						new UnflaggedOption("resultsBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the resulting files."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final Class<?> graphClass = jsapResult.getClass("graphClass");
		final String basename = jsapResult.getString("basename");
		final String resultsBasename = jsapResult.userSpecified("resultsBasename") ? jsapResult.getString("resultsBasename") : basename;

		final ProgressLogger pl = new ProgressLogger();
		pl.logInterval = jsapResult.getLong("logInterval");

		final ImmutableGraph graph;

		if (graphClass != null) graph = (ImmutableGraph)graphClass.getMethod("loadOffline", CharSequence.class).invoke(null, basename);
		else graph = ImmutableGraph.loadOffline(basename, pl);

		final LongArrayBitVector buckets = (LongArrayBitVector)(new File(basename + ".buckets").exists() ? BinIO.loadObject(basename + ".buckets") : null);
		final int[] sccsize = new File(basename + ".sccsizes").exists() ? BinIO.loadInts(basename + ".sccsizes") : null;

		run(graph, buckets, sccsize, resultsBasename, jsapResult.getBoolean("saveDegrees"), pl);
	}
}
