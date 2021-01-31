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
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.UnionImmutableGraph;

/**
 * Computes the connected components of a <em>symmetric</em> (a.k.a&#46; <em>undirected</em>) graph
 * using a {@linkplain ParallelBreadthFirstVisit parallel breadth-first visit}.
 *
 * <p>The {@link #compute(ImmutableGraph, int, ProgressLogger)} method of this class will return an
 * instance that contains the data computed by visiting the graph (using an instance of
 * {@link ParallelBreadthFirstVisit}). Note that it is your responsibility to pass a symmetric graph
 * to {@link #compute(ImmutableGraph, int, ProgressLogger)}. Otherwise, results will be
 * unpredictable.
 *
 * <p>After getting an instance, it is possible to run the {@link #computeSizes()} and
 * {@link #sortBySize(int[])} methods to obtain further information. This scheme has been devised to
 * exploit the available memory as much as possible&mdash;after the components have been computed,
 * the returned instance keeps no track of the graph, and the related memory can be freed by the
 * garbage collector.
 *
 * <p>Furthermore, it is possible to remove all components except the biggest one from a graph,
 * using the function {@link #getLargestComponent}.
 *
 * <h2>Performance issues</h2>
 *
 * <p>This class uses an instance of {@link ParallelBreadthFirstVisit} to ensure a high degree of
 * parallelism (see its documentation for memory requirements).
 */

public class ConnectedComponents {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConnectedComponents.class);

	/** The number of connected components. */
	public final int numberOfComponents;

	/** The component of each node. */
	public final int component[];

	protected ConnectedComponents(final int numberOfComponents, final int[] component) {
		this.numberOfComponents = numberOfComponents;
		this.component = component;
	}

	/**
	 * Computes the connected components of a symmetric graph.
	 *
	 * @param symGraph a symmetric graph.
	 * @param threads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an instance of this class containing the computed components.
	 */
	public static ConnectedComponents compute(final ImmutableGraph symGraph, final int threads, final ProgressLogger pl) {
		ParallelBreadthFirstVisit visit = new ParallelBreadthFirstVisit(symGraph, threads, false, pl);
		visit.visitAll();
		final AtomicIntegerArray visited = visit.marker;
		final int numberOfComponents = visit.round + 1;
		visit = null;
		final int[] component = new int[visited.length()];
		for (int i = component.length; i-- != 0;)
			component[i] = visited.get(i);
		return new ConnectedComponents(numberOfComponents, component);
	}

	/**
	 * Returns the largest connected components of a symmetric graph.
	 *
	 * @param symGraph a symmetric graph.
	 * @param threads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an ImmutableGraph containing the largest connected component of the input graph.
	 */
	public static ImmutableGraph getLargestComponent(final ImmutableGraph symGraph, final int threads, final ProgressLogger pl) {
		ParallelBreadthFirstVisit visit = new ParallelBreadthFirstVisit(symGraph, threads, false, pl);
		visit.visitAll();
		final AtomicIntegerArray visited = visit.marker;
		final int numberOfComponents = visit.round + 1;
		visit = null;
		final int[] component = new int[visited.length()];
		final int[] componentSizes = new int [numberOfComponents];
		final int[] map = new int[symGraph.numNodes()];
		int largestCC = 0, largestCCSize = Integer.MIN_VALUE;

		for (int i = component.length; i-- != 0;) {
			component[i] = visited.get(i);
			componentSizes[component[i]]++;
		}
		for (int i = 0; i < componentSizes.length; i++)
			if (componentSizes[i] > largestCCSize) {
				largestCC = i;
				largestCCSize = componentSizes[i];
			}

		for (int i = symGraph.numNodes(); i-- != 0;) {
			if (component[i] == largestCC) {
				map[i] = --largestCCSize;
			} else {
				map[i] = -1;
			}
		}

		return Transform.map(symGraph, map, pl);
	}

	/**
	 * Returns the size array for this set of connected components.
	 *
	 * @return the size array for this set of connected components.
	 */
	public int[] computeSizes() {
		final int[] size = new int[numberOfComponents];
		for (int i = component.length; i-- != 0;)
			size[component[i]]++;
		return size;
	}

	/**
	 * Renumbers by decreasing size the components of this set.
	 *
	 * <p>After a call to this method, both the internal status of this class and the argument array
	 * are permuted so that the sizes of connected components are decreasing in the component index.
	 *
	 * @param size the components sizes, as returned by {@link #computeSizes()}.
	 */
	public void sortBySize(final int[] size) {
		final int[] perm = Util.identity(size.length);
		IntArrays.parallelRadixSortIndirect(perm, size, false);
		IntArrays.reverse(perm);
		final int[] copy = size.clone();
		for (int i = size.length; i-- != 0;)
			size[i] = copy[perm[i]];
		Util.invertPermutationInPlace(perm);
		for (int i = component.length; i-- != 0;)
			component[i] = perm[component[i]];
	}

	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(ConnectedComponents.class.getName(),
				"Computes the connected components of a symmetric graph of given basename. The resulting data is saved " +
				"in files stemmed from the given basename with extension .wcc (a list of binary integers specifying the " +
				"component of each node) and .wccsizes (a list of binary integer specifying the size of each component). " +
				"The symmetric graph can also be specified using a generic (non-symmetric) graph and its transpose.",
				new Parameter[] {
					new Switch("sizes", 's', "sizes", "Compute component sizes."),
					new Switch("renumber", 'r', "renumber", "Renumber components in decreasing-size order."),
					new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
					new Switch("mapped", 'm', "mapped", "Do not load the graph in main memory, but rather memory-map it."),
					new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
					new FlaggedOption("basenamet", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "transpose", "The basename of the transpose, in case the graph is not symmetric."),
					new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of a symmetric graph (or of a generic graph, if the transpose is provided, too)."),
					new UnflaggedOption("resultsBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the resulting files."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final String basenamet = jsapResult.getString("basenamet");
		final String resultsBasename = jsapResult.getString("resultsBasename", basename);
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);

		final ImmutableGraph graph = jsapResult.userSpecified("mapped") ? ImmutableGraph.loadMapped(basename) : ImmutableGraph.load(basename, pl);
		final ImmutableGraph grapht = basenamet == null ? null : jsapResult.userSpecified("mapped") ? ImmutableGraph.loadMapped(basenamet) : ImmutableGraph.load(basenamet, pl);
		final ConnectedComponents components = ConnectedComponents.compute(basenamet != null ? new UnionImmutableGraph(graph, grapht) : graph, threads, pl);

		if (jsapResult.getBoolean("sizes") || jsapResult.getBoolean("renumber")) {
			final int size[] = components.computeSizes();
			if (jsapResult.getBoolean("renumber")) components.sortBySize(size);
			if (jsapResult.getBoolean("sizes")) BinIO.storeInts(size, resultsBasename + ".wccsizes");
		}
		BinIO.storeInts(components.component, resultsBasename + ".wcc");
	}
}
