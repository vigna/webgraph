/*
 * Copyright (C) 2007-2023 Sebastiano Vigna
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

import it.unimi.dsi.Util;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Stack;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanStack;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntStack;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.Transform.LabelledArcFilter;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

/** Computes the strongly connected components (and optionally the buckets) of an immutable graph.
 *
 * <p>The {@link #compute(ImmutableGraph, boolean, ProgressLogger)} method of this class will return
 * an instance that contains the data computed by running a variant of Tarjan's algorithm on an immutable graph.
 * The implementation is iterative, rather than recursive, to work around known limitations on the size of
 * the stack in current JVMs.
 * Besides the usually strongly connected components, it is possible to compute the <em>buckets</em> of the
 * graph, that is, nodes belonging to components that are terminal, but not dangling, in the component DAG.
 *
 * <p>After getting an instance, it is possible to run the {@link #computeSizes()} and {@link #sortBySize(int[])}
 * methods to obtain further information. This scheme has been devised to exploit the available memory as much
 * as possible&mdash;after the components have been computed, the returned instance keeps no track of
 * the graph, and the related memory can be freed by the garbage collector.
 */


public class StronglyConnectedComponents {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	private static final Logger LOGGER = LoggerFactory.getLogger(StronglyConnectedComponents.class);

	/** The number of strongly connected components. */
	final public int numberOfComponents;
	/** The component of each node. */
	final public int[] component;
	/** The bit vector for buckets, or <code>null</code>, in which case buckets have not been computed. */
	final public LongArrayBitVector buckets;

	protected StronglyConnectedComponents(final int numberOfComponents, final int[] status, final LongArrayBitVector buckets) {
		this.numberOfComponents = numberOfComponents;
		this.component = status;
		this.buckets = buckets;
	}

	private final static class Visit {
		/** The graph. */
		private final ImmutableGraph graph;
		/** The number of nodes in {@link #graph}. */
		private final int n;
		/** A progress logger. */
		private final ProgressLogger pl;
		/** Whether we should compute buckets. */
		private final boolean computeBuckets;
		/** For non visited nodes, 0. For visited non emitted nodes the visit time. For emitted node -c-1, where c is the component number. */
		private final int[] status;
		/** The buckets. */
		private final LongArrayBitVector buckets;
		/** The component stack. */
		private final IntArrayList componentStack;
		/** The first-visit clock (incremented at each visited node). */
		private int clock;
		/** The number of components already output. */
		private int numberOfComponents;

		private Visit(final ImmutableGraph graph, final int[] status, final LongArrayBitVector buckets, final ProgressLogger pl) {
			this.graph = graph;
			this.buckets = buckets;
			this.status = status;
			this.pl = pl;
			this.computeBuckets = buckets != null;
			this.n = graph.numNodes();
			componentStack = new IntArrayList(n);
		}

		/** Performs a visit starting form a given node.
		 *
		 * @param startNode the first node to visit.
		 */
		private void visit(final int startNode) {
			final BooleanStack olderNodeFound = new BooleanArrayList();
			final IntStack nodeStack = new IntArrayList();
			final Stack<LazyIntIterator> successorsStack = new ObjectArrayList<>();
			final int[] status = this.status;
			// For simplicify, we compute nonbuckets and then flip the values.
			final LongArrayBitVector nonBuckets = this.buckets;

			status[startNode] = ++clock;
			componentStack.push(startNode);
			nodeStack.push(startNode);
			successorsStack.push(graph.successors(startNode));
			olderNodeFound.push(false);
			if (computeBuckets && graph.outdegree(startNode) == 0) nonBuckets.set(startNode);

			main: while(! nodeStack.isEmpty()) {
				final int currentNode = nodeStack.topInt();
				final LazyIntIterator successors = successorsStack.top();

				for(int s; (s = successors.nextInt()) != -1;) {
					final int successorStatus = status[s];
					if (successorStatus == 0) {
						status[s] = ++clock;
						nodeStack.push(s);
						componentStack.push(s);
						successorsStack.push(graph.successors(s));
						olderNodeFound.push(false);
						if (computeBuckets && graph.outdegree(s) == 0) nonBuckets.set(s);
						continue main;
					}
					else if (successorStatus > 0) {
						if (successorStatus < status[currentNode]) {
							status[currentNode]  = successorStatus;
							olderNodeFound.popBoolean();
							olderNodeFound.push(true);
						}
					}
					else if (computeBuckets) nonBuckets.set(currentNode);
				}

				nodeStack.popInt();
				successorsStack.pop();
				if (pl != null) pl.lightUpdate();

				if (olderNodeFound.popBoolean()) {
					final int parentNode = nodeStack.topInt();
					final int currentNodeStatus = status[currentNode];
					if (currentNodeStatus < status[parentNode]) {
						status[parentNode]  = currentNodeStatus;
						olderNodeFound.popBoolean();
						olderNodeFound.push(true);
					}

					if (computeBuckets && nonBuckets.getBoolean(currentNode)) nonBuckets.set(parentNode);
				}
				else {
					if (computeBuckets && ! nodeStack.isEmpty()) nonBuckets.set(nodeStack.topInt());
					final boolean notABucket = computeBuckets ? nonBuckets.getBoolean(currentNode) : false;
					numberOfComponents++;
					int z;
					do {
						z = componentStack.popInt();
						// Component markers are -c-1, where c is the component number.
						status[z] = -numberOfComponents;
						if (notABucket) nonBuckets.set(z);
					} while(z != currentNode);
				}
			}
		}


		public void run() {

			if (pl != null) {
				pl.itemsName = "nodes";
				pl.expectedUpdates = n;
				pl.displayFreeMemory = true;
				pl.start("Computing strongly connected components...");
			}
			for (int x = 0; x < n; x++) if (status[x] == 0) visit(x);
			if (pl != null) pl.done();

			// Turn component markers into component numbers.
			for(int i = status.length; i-- != 0;) status[i] = -status[i] - 1;

			if (buckets != null) buckets.flip();
		}
	}

	/** Computes the strongly connected components of a given graph.
	 *
	 * @param graph the graph whose strongly connected components are to be computed.
	 * @param computeBuckets if true, buckets will be computed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an instance of this class containing the computed components.
	 */
	public static StronglyConnectedComponents compute(final ImmutableGraph graph, final boolean computeBuckets, final ProgressLogger pl) {
		final int n = graph.numNodes();
		final Visit visit = new Visit(graph, new int[n], computeBuckets ? LongArrayBitVector.ofLength(n) : null, pl);
		visit.run();
		return new StronglyConnectedComponents(visit.numberOfComponents, visit.status, visit.buckets);
	}


	private final static class FilteredVisit {
		/** The graph. */
		private final ArcLabelledImmutableGraph graph;
		/** The number of nodes in {@link #graph}. */
		private final int n;
		/** A progress logger. */
		private final ProgressLogger pl;
		/** A filter on arc labels. */
		private final LabelledArcFilter filter;
		/** Whether we should compute buckets. */
		private final boolean computeBuckets;
		/** For non visited nodes, 0. For visited non emitted nodes the visit time. For emitted node -c-1, where c is the component number. */
		private final int[] status;
		/** The buckets. */
		private final LongArrayBitVector buckets;
		/** The component stack. */
		private final IntArrayList componentStack;
		/** The first-visit clock (incremented at each visited node). */
		private int clock;
		/** The number of components already output. */
		private int numberOfComponents;

		private FilteredVisit(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final int[] status, final LongArrayBitVector buckets, final ProgressLogger pl) {
			this.graph = graph;
			this.filter = filter;
			this.buckets = buckets;
			this.status = status;
			this.pl = pl;
			this.computeBuckets = buckets != null;
			this.n = graph.numNodes();
			componentStack = new IntArrayList(n);
		}

		private long filteredOutdegree(final int node) {
			// Definitely not so efficient, ma very simple.
			long filteredOutdegree = 0;
			final LabelledArcIterator successors = graph.successors(node);
			for(int s; (s = successors.nextInt()) != -1;) if (filter.accept(node, s, successors.label())) filteredOutdegree++;
			return filteredOutdegree;
		}

		/** Performs a visit starting form a given node.
		 *
		 * @param startNode the first node to visit.
		 */
		private void visit(final int startNode) {
			final LongArrayBitVector olderNodeFound = LongArrayBitVector.ofLength(n);
			final IntStack nodeStack = new IntArrayList();
			final Stack<LabelledArcIterator> successorsStack = new ObjectArrayList<>();
			final int[] status = this.status;
			// For simplicify, we compute nonbuckets and then flip the values.
			final LongArrayBitVector nonBuckets = this.buckets;

			status[startNode]  = ++clock;
			componentStack.push(startNode);
			nodeStack.push(startNode);
			successorsStack.push(graph.successors(startNode));
			if (computeBuckets && filteredOutdegree(startNode) == 0) nonBuckets.set(startNode);

			main: while(! nodeStack.isEmpty()) {
				final int currentNode = nodeStack.topInt();
				final LabelledArcIterator successors = successorsStack.top();

				for(int s; (s = successors.nextInt()) != -1;) {
					if (! filter.accept(currentNode, s, successors.label())) continue;
					final int successorStatus = status[s];
					if (successorStatus == 0) {
						status[s] = ++clock;
						nodeStack.push(s);
						componentStack.push(s);
						successorsStack.push(graph.successors(s));
						if (computeBuckets && filteredOutdegree(s) == 0) nonBuckets.set(s);
						continue main;
					}
					else if (successorStatus > 0) {
						if (successorStatus < status[currentNode]) {
							status[currentNode] = successorStatus;
							olderNodeFound.set(currentNode);
						}
					}
					else if (computeBuckets) nonBuckets.set(currentNode);
				}

				nodeStack.popInt();
				successorsStack.pop();
				if (pl != null) pl.lightUpdate();

				if (olderNodeFound.getBoolean(currentNode)) {
					final int parentNode = nodeStack.topInt();
					final int currentNodeStatus = status[currentNode];
					if (currentNodeStatus < status[parentNode]) {
						status[parentNode] = currentNodeStatus;
						olderNodeFound.set(parentNode);
					}

					if (computeBuckets && nonBuckets.getBoolean(currentNode)) nonBuckets.set(parentNode);
				}
				else {
					if (computeBuckets && ! nodeStack.isEmpty()) nonBuckets.set(nodeStack.topInt());
					final boolean notABucket = computeBuckets ? nonBuckets.getBoolean(currentNode) : false;
					numberOfComponents++;
					int z;
					do {
						z = componentStack.popInt();
						// Component markers are -c-1, where c is the component number.
						status[z] = -numberOfComponents;
						if (notABucket) nonBuckets.set(z);
					} while(z != currentNode);
				}
			}
		}


		public void run() {

			if (pl != null) {
				pl.itemsName = "nodes";
				pl.expectedUpdates = n;
				pl.displayFreeMemory = true;
				pl.start("Computing strongly connected components...");
			}
			for (int x = 0; x < n; x++) if (status[x] == 0) visit(x);
			if (pl != null) pl.done();

			// Turn component markers into component numbers.
			for(int i = status.length; i-- != 0;) status[i] = -status[i] - 1;

			if (buckets != null) buckets.flip();
		}
	}

	/** Computes the strongly connected components of a given arc-labelled graph, filtering its arcs.
	 *
	 * @param graph the arc-labelled graph whose strongly connected components are to be computed.
	 * @param filter a filter selecting the arcs that must be taken into consideration.
	 * @param computeBuckets if true, buckets will be computed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an instance of this class containing the computed components.
	 */
	public static StronglyConnectedComponents compute(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final boolean computeBuckets, final ProgressLogger pl) {
		final int n = graph.numNodes();
		final FilteredVisit filteredVisit = new FilteredVisit(graph, filter, new int[n], computeBuckets ? LongArrayBitVector.ofLength(n) : null, pl);
		filteredVisit.run();
		return new StronglyConnectedComponents(filteredVisit.numberOfComponents, filteredVisit.status, filteredVisit.buckets);
	}


	/** Returns the size array for this set of strongly connected components.
	 *
	 * @return the size array for this set of strongly connected components.
	 */
	public int[] computeSizes() {
		final int[] size = new int[numberOfComponents];
		for(int i = component.length; i-- != 0;) size[component[i]]++;
		return size;
	}

	/** Renumbers by decreasing size the components of this set.
	 *
	 * <p>After a call to this method, both the internal status of this class and the argument
	 * array are permuted so that the sizes of strongly connected components are decreasing
	 * in the component index.
	 *
	 *  @param size the components sizes, as returned by {@link #computeSizes()}.
	 */
	public void sortBySize(final int[] size) {
		final int[] perm = Util.identity(size.length);
		IntArrays.parallelRadixSortIndirect(perm, size, false);
		IntArrays.reverse(perm);
		final int[] copy = size.clone();
		for (int i = size.length; i-- != 0;) size[i] = copy[perm[i]];
		Util.invertPermutationInPlace(perm);
		for(int i = component.length; i-- != 0;) component[i] = perm[component[i]];
	}



	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(StronglyConnectedComponents.class.getName(),
				"Computes the strongly connected components (and optionally the buckets) of a graph of given basename. The resulting data is saved " +
				"in files stemmed from the given basename with extension .scc (a list of binary integers specifying the " +
				"component of each node), .sccsizes (a list of binary integer specifying the size of each component) and .buckets " +
				" (a serialised LongArrayBigVector specifying buckets). Please use suitable JVM options to set a large stack size.",
				new Parameter[] {
			new Switch("sizes", 's', "sizes", "Compute component sizes."),
			new Switch("renumber", 'r', "renumber", "Renumber components in decreasing-size order."),
			new Switch("buckets", 'b', "buckets", "Compute buckets (nodes belonging to a bucket component, i.e., a terminal nondangling component)."),
			new FlaggedOption("filter", new ObjectParser(LabelledArcFilter.class, GraphClassParser.PACKAGE), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "filter", "A filter for labelled arcs; requires the provided graph to be arc labelled."),
			new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("resultsBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the resulting files."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final String resultsBasename = jsapResult.getString("resultsBasename", basename);
		final LabelledArcFilter filter = (LabelledArcFilter)jsapResult.getObject("filter");
		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);

		final StronglyConnectedComponents components =
			filter != null ? StronglyConnectedComponents.compute(ArcLabelledImmutableGraph.load(basename), filter, jsapResult.getBoolean("buckets"), pl)
					: StronglyConnectedComponents.compute(ImmutableGraph.load(basename), jsapResult.getBoolean("buckets"), pl);

		if (jsapResult.getBoolean("sizes") || jsapResult.getBoolean("renumber")) {
			final int size[] = components.computeSizes();
			if (jsapResult.getBoolean("renumber")) components.sortBySize(size);
			if (jsapResult.getBoolean("sizes")) BinIO.storeInts(size, resultsBasename + ".sccsizes");
		}
		BinIO.storeInts(components.component, resultsBasename + ".scc");
		if (components.buckets != null) BinIO.storeObject(components.buckets, resultsBasename + ".buckets");
	}
}
