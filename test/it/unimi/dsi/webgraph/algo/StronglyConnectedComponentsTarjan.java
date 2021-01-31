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

package it.unimi.dsi.webgraph.algo;

import java.util.BitSet;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntStack;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.Transform.LabelledArcFilter;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

/** Computes the strongly connected components (and optionally the buckets) of an immutable graph.
 *
 * <p>This class is a double implementation for debugging purposes.
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
 *
 * <h2>Stack size</h2>
 *
 * <p>The method {@link #compute(ImmutableGraph, boolean, ProgressLogger)} might require a large stack size,
 * that should be set using suitable JVM options. Note, however,
 * that the stack size must be enlarged also on the operating-system side&mdash;for instance, using <code>ulimit -s unlimited</code>.
 */


public class StronglyConnectedComponentsTarjan {
	/** The number of strongly connected components. */
	final public int numberOfComponents;
	/** The component of each node. */
	final public int component[];
	/** The bit set for buckets, or <code>null</code>, in which case buckets have not been computed. */
	final public BitSet buckets;

	protected StronglyConnectedComponentsTarjan(final int numberOfComponents, final int[] component, final BitSet buckets) {
		this.numberOfComponents = numberOfComponents;
		this.component = component;
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
		private final int status[];
		/** The buckets. */
		private final BitSet buckets;
		/** The component stack. */
		private final IntStack stack;

		/** The first-visit clock (incremented at each visited node). */
		private int clock;
		/** The number of components already output. */
		private int numberOfComponents;

		private Visit(final ImmutableGraph graph, final int[] status, final BitSet buckets, final ProgressLogger pl) {
			this.graph = graph;
			this.buckets = buckets;
			this.status = status;
			this.pl = pl;
			this.computeBuckets = buckets != null;
			this.n = graph.numNodes();
			stack = new IntArrayList(n);
		}

		/** Visits a node.
		 *
		 * @param x the node to visit.
		 * @return true if <code>x</code> is a bucket.
		 */
		private boolean visit(final int x) {
			final int[] status = this.status;
			if (pl != null) pl.lightUpdate();
			status[x] = ++clock;
			stack.push(x);

			int d = graph.outdegree(x);
			boolean noOlderNodeFound = true, isBucket = d != 0; // If we're dangling we're certainly not a bucket.

			if (d != 0) {
				final LazyIntIterator successors = graph.successors(x);
				while(d-- != 0) {
					final int s = successors.nextInt();
					// If we can reach a non-bucket or another component we are not a bucket.
					if (status[s] == 0 && ! visit(s) || status[s] < 0) isBucket = false;
					if (status[s] > 0 && status[s] < status[x]) {
						status[x] = status[s];
						noOlderNodeFound = false;
					}
				}
			}

			if (noOlderNodeFound) {
				numberOfComponents++;
				int z;
				do {
					z = stack.popInt();
					// Component markers are -c-1, where c is the component number.
					status[z] = -numberOfComponents;
					if (isBucket && computeBuckets) buckets.set(z);
				} while(z != x);
			}

			return isBucket;
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
			for (int x = n; x-- != 0;) status[x] = -status[x] - 1;

			stack.push(numberOfComponents); // Horrible kluge to return the number of components.
		}
	}

	/** Computes the strongly connected components of a given graph.
	 *
	 * @param graph the graph whose strongly connected components are to be computed.
	 * @param computeBuckets if true, buckets will be computed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an instance of this class containing the computed components.
	 */
	public static StronglyConnectedComponentsTarjan compute(final ImmutableGraph graph, final boolean computeBuckets, final ProgressLogger pl) {
		final int n = graph.numNodes();
		final Visit visit = new Visit(graph, new int[n], computeBuckets ? new BitSet(n) : null, pl);
		visit.run();
		return new StronglyConnectedComponentsTarjan(visit.numberOfComponents, visit.status, visit.buckets);
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
		private final int status[];
		/** The buckets. */
		private final BitSet buckets;
		/** The component stack. */
		private final IntStack stack;


		/** The first-visit clock (incremented at each visited node). */
		private int clock;
		/** The number of components already output. */
		private int numberOfComponents;

		private FilteredVisit(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final int[] status, final BitSet buckets, final ProgressLogger pl) {
			this.graph = graph;
			this.filter = filter;
			this.buckets = buckets;
			this.status = status;
			this.pl = pl;
			this.computeBuckets = buckets != null;
			this.n = graph.numNodes();
			stack = new IntArrayList(n);
		}

		/** Visits a node.
		 *
		 * @param x the node to visit.
		 * @return true if <code>x</code> is a bucket.
		 */
		private boolean visit(final int x) {
			final int[] status = this.status;
			if (pl != null) pl.lightUpdate();
			status[x] = ++clock;
			stack.push(x);

			int d = graph.outdegree(x), filteredDegree = 0;
			boolean noOlderNodeFound = true, isBucket = true;

			if (d != 0) {
				final LabelledArcIterator successors = graph.successors(x);
				while(d-- != 0) {
					final int s = successors.nextInt();
					if (! filter.accept(x, s, successors.label())) continue;
					filteredDegree++;
					// If we can reach a non-bucket or another component we are not a bucket.
					if (status[s] == 0 && ! visit(s) || status[s] < 0) isBucket = false;
					if (status[s] > 0 && status[s] < status[x]) {
						status[x] = status[s];
						noOlderNodeFound = false;
					}
				}
			}

			if (filteredDegree == 0) isBucket = false;

			if (noOlderNodeFound) {
				numberOfComponents++;
				int z;
				do {
					z = stack.popInt();
					// Component markers are -c-1, where c is the component number.
					status[z] = -numberOfComponents;
					if (isBucket && computeBuckets) buckets.set(z);
				} while(z != x);
			}

			return isBucket;
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
			for (int x = n; x-- != 0;) status[x] = -status[x] - 1;

			stack.push(numberOfComponents); // Horrible kluge to return the number of components.
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
	public static StronglyConnectedComponentsTarjan compute(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final boolean computeBuckets, final ProgressLogger pl) {
		final int n = graph.numNodes();
		final FilteredVisit filteredVisit = new FilteredVisit(graph, filter, new int[n], computeBuckets ? new BitSet(n) : null, pl);
		filteredVisit.run();
		return new StronglyConnectedComponentsTarjan(filteredVisit.numberOfComponents, filteredVisit.status, filteredVisit.buckets);
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
		IntArrays.quickSort(perm, 0, perm.length, (x,y) -> Integer.compare(size[y], size[x]));
		final int[] copy = size.clone();
		for (int i = size.length; i-- != 0;) size[i] = copy[perm[i]];
		Util.invertPermutationInPlace(perm);
		for(int i = component.length; i-- != 0;) component[i] = perm[component[i]];
	}
}
