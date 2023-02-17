/*
 * Copyright (C) 2003-2023 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
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
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableSequentialGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.Label;
import it.unimi.dsi.webgraph.labelling.LabelMergeStrategy;
import it.unimi.dsi.webgraph.labelling.LabelSemiring;
import it.unimi.dsi.webgraph.labelling.Labels;
import it.unimi.dsi.webgraph.labelling.UnionArcLabelledImmutableGraph;

/** Static methods that manipulate immutable graphs.
 *
 *  <P>Most methods take an {@link
 *  it.unimi.dsi.webgraph.ImmutableGraph} (along with some other data, that
 *  depend on the kind of transformation), and return another {@link
 *  it.unimi.dsi.webgraph.ImmutableGraph} that represents the transformed
 *  version.
 */

public class Transform {

	private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);

	private static final boolean DEBUG = false;
	private static final boolean ASSERTS = false;

	private Transform() {}

	/** Provides a method to accept or reject an arc.
	 *
	 * <P>Note that arc filters are usually stateless. Thus, their declaration
	 * should comprise a static singleton (e.g., {@link Transform#NO_LOOPS}).
	 */
	public interface ArcFilter {

		/**
		 * Tells if the arc <code>(i,j)</code> has to be accepted or not.
		 *
		 * @param i the source of the arc.
		 * @param j the destination of the arc.
		 * @return if the arc has to be accepted.
		 */
		public boolean accept(int i, int j);
	}

	/** Provides a method to accept or reject a labelled arc.
	 *
	 * <P>Note that arc filters are usually stateless. Thus, their declaration
	 * should comprise a static singleton (e.g., {@link Transform#NO_LOOPS}).
	 */
	public interface LabelledArcFilter {

		/**
		 * Tells if the arc <code>(i,j)</code> with label <code>label</code> has to be accepted or not.
		 *
		 * @param i the source of the arc.
		 * @param j the destination of the arc.
		 * @param label the label of the arc.
		 * @return if the arc has to be accepted.
		 */
		public boolean accept(int i, int j, Label label);
	}

	/** An arc filter that rejects loops. */
	final static private class NoLoops implements ArcFilter, LabelledArcFilter {
		private NoLoops() {}
		/** Returns true if the two arguments differ.
		 *
		 * @return <code>i != j</code>.
		 */
		@Override
		public boolean accept(final int i, final int j) {
			return i != j;
		}
		@Override
		public boolean accept(final int i, final int j, final Label label) {
			return i != j;
		}
	}

	/** An arc filter that only accepts arcs whose endpoints belong to the same
	 * (if the parameter <code>keepOnlySame</code> is true) or to
	 *  different (if <code>keepOnlySame</code> is false) classes.
	 *  Classes are specified by one integer per node, read from a given file in {@link DataInput} format. */
	public final static class NodeClassFilter implements ArcFilter, LabelledArcFilter {
		private final boolean keepOnlySame;
		private final int[] nodeClass;

		/** Creates a new instance.
		 *
		 * @param classFile name of the class file.
		 * @param keepOnlySame whether to keep nodes in the same class.
		 */
		public NodeClassFilter(final String classFile, final boolean keepOnlySame) {
			try {
				nodeClass = BinIO.loadInts(classFile);
			}
			catch (final IOException e) {
				throw new RuntimeException(e);
			}
			this.keepOnlySame = keepOnlySame;
		}

		/** Creates a new instance.
		 *
		 * <p>This constructor has the same arguments as {@link it.unimi.dsi.webgraph.Transform.NodeClassFilter#NodeClassFilter(String, boolean)},
		 * but it can be used with an {@link ObjectParser}.
		 *
		 * @param classFile name of the class file.
		 * @param keepOnlySame whether to keep nodes in the same class.
		 */
		public NodeClassFilter(final String classFile, final String keepOnlySame) {
			this(classFile, Boolean.parseBoolean(keepOnlySame));
		}

		@Override
		public boolean accept(final int i, final int j) {
			return keepOnlySame == (nodeClass[i] == nodeClass[j]);
		}

		@Override
		public boolean accept(final int i, final int j, final Label label) {
			return keepOnlySame == (nodeClass[i] == nodeClass[j]);
		}
	}

	/** An arc filter that rejects arcs whose well-known attribute has a value smaller than a given threshold. */
	final static public class LowerBound implements LabelledArcFilter {
		private final int lowerBound;

		public LowerBound(final int lowerBound) {
			this.lowerBound = lowerBound;
		}

		public LowerBound(final String lowerBound) {
			this(Integer.parseInt(lowerBound));
		}
		/** Returns true if the integer value associated to the well-known attribute of the label is larger than the threshold.
		 *
		 * @return true if <code>label.{@link Label#getInt()}</code> is larger than the threshold.
		 */
		@Override
		public boolean accept(final int i, final int j, final Label label) {
			return label.getInt() >= lowerBound;
		}
	}


	/** A singleton providing an arc filter that rejects loops. */
	final static public NoLoops NO_LOOPS = new NoLoops();

	/** A class that exposes an immutable graph viewed through a filter. */
	private static final class FilteredImmutableGraph extends ImmutableGraph {
		private final class FilteredImmutableGraphNodeIterator extends NodeIterator {
			private final NodeIterator nodeIterator;
			private final int nextNode;
			private int outdegree;
			private int[] succ;

			public FilteredImmutableGraphNodeIterator(final NodeIterator nodeIterator) {
				this(nodeIterator, 0, -1, IntArrays.EMPTY_ARRAY);
			}

			public FilteredImmutableGraphNodeIterator(final NodeIterator nodeIterator, final int nextNode, final int outdegree, final int[] succ) {
				this.nodeIterator = nodeIterator;
				this.nextNode = nextNode;
				this.outdegree = outdegree;
				this.succ = succ;
			}

			@Override
			public int outdegree() {
				if (outdegree == -1) throw new IllegalStateException();
				return outdegree;
			}

			@Override
			public int nextInt() {
				final int currNode = nodeIterator.nextInt();
				final int oldOutdegree = nodeIterator.outdegree();
				final int[] oldSucc = nodeIterator.successorArray();
				succ = IntArrays.ensureCapacity(succ, oldOutdegree, 0);
				outdegree = 0;
				for(int i = 0; i < oldOutdegree; i++) if (filter.accept(currNode, oldSucc[i])) succ[outdegree++] = oldSucc[i];
				return currNode;
			}

			@Override
			public int[] successorArray() {
				if (outdegree == -1) throw new IllegalStateException();
				return succ;
			}

			@Override
			public boolean hasNext() {
				return nodeIterator.hasNext();
			}

			@Override
			public NodeIterator copy(final int upperBound) {
				return new FilteredImmutableGraphNodeIterator(nodeIterator.copy(upperBound), nextNode, outdegree, Arrays.copyOf(succ, Math.max(0, outdegree)));
			}
		}

		private final ArcFilter filter;
		private final ImmutableGraph graph;
		private int succ[];
		private int cachedNode = -1;

		private FilteredImmutableGraph(final ArcFilter filter, final ImmutableGraph graph) {
			this.filter = filter;
			this.graph = graph;
		}

		@Override
		public int numNodes() {
			return graph.numNodes();
		}

		@Override
		public FilteredImmutableGraph copy() {
			return new FilteredImmutableGraph(filter, graph.copy());
		}

		@Override
		public boolean randomAccess() {
			return graph.randomAccess();
		}

		@Override
		public boolean hasCopiableIterators() {
			return graph.hasCopiableIterators();
		}

		@Override
		public LazyIntIterator successors(final int x) {
			return new AbstractLazyIntIterator() {

				private final LazyIntIterator s = graph.successors(x);

				@Override
				public int nextInt() {
					int t;
					while ((t = s.nextInt()) != -1) if (filter.accept(x, t)) return t;
					return -1;
				}
			};
		}

		private void fillCache(final int x) {
			if (x == cachedNode) return;
			succ = LazyIntIterators.unwrap(successors(x));
			cachedNode = x;
		}

		@Override
		public int[] successorArray(final int x) {
			fillCache(x);
			return succ;
		}

		@Override
		public int outdegree(final int x) {
			fillCache(x);
			return succ.length;
		}

		@Override
		public NodeIterator nodeIterator() {
			return new FilteredImmutableGraphNodeIterator(graph.nodeIterator());
		}

		@Override
		public NodeIterator nodeIterator(final int from) {
			return new FilteredImmutableGraphNodeIterator(graph.nodeIterator(from), from, -1, IntArrays.EMPTY_ARRAY);
		}

	}

	/** A class that exposes an arc-labelled immutable graph viewed through a filter. */
	private static final class FilteredArcLabelledImmutableGraph extends ArcLabelledImmutableGraph {
		private final LabelledArcFilter filter;
		private final ArcLabelledImmutableGraph graph;
		private int succ[];
		private Label label[];
		private int cachedNode = -1;

		@Override
		public boolean hasCopiableIterators() {
			return graph.hasCopiableIterators();
		}

		private final class FilteredArcLabelledNodeIterator extends ArcLabelledNodeIterator {
			private final ArcLabelledNodeIterator nodeIterator;
			private final int upperBound;
			private int currNode;
			private int outdegree;

			public FilteredArcLabelledNodeIterator(final int upperBound) {
				this(upperBound, graph.nodeIterator(), -1, -1);
			}

			public FilteredArcLabelledNodeIterator(final int upperBound, final ArcLabelledNodeIterator nodeIterator, final int currNode, final int outdegree) {
				this.upperBound = upperBound;
				this.nodeIterator = nodeIterator;
				this.currNode = currNode;
				this.outdegree = outdegree;
			}

			@Override
			public int outdegree() {
				if (currNode == -1) throw new IllegalStateException();
				if (outdegree == -1) {
					int d = 0;
					final LabelledArcIterator successors = successors();
					while(successors.nextInt() != -1) d++;
					outdegree = d;
				}
				return outdegree;
			}

			@Override
			public int nextInt() {
				outdegree = -1;
				return currNode = nodeIterator.nextInt();
			}

			@Override
			public boolean hasNext() {
				return currNode + 1 < upperBound && nodeIterator.hasNext();
			}

			@Override
			public LabelledArcIterator successors() {
				return new FilteredLabelledArcIterator(currNode, nodeIterator.successors());
			}

			@Override
			public ArcLabelledNodeIterator copy(final int upperBound) {
				return new FilteredArcLabelledNodeIterator(upperBound, nodeIterator.copy(upperBound), currNode, outdegree);
			}
		}

		private final class FilteredLabelledArcIterator extends AbstractLazyIntIterator implements LabelledArcIterator {
			private final int x;

			private final LabelledArcIterator successors;

			private FilteredLabelledArcIterator(final int x, final LabelledArcIterator successors) {
				this.x = x;
				this.successors = successors;
			}

			@Override
			public int nextInt() {
				int t;
				while ((t = successors.nextInt()) != -1) if (filter.accept(x, t, successors.label())) return t;
				return -1;
			}

			@Override
			public Label label() {
				return successors.label();
			}
		}

		private FilteredArcLabelledImmutableGraph(final LabelledArcFilter filter, final ArcLabelledImmutableGraph graph) {
			this.filter = filter;
			this.graph = graph;
		}

		@Override
		public int numNodes() {
			return graph.numNodes();
		}

		@Override
		public ArcLabelledImmutableGraph copy() {
			return new FilteredArcLabelledImmutableGraph(filter, graph.copy());
		}

		@Override
		public boolean randomAccess() {
			return graph.randomAccess();
		}

		@Override
		public Label prototype() {
			return graph.prototype();
		}

		private void fillCache(final int x) {
			if (x == cachedNode) return;
			cachedNode = x;
			succ = LazyIntIterators.unwrap(successors(x));
			label = super.labelArray(x);
		}

		@Override
		public LabelledArcIterator successors(final int x) {
			return new FilteredLabelledArcIterator(x, graph.successors(x));
		}

		@Override
		public int[] successorArray(final int x) {
			fillCache(x);
			return succ;
		}

		@Override
		public Label[] labelArray(final int x) {
			fillCache(x);
			return label;
		}

		@Override
		public int outdegree(final int x) {
			fillCache(x);
			return succ.length;
		}

		@Override
		public ArcLabelledNodeIterator nodeIterator() {
			return new FilteredArcLabelledNodeIterator(Integer.MAX_VALUE);
		}

	}

	/** Returns a graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @return the filtered graph.
	 */
	public static ImmutableGraph filterArcs(final ImmutableGraph graph, final ArcFilter filter) {
		return new FilteredImmutableGraph(filter, graph);
	}

	/** Returns a graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @param ignored a progress logger.
	 * @return the filtered graph.
	 */
	public static ImmutableGraph filterArcs(final ImmutableGraph graph, final ArcFilter filter, final ProgressLogger ignored) {
		return filterArcs(graph, filter);
	}

	/** Returns a labelled graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a labelled graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @return the filtered graph.
	 */
	public static ArcLabelledImmutableGraph filterArcs(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter) {
		return new FilteredArcLabelledImmutableGraph(filter, graph);
	}

	/** Returns a labelled graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a labelled graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @param ignored a progress logger.
	 * @return the filtered graph.
	 */
	public static ArcLabelledImmutableGraph filterArcs(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final ProgressLogger ignored) {
		return filterArcs(graph, filter);
	}

	private static final class RemappedImmutableGraph extends ImmutableGraph {
		private final int[] map;
		private final ImmutableGraph g;
		private final boolean isInjective;
		private final boolean isPermutation;
		private final int remappedNodes;
		private final int destNumNodes;
		private final int[] pseudoInverse;
		private int[] succ;
		private int outdegree;
		private int currentNode = -1;

		private RemappedImmutableGraph(final int[] map, final ImmutableGraph g, final boolean isInjective, final boolean isPermutation, final int remappedNodes, final int destNumNodes, final int[] pseudoInverse) {
			this.map = map;
			this.g = g;
			this.isInjective = isInjective;
			this.isPermutation = isPermutation;
			this.remappedNodes = remappedNodes;
			this.destNumNodes = destNumNodes;
			this.pseudoInverse = pseudoInverse;
		}

		@Override
		public RemappedImmutableGraph copy() {
			return new RemappedImmutableGraph(map, g.copy(), isInjective, isPermutation, remappedNodes, destNumNodes, pseudoInverse);
		}

		@Override
		public int numNodes() {
			return destNumNodes;
		}

		@Override
		public boolean randomAccess() {
			return true;
		}

		@Override
		public boolean hasCopiableIterators() {
			return true;
		}

		@Override
		public int[] successorArray(final int x) {
			if (currentNode != x) {
				final IntSet succSet = new IntOpenHashSet();
				succSet.clear();

				if (isPermutation) {
					final LazyIntIterator i = g.successors(pseudoInverse[x]);
					for(int d = g.outdegree(pseudoInverse[x]); d-- != 0;) succSet.add(map[i.nextInt()]);
				}
				else {
					int low = 0, high = remappedNodes - 1, mid = 0;
					while (low <= high) {
						mid = (low + high) >>> 1;
						final int midVal = map[pseudoInverse[mid]];
						if (midVal < x)low = mid + 1;
						else if (midVal > x) high = mid - 1;
						else break;
					}
					int t, p;
					if (isInjective) {
						if (map[p = pseudoInverse[mid]] == x) {
							final LazyIntIterator i = g.successors(p);
							for(int d = g.outdegree(p); d-- != 0;) if ((t = map[i.nextInt()]) != -1) succSet.add(t);
						}
					}
					else {
						while (mid > 0 && map[pseudoInverse[mid - 1]] == x) mid--;
						while (mid < remappedNodes && map[p = pseudoInverse[mid]] == x) {
							final LazyIntIterator i = g.successors(p);
							for(int d = g.outdegree(p); d-- != 0;) if ((t = map[i.nextInt()]) != -1) succSet.add(t);
							mid++;
						}
					}
				}
				outdegree = succSet.size();
				currentNode = x;
				succ = succSet.toIntArray();
				if (outdegree > 0) IntArrays.quickSort(succ, 0, outdegree);
			}
			return succ;
		}

		@Override
		public int outdegree(final int x) {
			if (currentNode != x) successorArray(x);
			return outdegree;
		}
	}

	/** Remaps the graph nodes through a partial function specified via
	 *  an array. More specifically, <code>map.length=g.numNodes()</code>,
	 *  and <code>map[i]</code> is the new name of node <code>i</code>, or -1 if the node
	 *  should not be mapped. If some
	 *  index appearing in <code>map</code> is larger than or equal to the
	 *  number of nodes of <code>g</code>, the resulting graph is enlarged correspondingly.
	 *
	 *  <P>Arcs are mapped in the obvious way; in other words, there is
	 *  an arc from <code>map[i]</code> to <code>map[j]</code> (both nonnegative)
	 *  in the transformed
	 *  graph iff there was an arc from <code>i</code> to <code>j</code>
	 *  in the original graph.
	 *
	 *  <P>Note that if <code>map</code> is bijective, the returned graph
	 *  is simply a permutation of the original graph.
	 *  Otherwise, the returned graph is obtained by deleting nodes mapped
	 *  to -1, quotienting nodes w.r.t. the equivalence relation induced by the fibres of <code>map</code>
	 *  and renumbering the result, always according to <code>map</code>.
	 *
	 * <P>This method <strong>requires</strong> {@linkplain ImmutableGraph#randomAccess()} random access.
	 *
	 * @param g the graph to be transformed.
	 * @param map the transformation map.
	 * @param pl a progress logger to be used during the precomputation, or <code>null</code>.
	 * @return the transformed graph (provides {@linkplain ImmutableGraph#randomAccess() random access}.
	 */
	public static ImmutableGraph map(final ImmutableGraph g, final int map[], final ProgressLogger pl) {
		int i, j;
		if (! g.randomAccess()) throw new IllegalArgumentException("Graph mapping requires random access");

		final int sourceNumNodes = g.numNodes();
		if (map.length != sourceNumNodes) throw new IllegalArgumentException("The graph to be mapped has " + sourceNumNodes + " whereas the map contains " + map.length + " entries");

		int max = -1;
		if (pl != null) {
			pl.itemsName = "nodes";
			pl.start("Storing identity...");
		}

		// Compute the number of actually remapped nodes (those with f[] != -1)
		for (i = j = 0; i < sourceNumNodes; i++) if (map[i] >= 0) j++;
		final int remappedNodes = j;
		final boolean everywhereDefined = remappedNodes == sourceNumNodes;

		/* The pseudoinverse array: for each node of the transformed graph that is image of a node
		 * of the source graph, it contains the index of that node. */
		final int pseudoInverse[] = new int[remappedNodes];

		for (i = j = 0; i < sourceNumNodes; i++) {
			if (max < map[i]) max = map[i];
			//if (f[i] < 0) throw new IllegalArgumentException("The supplied map contains a negative value (" + f[i] +") at index " + i);
			if (map[i] >= 0) pseudoInverse[j++] = i;
		}

		final int destNumNodes = max + 1;
		final boolean notEnlarged = destNumNodes <= sourceNumNodes;

		if (pl != null) {
			pl.count = remappedNodes;
			pl.done();
		}

		// sort sf[]
		if (pl != null) pl.start("Sorting to obtain pseudoinverse...");
		IntArrays.radixSortIndirect(pseudoInverse, map, 0, remappedNodes, false);
		if (pl != null) {
			pl.count = sourceNumNodes;
			pl.done();
		}

		// check if f is injective
		if (pl != null) pl.start("Checking whether it is injective...");
		int k = remappedNodes - 1;
		// Note that we need the first check for the empty graph.
		if (k >= 0) while(k-- != 0) if (map[pseudoInverse[k]] == map[pseudoInverse[k + 1]]) break;
		final boolean isInjective = k == -1;
		if (pl != null) {
			pl.count = sourceNumNodes;
			pl.stop("(It is" + (isInjective ? "" : " not") + " injective.)");
			pl.done();
		}

		final boolean isPermutation = isInjective && everywhereDefined && notEnlarged;

		return new RemappedImmutableGraph(map, g, isInjective, isPermutation, remappedNodes, destNumNodes, pseudoInverse);
	}

	/** Remaps the graph nodes through a function specified via
	 *  an array.
	 *
	 * @param g the graph to be transformed.
	 * @param f the transformation map.
	 * @return the transformed graph.
	 * @see #map(ImmutableGraph, int[], ProgressLogger)
	 */
	public static ImmutableGraph map(final ImmutableGraph g, final int f[]) {
		return map(g, f, null);
	}

	/** Returns a symmetrized graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return the symmetrized graph.
	 * @see #symmetrizeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize) throws IOException {
		return symmetrizeOffline(g, batchSize, null, null);
	}

	/**
	 * Returns a symmetrized graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return the symmetrized graph.
	 * @see #symmetrizeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return symmetrizeOffline(g, batchSize, tempDir, null);
	}

	/**
	 * Returns a symmetrized graph using an offline transposition.
	 *
	 * <P>
	 * The symmetrized graph is the union of a graph and of its transpose. This method will compute the
	 * transpose on the fly using {@link #transposeOffline(ImmutableGraph, int, File, ProgressLogger)}.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the symmetrized graph.
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		return union(g, transposeOffline(g, batchSize, tempDir, pl));
	}

	/**
	 * Returns an arc-labelled symmetrized graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc is present in both
	 *            {@code g} and its transpose; if <code>null</code>,
	 *            {@link Labels#KEEP_FIRST_MERGE_STRATEGY} is used.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @return an immutable, sequentially accessible arc-labelled graph obtained by symmetrizing
	 *         {@code g}.
	 * @see #symmetrizeOffline(ArcLabelledImmutableGraph, LabelMergeStrategy, int, File)
	 */
	public static ArcLabelledImmutableGraph symmetrizeOffline(final ArcLabelledImmutableGraph g, final LabelMergeStrategy labelMergeStrategy, final int batchSize) throws IOException {
		return symmetrizeOffline(g, labelMergeStrategy, batchSize, null, null);
	}

	/**
	 * Returns an arc-labelled symmetrized graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc is present in both
	 *            {@code g} and its transpose; if <code>null</code>,
	 *            {@link Labels#KEEP_FIRST_MERGE_STRATEGY} is used.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible arc-labelled graph obtained by symmetrizing
	 *         {@code g}.
	 * @see #symmetrizeOffline(ArcLabelledImmutableGraph, LabelMergeStrategy, int, File, ProgressLogger)
	 */
	public static ArcLabelledImmutableGraph symmetrizeOffline(final ArcLabelledImmutableGraph g, final LabelMergeStrategy labelMergeStrategy, final int batchSize, final File tempDir) throws IOException {
		return symmetrizeOffline(g, labelMergeStrategy, batchSize, tempDir, null);
	}

	/**
	 * Returns an arc-labelled symmetrized graph using an offline transposition.
	 *
	 * <P>
	 * The symmetrized graph is the union of a graph and of its transpose. This method will compute the
	 * transpose on the fly using
	 * {@link #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)}.
	 *
	 * @param g the source graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc is present in both
	 *            {@code g} and its transpose; if <code>null</code>,
	 *            {@link Labels#KEEP_FIRST_MERGE_STRATEGY} is used.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable, sequentially accessible arc-labelled graph obtained by symmetrizing
	 *         {@code g}.
	 */
	public static ArcLabelledImmutableGraph symmetrizeOffline(final ArcLabelledImmutableGraph g, final LabelMergeStrategy labelMergeStrategy, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		return union(g, transposeOffline(g, batchSize, tempDir, pl), labelMergeStrategy);
	}

	/**
	 * Returns a simplified (loopless and symmetric) graph using the graph and its transpose.
	 *
	 * @param g the source graph.
	 * @param t the graph <code>g</code> transposed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the simplified (loopless and symmetric) graph.
	 */
	public static ImmutableGraph simplify(final ImmutableGraph g, final ImmutableGraph t, final ProgressLogger pl) {
		return filterArcs(union(g, t), NO_LOOPS, pl);
	}

	/**
	 * Returns a simplified (loopless and symmetric) graph using the graph and its transpose.
	 *
	 * @param g the source graph.
	 * @param t the graph <code>g</code> transposed.
	 * @return the simplified (loopless and symmetric) graph.
	 */
	public static ImmutableGraph simplify(final ImmutableGraph g, final ImmutableGraph t) {
		return filterArcs(union(g, t), NO_LOOPS, null);
	}

	/**
	 * Returns a simplified (loopless and symmetric) graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @return the simplified (loopless and symmetric) graph.
	 * @see #simplifyOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph simplifyOffline(final ImmutableGraph g, final int batchSize) throws IOException {
		return simplifyOffline(g, batchSize, null, null);
	}

	/**
	 * Returns a simplified (loopless and symmetric) graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return the simplified (loopless and symmetric) graph.
	 * @see #simplifyOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph simplifyOffline(final ImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return simplifyOffline(g, batchSize, tempDir, null);
	}

	/**
	 * Returns a simplified graph(loopless and symmetric) using an offline transposition.
	 *
	 * <P>
	 * The simplified graph is the union of a graph and of its transpose, with the loops removed. This
	 * method will compute the transpose on the fly using
	 * {@link #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)}.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the simplified (loopless and symmetric) graph.
	 */
	public static ImmutableGraph simplifyOffline(final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		return filterArcs(symmetrizeOffline(g, batchSize, tempDir, pl), NO_LOOPS);
	}

	/** Returns a symmetrized graph.
	 *
	 * <P>The symmetrized graph is the union of a graph and of its transpose. This method will
	 * use the provided transposed graph, if any, instead of computing it on the fly.
	 *
	 * @param g the source graph.
	 * @param t the graph <code>g</code> transposed; if <code>null</code>, the transposed graph will be computed on the fly.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the symmetrized graph.
	 */
	public static ImmutableGraph symmetrize(final ImmutableGraph g, final ImmutableGraph t, final ProgressLogger pl) {
		return t == null ?
			union(g, transpose(g, pl)) :
			union(g, t);
	}

	/** Returns a symmetrized graph.
	 *
	 * <P>The symmetrized graph is the union of a graph and of its transpose. This method will
	 * use the provided transposed graph, if any, instead of computing it on the fly.
	 *
	 * @param g the source graph.
	 * @param t the graph <code>g</code> transposed; if <code>null</code>, the transposed graph will be computed on the fly.
	 * @return the symmetrized graph.
	 */
	public static ImmutableGraph symmetrize(final ImmutableGraph g, final ImmutableGraph t) {
		return symmetrize(g, t, null);
	}

	/**
	 * Returns a symmetrized graph.
	 *
	 * @param g the source graph.
	 * @param pl a progress logger.
	 * @return the symmetrized graph.
	 * @see #symmetrize(ImmutableGraph, ImmutableGraph, ProgressLogger)
	 */
	public static ImmutableGraph symmetrize(final ImmutableGraph g, final ProgressLogger pl) {
		return symmetrize(g, null, pl);
	}

	/**
	 * Returns a symmetrized graph.
	 *
	 * @param g the source graph.
	 * @return the symmetrized graph.
	 * @see #symmetrize(ImmutableGraph, ImmutableGraph, ProgressLogger)
	 */
	public static ImmutableGraph symmetrize(final ImmutableGraph g) {
		return symmetrize(g, null, null);
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>.
	 *
	 * <P>This method can process {@linkplain ImmutableGraph#loadOffline(CharSequence) offline graphs}).
	 *
	 * @param g an immutable graph.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable graph obtained by transposing <code>g</code>.
	 */

	public static ImmutableGraph transpose(final ImmutableGraph g, final ProgressLogger pl) {

		int i, j, d, a[];

		final int n = g.numNodes();
		final int numPred[] = new int[n];

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start("Counting predecessors...");
		}

		NodeIterator nodeIterator = g.nodeIterator();

		long m = 0; // Number of arcs, computed on the fly.

		for(i = n; i-- != 0;) {
			nodeIterator.nextInt();
			d = nodeIterator.outdegree();
			a = nodeIterator.successorArray();
			m += d;
			while(d-- != 0) numPred[a[d]]++;
			if (pl != null) pl.lightUpdate();
		}

		if (pl != null) pl.done();

		final int pred[][] = new int[n][];

		if (pl != null) {
			pl.expectedUpdates = n;
			pl.start("Allocating memory for predecessors...");
		}

		for(i = n; i-- != 0;) {
			if (numPred[i] != 0) pred[i] = new int[numPred[i]];
			if (pl != null) pl.lightUpdate();
		}

		if (pl != null) pl.done();

		Arrays.fill(numPred, 0);

		if (pl != null) {
			pl.expectedUpdates = n;
			pl.start("Computing predecessors...");
		}

		nodeIterator = g.nodeIterator();

		for(i = n; i-- != 0;) {
			j = nodeIterator.nextInt();
			d = nodeIterator.outdegree();
			a = nodeIterator.successorArray();
			while(d-- != 0) pred[a[d]][numPred[a[d]]++] = j;
			if (pl != null) pl.update();
		}

		if (pl != null) pl.done();

		if (pl != null) {
			pl.expectedUpdates = n;
			pl.start("Sorting predecessors...");
		}

		for(i = n; i-- != 0;) {
			if (pred[i] != null) Arrays.sort(pred[i]);
			if (pl != null) pl.lightUpdate();
		}

		if (pl != null) pl.done();

		final long numArcs = m;
		return new ImmutableGraph() {
			@Override
			public int numNodes() { return n; }
			@Override
			public long numArcs() { return numArcs; }
			@Override
			public ImmutableGraph copy() { return this; }
			@Override
			public boolean randomAccess() { return true; }
			@Override
			public int[] successorArray(final int x) { return pred[x] != null ? pred[x] : IntArrays.EMPTY_ARRAY; }
			@Override
			public int outdegree(final int x) { return successorArray(x).length; }
		};
	}



	/* Provides a sequential immutable graph by merging batches on the fly. */
	public final static class BatchGraph extends ImmutableSequentialGraph {
		private final static class BatchGraphNodeIterator extends NodeIterator {
			/** The buffer size. We can't make it too big&mdash;there's one per batch, per thread. */
			private static final int STD_BUFFER_SIZE = 128 * 1024;
			/** The indirect queue used to merge the batches. */
			private final IntHeapSemiIndirectPriorityQueue queue;
			/** The reference array for {@link #queue}. */
			private final int[] refArray;
			/** The input bit streams over the batches. */
			private final InputBitStream[] batchIbs;
			/** The number of elements in each each {@linkplain #batchIbs batch}. */
			private final int[] inputStreamLength;
			/** The limit for {@link #hasNext()}. */
			private final int hasNextLimit;
			/** The target of the lastly returned arcs */
			private final int[] prevTarget;
			/** The last returned node (-1 if no node has been returned yet). */
			private int last;
			/** The outdegree of the current node (valid if not -1). */
			private int outdegree;
			/** The number of pairs associated with the current node (valid if {@link #last} is not -1). */
			private int numPairs;
			/** The successors of the current node (valid if {@link #last} is not -1);
			 * only the first {@link #outdegree} entries are meaningful. */
			private int[] successor;
			/** The batches underlying this iterator. */
			private final ObjectArrayList<File> batches;
			/** The number of nodes in the graph. */
			private final int n;

			private BatchGraphNodeIterator(final int n, final ObjectArrayList<File> batches, final int upperBound) throws IOException {
				this(n, batches, upperBound, null, null, null, null, -1, -1, IntArrays.EMPTY_ARRAY);
			}

			private BatchGraphNodeIterator(final int n, final ObjectArrayList<File> batches, final int upperBound, final InputBitStream[] baseIbs, final int[] refArray, final int[] prevTarget, final int[] inputStreamLength, final int last, final int outdegree, final int successor[]) throws IOException {
				this.n = n;
				this.batches = batches;
				this.hasNextLimit = Math.min(n, upperBound) - 1;
				this.last = last;
				this.outdegree = outdegree;
				this.successor = successor;
				batchIbs = new InputBitStream[batches.size()];

				if (refArray == null) {
					this.refArray = new int[batches.size()];
					this.prevTarget = new int[batches.size()];
					this.inputStreamLength = new int[batches.size()];
					Arrays.fill(this.prevTarget, -1);
					queue = new IntHeapSemiIndirectPriorityQueue(this.refArray);
					// We open all files and load the first element into the reference array.
					for(int i = 0; i < batches.size(); i++) {
						batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
						this.inputStreamLength[i] = batchIbs[i].readDelta();
						this.refArray[i] = batchIbs[i].readDelta();
						queue.enqueue(i);
					}
				}
				else {
					this.refArray = refArray;
					this.prevTarget = prevTarget;
					this.inputStreamLength = inputStreamLength;
					queue = new IntHeapSemiIndirectPriorityQueue(refArray);

					for(int i = 0; i < refArray.length; i++) {
						if (baseIbs[i] != null) {
							batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
							batchIbs[i].position(baseIbs[i].position());
							queue.enqueue(i);
						}
					}
				}
			}

			@Override
			public NodeIterator copy(final int upperBound) {
				try {
					if (last == -1) return new BatchGraphNodeIterator(n, batches, upperBound);
					else return new BatchGraphNodeIterator(n, batches, upperBound, batchIbs, refArray.clone(), prevTarget.clone(), inputStreamLength.clone(), last, outdegree(), Arrays.copyOf(successor, outdegree()));
				}
				catch (final IOException e) {
					throw new RuntimeException(e.getMessage(), e);
				}
			}

			@Override
			public int outdegree() {
				if (last == -1) throw new IllegalStateException();
				if (outdegree == -1) successorArray();
				return outdegree;
			}

			@Override
			public boolean hasNext() {
				return last < hasNextLimit;
			}

			@Override
			public int nextInt() {
				if (! hasNext()) throw new NoSuchElementException();
				last++;
				int d = 0;
				outdegree = -1;
				int i;

				try {
					/* We extract elements from the queue as long as their target is equal
					 * to last. If during the process we exhaust a batch, we close it. */

					while(! queue.isEmpty() && refArray[i = queue.first()] == last) {
						successor = IntArrays.grow(successor, d + 1);
						successor[d] = (prevTarget[i] += batchIbs[i].readDelta() + 1);
						if (--inputStreamLength[i] == 0) {
							queue.dequeue();
							batchIbs[i].close();
							batchIbs[i] = null;
						}
						else {
							// We read a new source and update the queue.
							final int sourceDelta = batchIbs[i].readDelta();
							if (sourceDelta != 0) {
								refArray[i] += sourceDelta;
								prevTarget[i] = -1;
								queue.changed();
							}
						}
						d++;
					}

					numPairs = d;
				}
				catch(final IOException e) {
					e.printStackTrace();
					throw new RuntimeException(this + " " + e);
				}

				return last;
			}

			@Override
			public int[] successorArray() {
				if (last == -1) throw new IllegalStateException();
				if (outdegree == -1) {
					final int numPairs = this.numPairs;
					// Neither quicksort nor heaps are stable, so we reestablish order here.
					IntArrays.quickSort(successor, 0, numPairs);
					if (numPairs!= 0) {
						int p = 0;
						for (int j = 1; j < numPairs; j++) if (successor[p] != successor[j]) successor[++p] = successor[j];
						outdegree = p + 1;
					}
					else outdegree = 0;
				}
				return successor;
			}

			@SuppressWarnings("deprecation")
			@Override
			protected void finalize() throws Throwable {
				try {
					for(final InputBitStream ibs: batchIbs) if (ibs != null) ibs.close();
				}
				finally {
					super.finalize();
				}
			}

		}

		private final ObjectArrayList<File> batches;
		private final int n;
		private final long numArcs;

		public BatchGraph(final int n, final long m, final ObjectArrayList<File> batches) {
			this.batches = batches;
			this.n = n;
			this.numArcs = m;
		}

		@Override
		public int numNodes() { return n; }
		@Override
		public long numArcs() { return numArcs; }

		@Override
		public boolean hasCopiableIterators() {
			return true;
		}

		@Override
		public BatchGraph copy() {
			return this;
		}

		@Override
		public NodeIterator nodeIterator() {
			try {
				return new BatchGraphNodeIterator(n, batches, n);
			}
			catch(final IOException e) {
				throw new RuntimeException(e);
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		protected void finalize() throws Throwable {
			try {
				for(final File f : batches) f.delete();
			}
			finally {
				super.finalize();
			}
		}

	}


	/** Sorts the given source and target arrays w.r.t. the target and stores them in a temporary file.
	 *
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param tempDir a temporary directory where to store the sorted arrays, or <code>null</code>
	 * @param batches a list of files to which the batch file will be added.
	 * @return the number of pairs in the batch (might be less than <code>n</code> because duplicates are eliminated).
	 */

	public static int processBatch(final int n, final int[] source, final int[] target, final File tempDir, final List<File> batches) throws IOException {

		IntArrays.parallelQuickSort(source, target, 0, n);

		final File batchFile = File.createTempFile("batch", ".bitstream", tempDir);
		batchFile.deleteOnExit();
		batches.add(batchFile);
		final OutputBitStream batch = new OutputBitStream(batchFile);
		int u = 0;
		if (n != 0) {
			// Compute unique pairs
			u = 1;
			for(int i = n - 1; i-- != 0;) if (source[i] != source[i + 1] || target[i] != target[i + 1]) u++;
			batch.writeDelta(u);
			int prevSource = source[0];
			batch.writeDelta(prevSource);
			batch.writeDelta(target[0]);

			for(int i = 1; i < n; i++) {
				if (source[i] != prevSource) {
					batch.writeDelta(source[i] - prevSource);
					batch.writeDelta(target[i]);
					prevSource = source[i];
				}
				else if (target[i] != target[i - 1]) {
					// We don't write duplicate pairs
					batch.writeDelta(0);
					assert target[i] > target[i - 1] : target[i] + "<=" + target[i - 1];
					batch.writeDelta(target[i] - target[i - 1] - 1);
				}
			}
		}
		else batch.writeDelta(0);

		batch.close();
		return u;
	}

	/** Sorts the given source and target arrays w.r.t. the target and stores them in two temporary files.
	 *  An additional positionable input bit stream is provided that contains labels, starting at given positions.
	 *  Labels are also written onto the appropriate file.
	 *
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param start the array containing the bit position (within the given input stream) where the label of the arc starts.
	 * @param labelBitStream the positionable bit stream containing the labels.
	 * @param tempDir a temporary directory where to store the sorted arrays.
	 * @param batches a list of files to which the batch file will be added.
	 * @param labelBatches a list of files to which the label batch file will be added.
	 */

	private static void processTransposeBatch(final int n, final int[] source, final int[] target, final long[] start,
			final InputBitStream labelBitStream, final File tempDir, final List<File> batches, final List<File> labelBatches,
			final Label prototype) throws IOException {
		it.unimi.dsi.fastutil.Arrays.parallelQuickSort(0, n, (x,y) -> {
			final int t = Integer.compare(source[x], source[y]);
			if (t != 0) return t;
			return Integer.compare(target[x], target[y]);
			},
		(x, y) -> {
			int t = source[x];
			source[x] = source[y];
			source[y] = t;
			t = target[x];
			target[x] = target[y];
			target[y] = t;
			final long u = start[x];
			start[x] = start[y];
			start[y] = u;
		});

		final File batchFile = File.createTempFile("batch", ".bitstream", tempDir);
		batchFile.deleteOnExit();
		batches.add(batchFile);
		final OutputBitStream batch = new OutputBitStream(batchFile);

		if (n != 0) {
			// Compute unique pairs
			batch.writeDelta(n);
			int prevSource = source[0];
			batch.writeDelta(prevSource);
			batch.writeDelta(target[0]);

			for(int i = 1; i < n; i++) {
				if (source[i] != prevSource) {
					batch.writeDelta(source[i] - prevSource);
					batch.writeDelta(target[i]);
					prevSource = source[i];
				}
				else if (target[i] != target[i - 1]) {
					// We don't write duplicate pairs
					batch.writeDelta(0);
					batch.writeDelta(target[i] - target[i - 1] - 1);
				}
			}
		}
		else batch.writeDelta(0);

		batch.close();

		final File labelFile = File.createTempFile("label-", ".bits", tempDir);
		labelFile.deleteOnExit();
		labelBatches.add(labelFile);
		final OutputBitStream labelObs = new OutputBitStream(labelFile);
		for (int i = 0; i < n; i++) {
			labelBitStream.position(start[i]);
			prototype.fromBitStream(labelBitStream, source[i]);
			prototype.toBitStream(labelObs, target[i]);
		}
		labelObs.close();
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize) throws IOException {
		return transposeOffline(g, batchSize, null);
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return transposeOffline(g, batchSize, tempDir, null);
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * <p>This method should be used to transpose very large graph in case {@link #transpose(ImmutableGraph)}
	 * requires too much memory. It creates a number of sorted batches on disk containing arcs
	 * represented by a pair of gap-compressed integers ordered by target
	 * and returns an {@link ImmutableGraph}
	 * that can be accessed only using a {@link ImmutableGraph#nodeIterator() node iterator}. The node iterator
	 * merges on the fly the batches, providing a transposed graph. The files are marked with
	 * {@link File#deleteOnExit()}, so they should disappear when the JVM exits. An additional safety-net
	 * finaliser tries to delete the batches, too.
	 *
	 * <p>Note that each {@link NodeIterator} returned by the transpose requires opening all batches at the same time.
	 * The batches are closed when they are exhausted, so a complete scan of the graph closes them all. In any case,
	 * another safety-net finaliser closes all files when the iterator is collected.
	 *
	 * <P>This method can process {@linkplain ImmutableGraph#loadOffline(CharSequence) offline graphs}.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {

		int j, currNode;
		final int[] source = new int[batchSize], target = new int[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		final int n = g.numNodes();

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start("Creating sorted batches...");
		}

		final NodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <source,target> and dumping them on disk.
		int succ[];
		long m = 0; // Number of arcs, computed on the fly.
		j = 0;
		for(long i = n; i-- != 0;) {
			currNode = nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			succ = nodeIterator.successorArray();
			m += d;

			for(int k = 0; k < d; k++) {
				target[j] = currNode;
				source[j++] = succ[k];

				if (j == batchSize) {
					processBatch(batchSize, source, target, tempDir, batches);
					j = 0;
				}
			}


			if (pl != null) pl.lightUpdate();
		}

		if (j != 0) processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			logBatches(batches, m, pl);
		}

		return new BatchGraph(n, m, batches);
	}

	protected static void logBatches(final ObjectArrayList<File> batches, final long pairs, final ProgressLogger pl) {
		long length = 0;
		for(final File f : batches) length += f.length();
		pl.logger().info("Created " + batches.size() + " batches using " + Util.format((double)Byte.SIZE * length / pairs) + " bits/arc.");
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via an array.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 * @see #mapOffline(ImmutableGraph, int[], int, File, ProgressLogger)
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final int map[], final int batchSize) throws IOException {
		return mapOffline(g, map, batchSize, null);
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via an array.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 * @see #mapOffline(ImmutableGraph, int[], int, File, ProgressLogger)
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final int map[], final int batchSize, final File tempDir) throws IOException {
		return mapOffline(g, map, batchSize, tempDir, null);
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via an array.
	 *
	 * See {@link #map(ImmutableGraph, int[], ProgressLogger)} for the semantics of this method and {@link #transpose(ImmutableGraph, ProgressLogger)} for
	 * implementation and performance-related details.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final int map[], final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {

		int j, currNode;
		final int[] source = new int[batchSize], target = new int[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		//final int n = g.numNodes();

		int max = -1;
		for (final int x: map) if (max < x) max = x;

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = g.numNodes();
			pl.start("Creating sorted batches...");
		}

		final NodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <map[source],map[target]> (if we have to) and dumping them on disk.
		int succ[];
		j = 0;
		long pairs = 0; // Number of pairs
		for(long i = g.numNodes(); i-- != 0;) {
			currNode = nodeIterator.nextInt();
			if (map[currNode] != -1) {
				final int d = nodeIterator.outdegree();
				succ = nodeIterator.successorArray();

				for(int k = 0; k < d; k++) {
					if (map[succ[k]] != -1) {
						source[j] = map[currNode];
						target[j++] = map[succ[k]];

						if (j == batchSize) {
							pairs += processBatch(batchSize, source, target, tempDir, batches);
							j = 0;
						}
					}
				}
			}

			if (pl != null) pl.lightUpdate();
		}

		// At this point the number of nodes is always known (a traversal has been completed).
		if (g.numNodes() != map.length) throw new IllegalArgumentException("Mismatch between number of nodes (" + g.numNodes() + ") and map length (" + map.length + ")");

		if (j != 0) pairs += processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}

		return new BatchGraph(max + 1, -1, batches);
	}

	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)
	 */
	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize) throws IOException {
		return transposeOffline(g, batchSize, null);
	}

	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)
	 */
	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return transposeOffline(g, batchSize, tempDir, null);
	}


	/**
	 * Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using
	 * an offline method.
	 *
	 * <p>
	 * This method should be used to transpose very large graph in case
	 * {@link #transpose(ImmutableGraph)} requires too much memory. It creates a number of sorted
	 * batches on disk containing arcs represented by a pair of integers in {@link java.io.DataInput}
	 * format ordered by target and returns an {@link ImmutableGraph} that can be accessed only using a
	 * {@link ImmutableGraph#nodeIterator() node iterator}. The node iterator merges on the fly the
	 * batches, providing a transposed graph. The files are marked with {@link File#deleteOnExit()}, so
	 * they should disappear when the JVM exits. An additional safety-net finaliser tries to delete the
	 * batches, too.
	 *
	 * <p>
	 * As far as labels are concerned, they are temporarily stored in an in-memory bit stream, that is
	 * permuted when it is stored on disk. The bit stream is backed by a byte array, so the main limit
	 * to the batch size is that the labels associated with a batch must fit into the largest allocable
	 * byte array (usually, 2<sup>31</sup>&minus;1 bytes minus a few header bytes).
	 *
	 * <p>
	 * Note that each {@link NodeIterator} returned by the transpose requires opening all batches at the
	 * same time. The batches are closed when they are exhausted, so a complete scan of the graph closes
	 * them all. In any case, another safety-net finaliser closes all files when the iterator is
	 * collected.
	 *
	 * <P>
	 * This method can process {@linkplain ArcLabelledImmutableGraph#loadOffline(CharSequence) offline
	 * graphs}. Note that no method to transpose on-line arc-labelled graph is provided currently.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method, plus an additional {@link FastByteArrayOutputStream} needed
	 *            to store all the labels for a batch.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 */

	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {

		int i, j, d, currNode;
		final int[] source = new int[batchSize], target = new int[batchSize];
		final long[] start = new long[batchSize];
		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream(fbos);
		final ObjectArrayList<File> batches = new ObjectArrayList<>(), labelBatches = new ObjectArrayList<>();
		final Label prototype = g.prototype().copy();

		final int n = g.numNodes();

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start("Creating sorted batches...");
		}

		final ArcLabelledNodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <source,target> and dumping them on disk.
		int succ[];
		Label label[] = null;
		long m = 0; // Number of arcs, computed on the fly.
		j = 0;
		for(i = n; i-- != 0;) {
			currNode = nodeIterator.nextInt();
			d = nodeIterator.outdegree();
			succ = nodeIterator.successorArray();
			label = nodeIterator.labelArray();
			m += d;

			for(int k = 0; k < d; k++) {
				source[j] = succ[k];
				target[j] = currNode;
				start[j] = obs.writtenBits();
				label[k].toBitStream(obs, currNode);
				j++;

				if (j == batchSize) {
					obs.flush();
					processTransposeBatch(batchSize, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype);
					fbos = new FastByteArrayOutputStream();
					obs = new OutputBitStream(fbos); //ALERT here we should re-use
					j = 0;
				}
			}


			if (pl != null) pl.lightUpdate();
		}

		if (j != 0) {
			obs.flush();
			processTransposeBatch(j, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype);
		}

		if (pl != null) {
			pl.done();
			logBatches(batches, m, pl);
		}

		final long numArcs = m;

		// Now we return an immutable graph whose nodeIterator() merges the batches on the fly.
		return new ArcLabelledBatchGraph(n, numArcs, batches, labelBatches, prototype);
	}

	public static class ArcLabelledBatchGraph extends ArcLabelledImmutableSequentialGraph {
		private int n;
		private long numArcs;
		private ObjectArrayList<File> batches;
		private ObjectArrayList<File> labelBatches;
		private Label prototype;

		public ArcLabelledBatchGraph(int n, long numArcs, ObjectArrayList<File> batches, ObjectArrayList<File> labelBatches, Label prototype) {
			this.n = n;
			this.numArcs = numArcs;
			this.batches = batches;
			this.labelBatches = labelBatches;
			this.prototype = prototype;
		}

		@Override
		public int numNodes() { return n; }
		@Override
		public long numArcs() { return numArcs; }
		@Override
		public boolean hasCopiableIterators() { return true; }

		class InternalArcLabelledNodeIterator extends ArcLabelledNodeIterator {
			/** The buffer size. We can't make it too big&mdash;there's two per batch, per thread. */
			private static final int STD_BUFFER_SIZE = 64 * 1024;
			private final int[] refArray;
			private final InputBitStream[] batchIbs;
			private final InputBitStream[] labelInputBitStream;
			private final int[] inputStreamLength;
			private final int[] prevTarget;

			// The indirect queue used to merge the batches.
			private final IntHeapSemiIndirectPriorityQueue queue;
			/** The limit for {@link #hasNext()}. */
			private final int hasNextLimit;

			/** The last returned node (-1 if no node has been returned yet). */
			private int last;
			/** The outdegree of the current node (valid if {@link #last} is not -1). */
			private int outdegree;
			/** The successors of the current node (valid if {@link #last} is not -1);
			 * only the first {@link #outdegree} entries are meaningful. */
			private int[] successor;
			/** The labels of the arcs going out of the current node (valid if {@link #last} is not -1);
			 * only the first {@link #outdegree} entries are meaningful. */
			private Label[] label;

			public InternalArcLabelledNodeIterator(final int upperBound) throws IOException {
				this(upperBound, null, null, null, null, null, -1, 0, IntArrays.EMPTY_ARRAY, Label.EMPTY_LABEL_ARRAY);
			}

			public InternalArcLabelledNodeIterator(final int upperBound, final InputBitStream[] baseIbs, final InputBitStream[] baseLabelInputBitStream, final int[] refArray, final int[] prevTarget, final int[] inputStreamLength, final int last, final int outdegree, final int successor[], final Label[] label) throws IOException {
				this.hasNextLimit = Math.min(n, upperBound) - 1;
				this.last = last;
				this.outdegree = outdegree;
				this.successor = successor;
				this.label = label;
				batchIbs = new InputBitStream[batches.size()];
				labelInputBitStream = new InputBitStream[batches.size()];

				if (refArray == null) {
					this.refArray = new int[batches.size()];
					this.prevTarget = new int[batches.size()];
					this.inputStreamLength = new int[batches.size()];
					Arrays.fill(this.prevTarget, -1);
					queue = new IntHeapSemiIndirectPriorityQueue(this.refArray);
					// We open all files and load the first element into the reference array.
					for(int i = 0; i < batches.size(); i++) {
						batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
						labelInputBitStream[i] = new InputBitStream(labelBatches.get(i), STD_BUFFER_SIZE);
						this.inputStreamLength[i] = batchIbs[i].readDelta();
						this.refArray[i] = batchIbs[i].readDelta();
						queue.enqueue(i);
					}
				}
				else {
					this.refArray = refArray;
					this.prevTarget = prevTarget;
					this.inputStreamLength = inputStreamLength;
					queue = new IntHeapSemiIndirectPriorityQueue(refArray);

					for(int i = 0; i < refArray.length; i++) {
						if (baseIbs[i] != null) {
							batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
							batchIbs[i].position(baseIbs[i].position());
							labelInputBitStream[i] = new InputBitStream(labelBatches.get(i), STD_BUFFER_SIZE);
							labelInputBitStream[i].position(baseLabelInputBitStream[i].position());
							queue.enqueue(i);
						}
					}
				}
			}

			@Override
			public int outdegree() {
				if (last == -1) throw new IllegalStateException();
				return outdegree;
			}

			@Override
			public boolean hasNext() {
				return last < hasNextLimit;
			}

			@Override
			public int nextInt() {
				last++;
				int d = 0;
				int i;

				try {
					/* We extract elements from the queue as long as their target is equal
					 * to last. If during the process we exhaust a batch, we close it. */

					while(! queue.isEmpty() && refArray[i = queue.first()] == last) {
						successor = IntArrays.grow(successor, d + 1);
						successor[d] = (prevTarget[i] += batchIbs[i].readDelta() + 1);
						label = ObjectArrays.grow(label, d + 1);
						label[d] = prototype.copy();
						label[d].fromBitStream(labelInputBitStream[i], last);

						if (--inputStreamLength[i] == 0) {
							queue.dequeue();
							batchIbs[i].close();
							labelInputBitStream[i].close();
							batchIbs[i] = null;
							labelInputBitStream[i] = null;
						}
						else {
							// We read a new source and update the queue.
							final int sourceDelta = batchIbs[i].readDelta();
							if (sourceDelta != 0) {
								refArray[i] += sourceDelta;
								prevTarget[i] = -1;
								queue.changed();
							}
						}
						d++;
					}
					// Neither quicksort nor heaps are stable, so we reestablish order here.
					it.unimi.dsi.fastutil.Arrays.quickSort(0, d, (x, y) -> Integer.compare(successor[x], successor[y]),
							(x, y) -> {
								final int t = successor[x];
								successor[x] = successor[y];
								successor[y] = t;
								final Label l = label[x];
								label[x] = label[y];
								label[y] = l;
							});
				}
				catch(final IOException e) {
					throw new RuntimeException(e);
				}

				outdegree = d;
				return last;
			}

			@Override
			public int[] successorArray() {
				if (last == -1) throw new IllegalStateException();
				return successor;
			}

			@SuppressWarnings("deprecation")
			@Override
			protected void finalize() throws Throwable {
				try {
					for(final InputBitStream ibs: batchIbs) if (ibs != null) ibs.close();
					for(final InputBitStream ibs: labelInputBitStream) if (ibs != null) ibs.close();
				}
				finally {
					super.finalize();
				}
			}

			@Override
			public LabelledArcIterator successors() {
				if (last == -1) throw new IllegalStateException();
				return new LabelledArcIterator() {
					int last = -1;

					@Override
					public Label label() {
						return label[last];
					}

					@Override
					public int nextInt() {
						if (last + 1 == outdegree) return -1;
						return successor[++last];
					}

					@Override
					public int skip(final int k) {
						final int toSkip = Math.min(k, outdegree - last - 1);
						last += toSkip;
						return toSkip;
					}
				};
			}


			@Override
			public ArcLabelledNodeIterator copy(final int upperBound) {
				try {
					if (last == -1) return new InternalArcLabelledNodeIterator(upperBound);
					else return new InternalArcLabelledNodeIterator(upperBound, batchIbs, labelInputBitStream,
							refArray.clone(), prevTarget.clone(), inputStreamLength.clone(), last, outdegree, Arrays.copyOf(successor, outdegree), Arrays.copyOf(label, outdegree));
				}
				catch (final IOException e) {
					throw new RuntimeException(e);
				}
			}
		}


		@Override
		public ArcLabelledNodeIterator nodeIterator() {
			try {
				return new InternalArcLabelledNodeIterator(Integer.MAX_VALUE);
			}
			catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		protected void finalize() throws Throwable {
			try {
				for(final File f : batches) f.delete();
				for(final File f : labelBatches) f.delete();
			}
			finally {
				super.finalize();
			}
		}
		
		@Override
		public Label prototype() {
			return prototype;
		}
	}


	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>.
	 *
	 * <P>This method can process {@linkplain ImmutableGraph#loadOffline(CharSequence) offline graphs}.
	 *
	 * @param g an immutable graph.
	 * @return an immutable graph obtained by transposing <code>g</code>.
	 * @see #transpose(ImmutableGraph, ProgressLogger)
	 */

	public static ImmutableGraph transpose(final ImmutableGraph g) {
		return transpose(g, null);
	}

	/** Returns the union of two arc-labelled immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc
	 *  is present in both graphs; if <code>null</code>, {@link Labels#KEEP_FIRST_MERGE_STRATEGY}
	 *  is used.
	 * @return the union of the two graphs.
	 */
	public static ArcLabelledImmutableGraph union(final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelMergeStrategy labelMergeStrategy) {
		return new UnionArcLabelledImmutableGraph(g0, g1, labelMergeStrategy == null? Labels.KEEP_FIRST_MERGE_STRATEGY : labelMergeStrategy);
	}

	/** Returns the union of two immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @return the union of the two graphs.
	 */
	public static ImmutableGraph union(final ImmutableGraph g0, final ImmutableGraph g1) {
		return g0 instanceof ArcLabelledImmutableGraph && g1 instanceof ArcLabelledImmutableGraph
			? union((ArcLabelledImmutableGraph)g0, (ArcLabelledImmutableGraph)g1, (LabelMergeStrategy)null)
					: new UnionImmutableGraph(g0, g1);
	}


	private static final class ComposedGraph extends ImmutableSequentialGraph {
		private final class ComposedGraphNodeIterator extends NodeIterator {
			private final NodeIterator it0;
			private final int upperBound;
			private int[] succ;
			private final IntOpenHashSet successors;
			private int outdegree; // -1 means that the cache is empty
			private int nextNode;

			public ComposedGraphNodeIterator(final int upperBound) {
				this(upperBound, g0.nodeIterator(), IntArrays.EMPTY_ARRAY, new IntOpenHashSet(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR), -1, 0);
			}

			public ComposedGraphNodeIterator(final int upperBound, final NodeIterator it, final int[] succ, final IntOpenHashSet successors, final int outdegree, final int nextNode) {
				this.it0 = it;
				this.upperBound = upperBound;
				this.succ = succ;
				this.successors = successors;
				this.outdegree = outdegree;
				this.nextNode = nextNode;
			}

			@Override
			public int nextInt() {
				outdegree = -1;
				nextNode++;
				return it0.nextInt();
			}

			@Override
			public boolean hasNext() {
				return nextNode < upperBound && it0.hasNext();
			}

			@Override
			public int outdegree() {
				if (outdegree < 0) successorArray();
				return outdegree;
			}

			@Override
			public int[] successorArray() {
				if (outdegree < 0) {
					final int d = it0.outdegree();
					final int[] s = it0.successorArray();
					successors.clear();
					for (int i = 0; i < d; i++) {
						final LazyIntIterator s1 = g1.successors(s[i]);
						int x;
						while ((x = s1.nextInt()) >= 0) successors.add(x);
					}
					outdegree = successors.size();
					succ = IntArrays.ensureCapacity(succ, outdegree, 0);
					successors.toArray(succ);
					IntArrays.quickSort(succ, 0, outdegree);
				}
				return succ;
			}

			@Override
			public NodeIterator copy(final int upperBound) {
				return new ComposedGraphNodeIterator(upperBound, it0.copy(Integer.MAX_VALUE), Arrays.copyOf(succ, succ.length), new IntOpenHashSet(successors), outdegree, nextNode);
			}
		}

		private final ImmutableGraph g0;
		private final ImmutableGraph g1;

		private ComposedGraph(final ImmutableGraph g0, final ImmutableGraph g1) {
			this.g0 = g0;
			this.g1 = g1;
		}

		@Override
		public int numNodes() {
			return Math.max(g0.numNodes(), g1.numNodes());
		}

		@Override
		public ImmutableSequentialGraph copy() {
			// Note that only the second graph needs duplication.
			return new ComposedGraph(g0, g1.copy());
		}

		@Override
		public boolean hasCopiableIterators() {
			return true;
		}

		@Override
		public NodeIterator nodeIterator() {
			return new ComposedGraphNodeIterator(Integer.MAX_VALUE);
		}
	}

	/** Returns the composition (a.k.a. matrix product) of two immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @return the composition of the two graphs.
	 */
	public static ImmutableGraph compose(final ImmutableGraph g0, final ImmutableGraph g1) {
		return new ComposedGraph(g0, g1);
	}


	/** Returns the composition (a.k.a. matrix product) of two arc-labelled immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param strategy a label semiring.
	 * @return the composition of the two graphs.
	 */
	public static ArcLabelledImmutableGraph compose(final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelSemiring strategy) {
		if (g0.prototype().getClass() != g1.prototype().getClass()) throw new IllegalArgumentException("The two graphs have different label classes (" + g0.prototype().getClass().getSimpleName() + ", " +g1.prototype().getClass().getSimpleName() + ")");

		return new ArcLabelledImmutableSequentialGraph() {

			class InternalArcLabelledNodeIterator extends ArcLabelledNodeIterator {
				private final int upperBound;
				private int nextNode;
				private int[] succ = IntArrays.EMPTY_ARRAY;
				private Label[] label = Label.EMPTY_LABEL_ARRAY;
				private int maxOutDegree;
				private int smallCount;
				private Int2ObjectOpenHashMap<Label> successors = new Int2ObjectOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR);
				private int outdegree = -1; // -1 means that the cache is empty
				private ArcLabelledNodeIterator it0;

				public InternalArcLabelledNodeIterator(final int upperBond) {
					successors.defaultReturnValue(strategy.zero());
					it0 = g0.nodeIterator();
					this.upperBound = upperBond;
				}

				@Override
				public int nextInt() {
					outdegree = -1;
					nextNode++;
					return it0.nextInt();
				}

				@Override
				public boolean hasNext() {
					return nextNode < upperBound && it0.hasNext();
				}


				@Override
				public int outdegree() {
					if (outdegree < 0) successorArray();
					return outdegree;
				}

				private void ensureCache() {
					if (outdegree < 0) {
						final int d = it0.outdegree();
						final LabelledArcIterator s = it0.successors();
						if (successors.size() < maxOutDegree / 2 && smallCount++ > 100) {
							smallCount = 0;
							maxOutDegree = 0;
							successors = new Int2ObjectOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR);
							successors.defaultReturnValue(strategy.zero());
						} else successors.clear();

						for (int i = 0; i < d; i++) {
							final LabelledArcIterator s1 = g1.successors(s.nextInt());
							int x;
							while ((x = s1.nextInt()) >= 0) successors.put(x, strategy.add(strategy.multiply(s.label(), s1.label()), successors.get(x)));
						}
						outdegree = successors.size();
						succ = IntArrays.ensureCapacity(succ, outdegree, 0);
						label = ObjectArrays.ensureCapacity(label, outdegree, 0);
						successors.keySet().toArray(succ);
						IntArrays.quickSort(succ, 0, outdegree);
						for (int i = outdegree; i-- != 0;) label[i] = successors.get(succ[i]);
						if (outdegree > maxOutDegree) maxOutDegree = outdegree;
					}
				}

				@Override
				public int[] successorArray() {
					ensureCache();
					return succ;
				}

				@Override
				public Label[] labelArray() {
					ensureCache();
					return label;
				}

				@Override
				public LabelledArcIterator successors() {
					ensureCache();
					return new LabelledArcIterator() {
						int i = -1;

						@Override
						public Label label() {
							return label[i];
						}

						@Override
						public int nextInt() {
							return i < outdegree - 1 ? succ[++i] : -1;
						}

						@Override
						public int skip(final int n) {
							final int incr = Math.min(n, outdegree - i - 1);
							i += incr;
							return incr;
						}
					};
				}

				@Override
				public ArcLabelledNodeIterator copy(final int upperBound) {
					final InternalArcLabelledNodeIterator result = new InternalArcLabelledNodeIterator(upperBound);
					result.it0 = it0.copy(upperBound);
					result.nextNode = nextNode;
					result.succ = Arrays.copyOf(succ, succ.length);
					result.label = Arrays.copyOf(label, label.length);
					result.maxOutDegree = maxOutDegree;
					result.smallCount = smallCount;
					result.successors = new Int2ObjectOpenHashMap<>(successors);
					result.successors.defaultReturnValue(successors.defaultReturnValue());
					result.outdegree = outdegree;
					return result;
				}
			}

			@Override
			public Label prototype() {
				return g0.prototype();
			}

			@Override
			public int numNodes() {
				return Math.max(g0.numNodes(), g1.numNodes());
			}

			@Override
			public boolean hasCopiableIterators() {
				return g0.hasCopiableIterators() && g1.hasCopiableIterators();
			}

			@Override
			public ArcLabelledNodeIterator nodeIterator() {
				return new InternalArcLabelledNodeIterator(Integer.MAX_VALUE);
			}
		};
	}

	/** Computes the line graph of a given symmetric graph. The line graph of <var>g</var> is a graph, whose nodes are
	 *  identified with pairs of the form &lt;<var>x</var>,&nbsp;<var>y</var>&gt; where <var>x</var> and <var>y</var> are nodes of <var>g</var>
	 *  and &lt;<var>x</var>,&nbsp;<var>y</var>&gt; is an arc of <var>g</var>. Moreover, there is an arc from &lt;<var>x</var>,&nbsp;<var>y</var>&gt; to
	 *  &lt;<var>y</var>,&nbsp;<var>z</var>&gt;.
	 *
	 * <P>Two additional files are created, with names stemmed from <code>mapBasename</code>; the <var>i</var>-th entries of the two files
	 * identify the source and target node (in the original graph) corresponding the node <var>i</var> in the line graph.
	 *
	 * @param g the graph (it must be symmetric and loopless).
	 * @param mapBasename the basename of two files that will, at the end, contain as many integers as the number of nodes in the line graph: the <var>i</var>-th
	 *   integer in the file <code><var>mapBasename</var>.source</code> will contain the source of the arc corresponding to the <var>i</var>-th
	 *   node in the line graph, and similarly <code><var>mapBasename</var>.target</code> will give the target.
	 * @param tempDir the temporary directory to be used.
	 * @param batchSize the size used for batches.
	 * @param pl the progress logger to be used.
	 * @return the line graph of <code>g</code>.
	 * @throws IOException
	 */
	public static ImmutableSequentialGraph line(final ImmutableGraph g, final String mapBasename, final File tempDir, final int batchSize, final ProgressLogger pl) throws IOException {
		final int n = g.numNodes();
		final int[] source = new int[batchSize], target = new int[batchSize];
		int currBatch = 0, pairs = 0;
		final ObjectArrayList<File> batches = new ObjectArrayList<>();
		final long[] edge = new long[(int)g.numArcs()];
		int edgesSoFar = 0;
		NodeIterator nodeIterator = g.nodeIterator();
		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start("Producing batches for line graph");
		}
		long expNumberOfArcs = 0;
		while (nodeIterator.hasNext()) {
			final int x = nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			expNumberOfArcs += d * d;
			final int[] succ = nodeIterator.successorArray();
			// New edges
			for (int i = 0; i < d; i++) {
				if (succ[i] == x) throw new IllegalArgumentException("The graph contains a loop on node " + x);
				edge[edgesSoFar++] = ((long)x << 32) | succ[i];
			}
		}
		LOGGER.info("Expected number of arcs: " + expNumberOfArcs);
		LongArrays.parallelQuickSort(edge);
		nodeIterator = g.nodeIterator();

		while (nodeIterator.hasNext()) {
			final int x = nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			final int[] succ = nodeIterator.successorArray().clone();
			for (int i = 0; i < d; i++) {
				final int from0 = x; //Math.min(x, succ[i]);
				final int to0 = succ[i]; //Math.max(x, succ[i]);
				final int edge0 = LongArrays.binarySearch(edge, 0, edgesSoFar, ((long)from0 << 32) | to0);
				assert edge0 >= 0;
				final int dNext = g.outdegree(to0);
				final int[] succNext = g.successorArray(to0);
				for (int j = 0; j < dNext; j++) {
					final int from1 = to0;  //Math.min(x, succ[j]);
					final int to1 = succNext[j]; //Math.max(x, succ[j]);
					final int edge1 = LongArrays.binarySearch(edge, 0, edgesSoFar, ((long)from1 << 32) | to1);
					assert edge1 >= 0;
					if (currBatch == batchSize) {
						pairs += processBatch(batchSize, source, target, tempDir, batches);
						currBatch = 0;
					}
					source[currBatch] = edge0;
					target[currBatch++] = edge1;
				}
			}
			if (pl != null) pl.lightUpdate();
		}
		if (currBatch > 0)  {
			pairs += processBatch(currBatch, source, target, tempDir, batches);
			currBatch = 0;
		}
		if (edgesSoFar != edge.length) throw new IllegalArgumentException("Something went wrong (probably the graph was not symmetric)");
		final DataOutputStream dosSource = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(mapBasename + ".source")));
		final DataOutputStream dosTarget = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(mapBasename + ".target")));
		for (final long e: edge) {
			dosSource.writeInt((int)(e >> 32));
			dosTarget.writeInt((int)(e & 0xFFFFFFFF));
		}
		dosSource.close();
		dosTarget.close();
		if (DEBUG)
			for (int i = 0; i < edgesSoFar; i++) {
				System.out.println(i + " <- (" + (edge[i] >> 32) + "," + (edge[i] & 0xFFFFFFFF) +")");
			}
		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}
		return new BatchGraph(edgesSoFar, -1, batches);
	}

	/** Returns a permutation that would make the given graph adjacency lists in Gray-code order.
	 *
	 * <P>Gray codes list all sequences of <var>n</var> zeros and ones in such a way that
	 * adjacent lists differ by exactly one bit. If we assign to each row of the adjacency matrix of
	 * a graph its index as a Gray code, we obtain a permutation that will make similar lines
	 * nearer.
	 *
	 * <P>Note that since a graph permutation permutes <em>both</em> rows and columns, this transformation is
	 * not idempotent: the Gray-code permutation produced from a matrix that has been Gray-code sorted will
	 * <em>not</em> be, in general, the identity.
	 *
	 * <P>The important feature of Gray-code ordering is that it is completely endogenous (e.g., determined
	 * by the graph itself), contrarily to, say, lexicographic URL ordering (which relies on the knowledge
	 * of the URL associated to each node).
	 *
	 * @param g an immutable graph.
	 * @return the permutation that would order the graph adjacency lists by Gray order
	 * (you can just pass it to {@link #map(ImmutableGraph, int[], ProgressLogger)}).
	 */
	public static int[] grayCodePermutation(final ImmutableGraph g) {
		final int n = g.numNodes();
		final int[] perm = new int[n];
		int i = n;
		while(i-- != 0) perm[i] = i;

		final IntComparator grayComparator =  (x, y) -> {
			final LazyIntIterator i1 = g.successors(x), j = g.successors(y);
			int a, b;

			/* This code duplicates eagerly of the behaviour of the lazy comparator
			   below. It is here for documentation and debugging purposes.

			byte[] g1 = new byte[g.numNodes()], g2 = new byte[g.numNodes()];
			while(i.hasNext()) g1[g.numNodes() - 1 - i.nextInt()] = 1;
			while(j.hasNext()) g2[g.numNodes() - 1 - j.nextInt()] = 1;
			for(int k = g.numNodes() - 2; k >= 0; k--) {
				g1[k] ^= g1[k + 1];
				g2[k] ^= g2[k + 1];
			}
			for(int k = g.numNodes() - 1; k >= 0; k--) if (g1[k] != g2[k]) return g1[k] - g2[k];
			return 0;
			*/

			boolean parity = false; // Keeps track of the parity of number of arcs before the current ones.
			for(;;) {
				a = i1.nextInt();
				b = j.nextInt();
				if (a == -1 && b == -1) return 0;
				if (a == -1) return parity ? 1 : -1;
				if (b == -1) return parity ? -1 : 1;
				if (a != b) return parity ^ (a < b) ? 1 : -1;
				parity = ! parity;
			}
		};

		IntArrays.parallelQuickSort(perm, 0, n, grayComparator);

		if (ASSERTS) for(int k = 0; k < n - 1; k++) assert grayComparator.compare(perm[k], perm[k + 1]) <= 0;

		final int[] invPerm = new int[n];
		i = n;
		while(i-- != 0) invPerm[perm[i]] = i;

		return invPerm;
	}

	/** Returns a random permutation for a given graph.
	 *
	 * @param g an immutable graph.
	 * @param seed for {@link XoRoShiRo128PlusRandom}.
	 * @return a random permutation for the given graph
	 */
	public static int[] randomPermutation(final ImmutableGraph g, final long seed) {
		return IntArrays.shuffle(Util.identity(g.numNodes()), new XoRoShiRo128PlusRandom(seed));
	}

	/** Returns a permutation that would make the given graph adjacency lists in host-by-host Gray-code order.
	 *
	 * <p>This permutation differs from {@link #grayCodePermutation(ImmutableGraph)} in that Gray codes
	 * are computed host by host. There are two variants, <em>strict</em> and <em>loose</em>. In the first case,
	 * we restrict the adjacency matrix to the submatrix corresponding to a host and compute the ordering. In
	 * the second case, we just restrict to the rows corresponding to a host, but then entire rows are used
	 * to compute the ordering.
	 *
	 * @param g an immutable graph.
	 * @param hostMap an array mapping each URL to its host (it is sufficient that each host is assigned a distinct number).
	 * @param strict if true, host-by-host Gray code computation will be strict, that is, the order is computed only
	 * between columns of the same host of the rows.
	 * @return the permutation that would order the graph adjacency lists by host-by-host Gray order
	 * (you can just pass it to {@link #map(ImmutableGraph, int[], ProgressLogger)}).
	 */
	public static int[] hostByHostGrayCodePermutation(final ImmutableGraph g, final int[] hostMap, final boolean strict) {
		final int n = g.numNodes();
		final int[] perm = new int[n];
		int i = n;
		while(i-- != 0) perm[i] = i;

		final IntComparator hostByHostGrayComparator =  (x, y) -> {
			final int t = hostMap[x] - hostMap[y];
			if (t != 0) return t;
			final LazyIntIterator i1 = g.successors(x), j = g.successors(y);
			int a, b;

			boolean parity = false; // Keeps track of the parity of number of arcs before the current ones.
			for(;;) {
				if (strict) {
					final int h = hostMap[x];
					do a = i1.nextInt(); while(a != -1 && hostMap[a] != h);
					do b = j.nextInt(); while(b != -1 && hostMap[b] != h);
				}
				else {
					a = i1.nextInt();
					b = j.nextInt();
				}
				if (a == -1 && b == -1) return 0;
				if (a == -1) return parity ? 1 : -1;
				if (b == -1) return parity ? -1 : 1;
				if (a != b) return parity ^ (a < b) ? 1 : -1;
				parity = ! parity;
			}
		};

		IntArrays.parallelQuickSort(perm, 0, n, hostByHostGrayComparator);

		if (ASSERTS) for(int k = 0; k < n - 1; k++) assert hostByHostGrayComparator.compare(perm[k], perm[k + 1]) <= 0;

		final int[] invPerm = new int[n];
		i = n;
		while(i-- != 0) invPerm[perm[i]] = i;

		return invPerm;
	}



	/** Returns a permutation that would make the given graph adjacency lists in lexicographical order.
	 *
	 * <P>Note that since a graph permutation permutes <em>both</em> rows and columns, this transformation is
	 * not idempotent: the lexicographical permutation produced from a matrix that has been
	 * lexicographically sorted will
	 * <em>not</em> be, in general, the identity.
	 *
	 * <P>The important feature of lexicographical ordering is that it is completely endogenous (e.g., determined
	 * by the graph itself), contrarily to, say, lexicographic URL ordering (which relies on the knowledge
	 * of the URL associated to each node).
	 *
	 * <p><strong>Warning</strong>: rows are numbered from zero <em>from the left</em>. This means,
	 * for instance, that nodes with an arc towards node zero are lexicographically smaller
	 * than nodes without it.
	 *
	 * @param g an immutable graph.
	 * @return the permutation that would order the graph adjacency lists by lexicographical order
	 * (you can just pass it to {@link #map(ImmutableGraph, int[], ProgressLogger)}).
	 */
	public static int[] lexicographicalPermutation(final ImmutableGraph g) {
		final int n = g.numNodes();
		final int[] perm = new int[n];
		int i = n;
		while(i-- != 0) perm[i] = i;

		final IntComparator lexicographicalComparator =  (x, y) -> {
			final LazyIntIterator i1 = g.successors(x), j = g.successors(y);
			int a, b;
			for(;;) {
				a = i1.nextInt();
				b = j.nextInt();
				if (a == -1 && b == -1) return 0;
				if (a == -1) return -1;
				if (b == -1) return 1;
				if (a != b) return b - a;
			}
		};

		IntArrays.parallelQuickSort(perm, 0, n, lexicographicalComparator);

		if (ASSERTS) for(int k = 0; k < n - 1; k++) assert lexicographicalComparator.compare(perm[k], perm[k + 1]) <= 0;

		final int[] invPerm = new int[n];
		i = n;
		while(i-- != 0) invPerm[perm[i]] = i;

		return invPerm;
	}



	/** Ensures that the arguments are exactly <code>n</code>, if <code>n</code> is nonnegative, or
	 * at least -<code>n</code>, otherwise.
	 */

	protected static boolean ensureNumArgs(final String param[], final int n) {
		if (n >= 0 && param.length != n || n < 0 && param.length < -n) {
			return false;
		}
		return true;
	}

	/** Loads a graph with given data and returns it.
	 *
	 * @param graphClass the class of the graph to be loaded.
	 * @param baseName the graph basename.
	 * @param offline whether the graph is to be loaded in an offline fashion.
	 * @param pl a progress logger.
	 * @return the loaded graph.
	 */
	protected static ImmutableGraph load(final Class<?> graphClass, final String baseName, final boolean offline, final ProgressLogger pl) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException {
		ImmutableGraph graph = null;

		if (graphClass != null) {
			if (offline) graph = (ImmutableGraph)graphClass.getMethod("loadOffline", CharSequence.class).invoke(null, baseName);
			else graph = (ImmutableGraph)graphClass.getMethod("load", CharSequence.class, ProgressLogger.class).invoke(null, baseName, pl);
		}
		else graph = offline ? ImmutableGraph.loadOffline(baseName) : ImmutableGraph.load(baseName, pl);

		return graph;
	}


	public static void main(final String args[]) throws IOException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, JSAPException {
		Class<?> sourceGraphClass = null, destGraphClass = BVGraph.class;

		final Field[] field = Transform.class.getDeclaredFields();
		final List<String> filterList = new ArrayList<>();
		final List<String> labelledFilterList = new ArrayList<>();

		for(final Field f: field) {
			if (ArcFilter.class.isAssignableFrom(f.getType())) filterList.add(f.getName());
			if (LabelledArcFilter.class.isAssignableFrom(f.getType())) labelledFilterList.add(f.getName());
		}

		final SimpleJSAP jsap = new SimpleJSAP(Transform.class.getName(),
				"Transforms one or more graphs. All transformations require, after the name,\n" +
				"some parameters specified below:\n" +
				"\n" +
				"identity                  sourceBasename destBasename\n" +
				"map                       sourceBasename destBasename map [cutoff]\n" +
				"mapOffline                sourceBasename destBasename map [batchSize] [tempDir]\n" +
				"transpose                 sourceBasename destBasename\n" +
				"transposeOffline          sourceBasename destBasename [batchSize] [tempDir]\n" +
				"symmetrize                sourceBasename [transposeBasename] destBasename\n" +
				"symmetrizeOffline         sourceBasename destBasename [batchSize] [tempDir]\n" +
						"simplifyOffline           sourceBasename destBasename [batchSize] [tempDir]\n" +
						"simplify                  sourceBasename transposeBasename destBasename\n" +
				"union                     source1Basename source2Basename destBasename [strategy]\n" +
				"compose                   source1Basename source2Basename destBasename [semiring]\n" +
				"gray                      sourceBasename destBasename\n" +
				"grayPerm                  sourceBasename dest\n" +
				"strictHostByHostGray      sourceBasename destBasename hostMap\n" +
				"strictHostByHostGrayPerm  sourceBasename dest hostMap\n" +
				"looseHostByHostGray       sourceBasename destBasename hostMap\n" +
				"looseHostByHostGrayPerm   sourceBasename dest hostMap\n" +
				"lex                       sourceBasename destBasename\n" +
				"lexPerm                   sourceBasename dest\n" +
				"line                      sourceBasename destBasename mapName [batchSize]\n" +
				"random                    sourceBasename destBasename [seed]\n" +
				"arcfilter                 sourceBasename destBasename arcFilter (available filters: " + filterList + ")\n" +
				"larcfilter                sourceBasename destBasename arcFilter (available filters: " + labelledFilterList + ")\n" +
				"\n" +
				"Please consult the Javadoc documentation for more information on each transform.",
				new Parameter[] {
						new FlaggedOption("sourceGraphClass", GraphClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "source-graph-class", "Forces a Java class to load the source graph."),
						new FlaggedOption("destGraphClass", GraphClassParser.getParser(), BVGraph.class.getName(), JSAP.NOT_REQUIRED, 'd', "dest-graph-class", "Forces a Java class to store the destination graph."),
						new FlaggedOption("destArcLabelledGraphClass", GraphClassParser.getParser(), BitStreamArcLabelledImmutableGraph.class.getName(), JSAP.NOT_REQUIRED, 'L', "dest-arc-labelled-graph-class", "Forces a Java class to store the labels of the destination graph."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new Switch("offline", 'o', "offline", "Use the offline load method to reduce memory consumption (disables multi-threaded compression)."),
						new Switch("sequential", 'S', "sequential", "Equivalent to offline."),
						new Switch("ascii", 'a', "ascii", "Maps are in ASCII form (one integer per line)."),
						new UnflaggedOption("transform", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The transformation to be applied."),
						new UnflaggedOption("param", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The remaining parameters."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		sourceGraphClass = jsapResult.getClass("sourceGraphClass");
		destGraphClass = jsapResult.getClass("destGraphClass");
		final boolean offline = jsapResult.getBoolean("offline") || jsapResult.getBoolean("sequential");
		final boolean ascii = jsapResult.getBoolean("ascii");
		final String transform = jsapResult.getString("transform");
		final String[] param = jsapResult.getStringArray("param");

		String source[] = null, dest = null, map = null;
		ArcFilter arcFilter = null;
		LabelledArcFilter labelledArcFilter = null;
		LabelSemiring labelSemiring = null;
		LabelMergeStrategy labelMergeStrategy = null;
		int batchSize = 1000000, cutoff = -1;
		long seed = 0;
		File tempDir = null;

		if (! ensureNumArgs(param, -2)) return;

		if (transform.equals("identity") || transform.equals("transpose") || transform.equals("removeDangling") || transform.equals("gray") || transform.equals("grayPerm") || transform.equals("lex") || transform.equals("lexPerm")) {
			source = new String[] { param[0] };
			dest = param[1];
			if (! ensureNumArgs(param, 2)) return;
		}
		else if (transform.equals("removeHosts")) {
			if (! ensureNumArgs(param, -4)) return;
			source = new String[] { param[0] };
			dest = param[1];
		}
		else if (transform.equals("map") || transform.equals("strictHostByHostGray") || transform.equals("strictHostByHostGrayPerm") || transform.equals("looseHostByHostGray") || transform.equals("looseHostByHostGrayPerm")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0] };
			dest = param[1];
			map = param[2];
			if (param.length == 4) cutoff = Integer.parseInt(param[3]);
			else if (! ensureNumArgs(param, 3)) return;
		}
		else if (transform.equals("mapOffline")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0] };
			dest = param[1];
			map = param[2];
			if (param.length >= 4) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse(param[3])).intValue();
				if (param.length == 5) tempDir = new File(param[4]);
				else if (! ensureNumArgs(param, 4))	return;
			}
			else if (! ensureNumArgs(param, 3))	return;
		}
		else if (transform.equals("symmetrize")) {
			if (param.length == 2) {
				source = new String[] { param[0], null };
				dest = param[1];
			}
			else if (ensureNumArgs(param, 3)) {
				source = new String[] { param[0], param[1] };
				dest = param[2];
			}
			else return;
		}
		else if (transform.equals("random")) {
			if (param.length == 2) {
				source = new String[] { param[0], null };
				dest = param[1];
			}
			else if (ensureNumArgs(param, 3)) {
				source = new String[] { param[0] };
				dest = param[1];
				seed = Long.parseLong(param[2]);
			}
			else return;
		}
		else if (transform.equals("arcfilter")) {
			if (ensureNumArgs(param, 3)) {
				try {
					// First try: a public field
					arcFilter = (ArcFilter) Transform.class.getField(param[2]).get(null);
				}
				catch(final NoSuchFieldException e) {
					// No chance: let's try with a class
					arcFilter = ObjectParser.fromSpec(param[2], ArcFilter.class, GraphClassParser.PACKAGE);
				}
				source = new String[] { param[0], null };
				dest = param[1];
			}
			else return;
		}
		else if (transform.equals("larcfilter")) {
			if (ensureNumArgs(param, 3)) {
				try {
					// First try: a public field
					labelledArcFilter = (LabelledArcFilter) Transform.class.getField(param[2]).get(null);
				}
				catch(final NoSuchFieldException e) {
					// No chance: let's try with a class
					labelledArcFilter = ObjectParser.fromSpec(param[2], LabelledArcFilter.class, GraphClassParser.PACKAGE);
				}
				source = new String[] { param[0], null };
				dest = param[1];
			}
			else return;
		}
		else if (transform.equals("union")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0], param[1] };
			dest = param[2];
			if (param.length == 4) labelMergeStrategy = ObjectParser.fromSpec(param[3], LabelMergeStrategy.class, GraphClassParser.PACKAGE);
			else if (! ensureNumArgs(param, 3)) return;
		}
		else if (transform.equals("compose")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0], param[1] };
			dest = param[2];
			if (param.length == 4) labelSemiring = ObjectParser.fromSpec(param[3], LabelSemiring.class, GraphClassParser.PACKAGE);
			else if (! ensureNumArgs(param, 3)) return;
		}
		else if (transform.equals("simplify")) {
			if (!ensureNumArgs(param, 3)) return;
			source = new String[] { param[0], param[1] };
			dest = param[2];
		}
		else if (transform.equals("transposeOffline") || transform.equals("symmetrizeOffline") || transform.equals("simplifyOffline")) {
			if (! ensureNumArgs(param, -2)) return;
			source = new String[] { param[0] };
			dest = param[1];
			if (param.length >= 3) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse(param[2])).intValue();
				if (param.length == 4) tempDir = new File(param[3]);
				else if (! ensureNumArgs(param, 3))	return;
			}
			else if (! ensureNumArgs(param, 2))	return;
		}
		else if (transform.equals("line")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0] };
			dest = param[1];
			map = param[2];
			if (param.length == 4) batchSize = Integer.parseInt(param[3]);
		}
		else {
			System.err.println("Unknown transform: " + transform);
			return;
		}

		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		final ImmutableGraph[] graph = new ImmutableGraph[source.length];
		final ImmutableGraph result;
		final Class<?> destLabelledGraphClass = jsapResult.getClass("destArcLabelledGraphClass");
		if (! ArcLabelledImmutableGraph.class.isAssignableFrom(destLabelledGraphClass)) throw new IllegalArgumentException("The arc-labelled destination class " + destLabelledGraphClass.getName() + " is not an instance of ArcLabelledImmutableGraph");

		for (int i = 0; i < source.length; i++)
			// Note that composition requires the second graph to be always random access.
			if (source[i] == null) graph[i] = null;
			else graph[i] = load(sourceGraphClass, source[i], offline && ! (i == 1 && transform.equals("compose")), pl);

		final boolean graph0IsLabelled = graph[0] instanceof ArcLabelledImmutableGraph;
		final ArcLabelledImmutableGraph graph0Labelled = graph0IsLabelled ? (ArcLabelledImmutableGraph)graph[0] : null;
		final boolean graph1IsLabelled = graph.length > 1 && graph[1] instanceof ArcLabelledImmutableGraph;

		final String notForLabelled = "This transformation will just apply to the unlabelled graph; label information will be absent";

		if (transform.equals("identity")) result = graph[0];
		else if (transform.equals("map") || transform.equals("mapOffline")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			pl.start("Reading map...");

			final int n = graph[0].numNodes();
			final int[] f = new int[n];
			final long loaded;
			if (ascii) loaded = TextIO.loadInts(map, f);
			else loaded = BinIO.loadInts(map, f);

			if (n != loaded) throw new IllegalArgumentException("The source graph has " + n + " nodes, but the permutation contains " + loaded + " longs");

			// Delete from the graph all nodes whose index is above the cutoff, if any.
			if (cutoff != -1) for(int i = f.length; i-- != 0;) if (f[i] >= cutoff) f[i] = -1;

			pl.count = n;
			pl.done();

			result = transform.equals("map") ? map(graph[0], f, pl) : mapOffline(graph[0], f, batchSize, tempDir, pl);
			LOGGER.info("Transform computation completed.");
		}
		else if (transform.equals("arcfilter")) {
			if (graph0IsLabelled && ! (arcFilter instanceof LabelledArcFilter)) {
				LOGGER.warn(notForLabelled);
				result = filterArcs(graph[0], arcFilter, pl);
			}
			else result = filterArcs(graph[0], arcFilter, pl);
		}
		else if (transform.equals("larcfilter")) {
			if (! graph0IsLabelled) throw new IllegalArgumentException("Filtering on labelled arcs requires a labelled graph");
			result = filterArcs(graph0Labelled, labelledArcFilter, pl);
		}
		else if (transform.equals("symmetrize")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = symmetrize(graph[0], graph[1], pl);
		}
		else if (transform.equals("symmetrizeOffline")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = symmetrizeOffline(graph[0], batchSize, tempDir, pl);
		}
		else if (transform.equals("simplifyOffline")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = simplifyOffline(graph[0], batchSize, tempDir, pl);
		}
		else if (transform.equals("removeDangling")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);

			final int n = graph[0].numNodes();
			LOGGER.info("Finding dangling nodes...");

			final int[] f = new int[n];
			final NodeIterator nodeIterator = graph[0].nodeIterator();
			int c = 0;
			for(int i = 0; i < n; i++) {
				nodeIterator.nextInt();
				f[i] = nodeIterator.outdegree() != 0 ? c++ : -1;
			}
			result = map(graph[0], f, pl);
		}
		else if (transform.equals("removeHosts")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);

			@SuppressWarnings("unchecked")
			final List<CharSequence> urls = (List<CharSequence>)BinIO.loadObject(param[2]);
			final String[] host = Arrays.copyOfRange(param, 3, param.length);
			final int m = host.length;
			final int n = graph[0].numNodes();
			LOGGER.info("Finding pages with hosts " + Arrays.toString(host) + "...");

			final int[] f = new int[n];
			for(int i = 0, c = 0; i < n; i++) {
				final String url = urls.get(i).toString();
				int h;
				for(h = 0; h < m; h++)
					if (url.startsWith(host[h], 7) || url.startsWith(host[h], 8)) break;

				f[i] = h < m ? -1 : c++;
			}
			result = map(graph[0], f, pl);
		}
		else if (transform.equals("transpose")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = transpose(graph[0], pl);
		}
		else if (transform.equals("transposeOffline")) {
			result = graph0IsLabelled ? transposeOffline(graph0Labelled, batchSize, tempDir, pl) : transposeOffline(graph[0], batchSize, tempDir, pl);
		}
		else if (transform.equals("union")) {
			if (graph0IsLabelled && graph1IsLabelled) {
				if (labelMergeStrategy == null) throw new IllegalArgumentException("Uniting labelled graphs requires a merge strategy");
				result = union(graph0Labelled,  (ArcLabelledImmutableGraph)graph[1], labelMergeStrategy);
			}
			else {
				if (graph0IsLabelled || graph1IsLabelled) LOGGER.warn(notForLabelled);
				result = union(graph[0], graph[1]);
			}
		}
		else if (transform.equals("compose")) {
			if (graph0IsLabelled && graph1IsLabelled) {
				if (labelSemiring == null) throw new IllegalArgumentException("Composing labelled graphs requires a composition strategy");
				result = compose(graph0Labelled, (ArcLabelledImmutableGraph)graph[1], labelSemiring);
			}
			else {
				if (graph0IsLabelled || graph1IsLabelled) LOGGER.warn(notForLabelled);
				result = compose(graph[0], graph[1]);
			}
		}
		else if (transform.equals("simplify")) {
			if (graph0IsLabelled || graph1IsLabelled) LOGGER.warn(notForLabelled);
			result = simplify(graph[0], graph[1]);
		}
		else if (transform.equals("gray")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = map(graph[0], grayCodePermutation(graph[0]));
		}
		else if (transform.equals("grayPerm")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			BinIO.storeInts(grayCodePermutation(graph[0]), param[1]);
			return;
		}
		else if (transform.equals("strictHostByHostGray")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			final int[] f = new int[graph[0].numNodes()];
			if (ascii) TextIO.loadInts(map, f);
			else BinIO.loadInts(map, f);
			result = map(graph[0], hostByHostGrayCodePermutation(graph[0], f, true));
		}
		else if (transform.equals("strictHostByHostGrayPerm")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			final int[] f = new int[graph[0].numNodes()];
			if (ascii) TextIO.loadInts(map, f);
			else BinIO.loadInts(map, f);
			BinIO.storeInts(hostByHostGrayCodePermutation(graph[0], f, true), param[1]);
			return;
		}
		else if (transform.equals("looseHostByHostGray")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			final int[] f = new int[graph[0].numNodes()];
			if (ascii) TextIO.loadInts(map, f);
			else BinIO.loadInts(map, f);
			result = map(graph[0], hostByHostGrayCodePermutation(graph[0], f, false));
		}
		else if (transform.equals("looseHostByHostGrayPerm")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			final int[] f = new int[graph[0].numNodes()];
			if (ascii) TextIO.loadInts(map, f);
			else BinIO.loadInts(map, f);
			BinIO.storeInts(hostByHostGrayCodePermutation(graph[0], f, false), param[1]);
			return;
		}
		else if (transform.equals("lex")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = map(graph[0], lexicographicalPermutation(graph[0]));
		}
		else if (transform.equals("lexPerm")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			BinIO.storeInts(lexicographicalPermutation(graph[0]), param[1]);
			return;
		}
		else if (transform.equals("random")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = map(graph[0], randomPermutation(graph[0], seed));
		}
		else if (transform.equals("line")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = line(graph[0], map, tempDir, batchSize, pl);
		} else result = null;

		if (result instanceof ArcLabelledImmutableGraph) {
			// Note that we derelativise non-absolute pathnames to build the underlying graph name.
			LOGGER.info("The result is a labelled graph (class: " + destLabelledGraphClass.getName() + ")");
			final File destFile = new File(dest);
			final String underlyingName = (destFile.isAbsolute() ? dest : destFile.getName()) + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX;
			destLabelledGraphClass.getMethod("store", ArcLabelledImmutableGraph.class, CharSequence.class, CharSequence.class, ProgressLogger.class).invoke(null, result, dest, underlyingName, pl);
			ImmutableGraph.store(destGraphClass, result, dest + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX, pl);
		}
		else ImmutableGraph.store(destGraphClass, result, dest, pl);
	}
}
