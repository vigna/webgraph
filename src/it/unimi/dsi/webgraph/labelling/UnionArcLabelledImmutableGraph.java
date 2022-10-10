/*
 * Copyright (C) 2007-2021 Paolo Boldi and Sebastiano Vigna
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

package it.unimi.dsi.webgraph.labelling;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectIterators;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.UnionImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

/** An arc-labelled immutable graph representing the union of two given such graphs.
 *  Here by &ldquo;union&rdquo; we mean that an arc will belong to the union iff it belongs to at least one of the two graphs (the number of
 *  nodes of the union is taken to be the maximum among the number of nodes of each graph). Labels are assumed to have the same
 *  prototype in both graphs, and are treated as follows: if an arc is present in but one graph, its label in the resulting
 *  graph is going to be the label of the arc in the graph where it comes from; if an arc is present in both graphs, the labels
 *  are combined using a provided {@link LabelMergeStrategy}.
 *
 *  <h2>Remarks about the implementation</h2>
 *
 *  <p>Due to the lack of multiple inheritance, we could not extend both {@link UnionImmutableGraph}
 *  and {@link ArcLabelledImmutableGraph}, hence we forcedly decided to extend the latter. The possibility of using delegation
 *  on the former was also discarded because the code for reading and merging labels is so tightly coupled with the rest that it
 *  would have been essentially useless (and even dangerous) to delegate the iteration methods. As a result, some of the code of this
 *  class is actually almost a duplicate of the code of {@link UnionImmutableGraph}.
 */
public class UnionArcLabelledImmutableGraph extends ArcLabelledImmutableGraph {
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	private static final int INITIAL_ARRAY_SIZE = 16;

	private final ArcLabelledImmutableGraph g0, g1;
	private final int n0, n1, numNodes;

	/** The strategy used to merge labels when the same arc is present in both graphs. */
	private final LabelMergeStrategy labelMergeStrategy;

	/** The node whose successors are cached, or -1 if no successors are currently cached. */
	private int cachedNode = -1;

	/** The outdegree of the cached node, if any. */
	private int outdegree;

	/** The successors of the cached node, if any; note that the array might be larger. */
	private int cache[];

	/** The labels on the arcs going out of the cached node, if any; note that the array might be larger. */
	private Label labelCache[];
	/** The prototype for the labels of this graph. */
	private final Label prototype;

	@Override
	public UnionArcLabelledImmutableGraph copy() {
		return new UnionArcLabelledImmutableGraph(g0.copy(), g1.copy(), labelMergeStrategy);
	}

	/** Creates the union of two given graphs.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc is present in both graphs.
	 */
	public UnionArcLabelledImmutableGraph(final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelMergeStrategy labelMergeStrategy) {
		this.g0 = g0;
		this.g1 = g1;
		this.labelMergeStrategy = labelMergeStrategy;
		n0 = g0.numNodes();
		n1 = g1.numNodes();
		numNodes = Math.max(n0, n1);
		if (g0.prototype().getClass() != g1.prototype().getClass()) throw new IllegalArgumentException("The two graphs have different label classes (" + g0.prototype().getClass().getSimpleName() + ", " +g1.prototype().getClass().getSimpleName() + ")");
		prototype = g0.prototype();
	}


	private static class InternalNodeIterator extends ArcLabelledNodeIterator {
		/** If outdegree is nonnegative, the successors of the current node (this array may be, however, larger). */
		private int cache[];
		/** If outdegree is nonnegative, the labels on the arcs going out of the current node (this array may be, however, larger). */
		private Label labelCache[];
		/** The outdegree of the current node, or -1 if the successor array for the current node has not been computed yet. */
		private int outdegree = -1;
		private ArcLabelledNodeIterator i0;
		private ArcLabelledNodeIterator i1;
		private final LabelMergeStrategy labelMergeStrategy;

		public InternalNodeIterator(final ArcLabelledNodeIterator i0, final ArcLabelledNodeIterator i1, final LabelMergeStrategy labelMergeStrategy) {
			this(i0, i1, labelMergeStrategy, -1, IntArrays.EMPTY_ARRAY, Label.EMPTY_LABEL_ARRAY);
		}

		public InternalNodeIterator(final ArcLabelledNodeIterator i0, final ArcLabelledNodeIterator i1, final LabelMergeStrategy labelMergeStrategy, final int outdegree, final int[] cache, final Label[] labelCache) {
			this.i0 = i0;
			this.i1 = i1;
			this.labelMergeStrategy = labelMergeStrategy;
			this.outdegree = outdegree;
			this.cache = cache;
			this.labelCache = labelCache;
		}

		@Override
		public boolean hasNext() {
			return i0 != null && i0.hasNext() || i1 != null && i1.hasNext();
		}

		@Override
		public int nextInt() {
			if (! hasNext()) throw new java.util.NoSuchElementException();
			outdegree = -1;
			int result = -1;
			if (i0 != null) {
				if (i0.hasNext()) result = i0.nextInt();
				else i0 = null;
			}
			if (i1 != null) {
				if (i1.hasNext()) result = i1.nextInt();
				else i1 = null;
			}
			return result;
		}

		@Override
		public int[] successorArray() {
			if (outdegree != -1) return cache;
			if (i0 == null) {
				outdegree = i1.outdegree();
				cache = i1.successorArray();
				labelCache = i1.labelArray();
				return cache;
			}
			if (i1 == null) {
				outdegree = i0.outdegree();
				cache = i0.successorArray();
				labelCache = i0.labelArray();
				return cache;
			}
			// We need to perform a manual merge
			final ArcLabelledNodeIterator.LabelledArcIterator succ0 = i0.successors();
			final ArcLabelledNodeIterator.LabelledArcIterator succ1 = i1.successors();
			int s0 = -1, s1 = -1;
			Label l0 = null, l1 = null;
			outdegree = 0;
			// Note that the parallel OR is necessary.
			while ((s0 != -1 || (s0 = succ0.nextInt()) != -1) | (s1 != -1 || (s1 = succ1.nextInt()) != -1)) {
				if (s0 != -1) l0 = succ0.label().copy();
				if (s1 != -1) l1 = succ1.label().copy();
				assert s0 >= 0 || s1 >= 0;
				cache = IntArrays.grow(cache, outdegree + 1);
				labelCache = ObjectArrays.grow(labelCache, outdegree + 1);
				if (s1 < 0 || 0 <= s0 && s0 < s1) {
					cache[outdegree] = s0;
					labelCache[outdegree] = l0;
					s0 = -1;
				} else if (s0 < 0 || 0 <= s1 && s1 < s0) {
					cache[outdegree] = s1;
					labelCache[outdegree] = l1;
					s1 = -1;
				} else {
					assert s0 == s1 && s0 >= 0;
					cache[outdegree] = s0;
					labelCache[outdegree] = labelMergeStrategy.merge(l0, l1);
					s0 = s1 = -1;
				}
				outdegree++;
			}
			return cache;
		}

		@Override
		public int outdegree() {
			successorArray(); // So that the cache is filled up
			return outdegree;
		}

		@Override
		public Label[] labelArray() {
			successorArray(); // So that the cache is filled up
			return labelCache;
		}

		@Override
		public LabelledArcIterator successors() {
			successorArray(); // So that the cache is filled up
			return new LabelledArcIterator() {
				int nextToBeReturned = 0;

				@Override
				public Label label() {
					return labelCache[nextToBeReturned - 1];
				}

				@Override
				public int nextInt() {
					if (nextToBeReturned == outdegree) return -1;
					return cache[nextToBeReturned++];
				}

				@Override
				public int skip(final int x) {
					final int skipped = Math.min(x, outdegree - nextToBeReturned);
					nextToBeReturned += skipped;
					return skipped;
				}
			};
		}

		@Override
		public ArcLabelledNodeIterator copy(final int upperBound) {
			return new InternalNodeIterator(i0 == null? i0 : i0.copy(upperBound), i1 == null? i1 : i1.copy(upperBound), labelMergeStrategy, outdegree, Arrays.copyOf(cache, Math.max(0, outdegree)), Arrays.copyOf(labelCache, Math.max(0, outdegree)));
		}
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator(final int from) {
		return new InternalNodeIterator(from < n0? g0.nodeIterator(from) : null, from < n1? g1.nodeIterator(from) : null, labelMergeStrategy);
	}

	@Override
	public int numNodes() {
		return numNodes;
	}

	@Override
	public boolean randomAccess() {
		return g0.randomAccess() && g1.randomAccess();
	}

	@Override
	public boolean hasCopiableIterators() {
		return g0.hasCopiableIterators() && g1.hasCopiableIterators();
	}

	@Override
	public int[] successorArray(final int x) {
		fillCache(x);
		return cache;
	}

	private void fillCache(final int x) {
		if (x == cachedNode) return;
		// We need to perform a manual merge
		final ArcLabelledNodeIterator.LabelledArcIterator succ0 = (LabelledArcIterator) (x < n0? g0.successors(x) : ObjectIterators.EMPTY_ITERATOR);
		final ArcLabelledNodeIterator.LabelledArcIterator succ1 = (LabelledArcIterator) (x < n1? g1.successors(x) : ObjectIterators.EMPTY_ITERATOR);
		int outdegree = 0;
		int s0 = -1, s1 = -1;
		Label l0 = null, l1 = null;
		int[] cache = new int[INITIAL_ARRAY_SIZE];
		Label[] labelCache = new Label[INITIAL_ARRAY_SIZE];
		while ((s0 != -1 || (s0 = succ0.nextInt()) != -1) | (s1 != -1 || (s1 = succ1.nextInt()) != -1)) {
			if (s0 != -1) l0 = succ0.label().copy();
			if (s1 != -1) l1 = succ1.label().copy();
			assert s0 >= 0 || s1 >= 0;
			cache = IntArrays.grow(cache, outdegree + 1);
			labelCache = ObjectArrays.grow(labelCache, outdegree + 1);
			if (s1 < 0 || 0 <= s0 && s0 < s1) {
				cache[outdegree] = s0;
				labelCache[outdegree] = l0;
				s0 = -1;
			} else if (s0 < 0 || 0 <= s1 && s1 < s0) {
				cache[outdegree] = s1;
				labelCache[outdegree] = l1;
				s1 = -1;
			} else {
				assert s0 == s1 && s0 >= 0;
				cache[outdegree] = s0;
				labelCache[outdegree] = labelMergeStrategy.merge(l0, l1);
				s0 = s1 = -1;
			}
			outdegree++;
		}

		this.cache = cache;
		this.labelCache = labelCache;
		this.outdegree = outdegree;
		cachedNode = x;
	}

	@Override
	public int outdegree(final int x) {
		fillCache(x);
		return outdegree;
	}

	@Override
	public Label[] labelArray(final int x) {
		fillCache(x);
		return labelCache;
	}

	@Override
	public LabelledArcIterator successors(final int x) {
		fillCache(x);
		final int outdegree = this.outdegree;
		final int[] cache = this.cache;
		final Label[] labelCache = this.labelCache;

		return new LabelledArcIterator() {
			int nextToBeReturned = 0;

			@Override
			public Label label() {
				return labelCache[nextToBeReturned - 1];
			}

			@Override
			public int nextInt() {
				if (nextToBeReturned == outdegree) return -1;
				return cache[nextToBeReturned++];
			}

			@Override
			public int skip(final int n) {
				final int skipped = Math.min(n, outdegree - nextToBeReturned);
				if (skipped < 0) return 0;
				nextToBeReturned += skipped;
				return skipped;
			}
		};
	}

	@Override
	public Label prototype() {
		return prototype;
	}

}
