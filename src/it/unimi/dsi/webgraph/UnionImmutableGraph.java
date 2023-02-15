/*
 * Copyright (C) 2003-2023 Paolo Boldi and Sebastiano Vigna
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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntArrays;

/** An immutable graph representing the union of two given graphs. Here by &ldquo;union&rdquo;
 *  we mean that an arc will belong to the union iff it belongs to at least one of the two graphs (the number of
 *  nodes of the union is taken to be the maximum among the number of nodes of each graph).
 */
public class UnionImmutableGraph extends ImmutableGraph {
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;

	private static final int INITIAL_ARRAY_SIZE = 64;

	private final ImmutableGraph g0, g1;
	private final int n0, n1, numNodes;

	/** The node whose successors are cached, or -1 if no successors are currently cached. */
	private int cachedNode = -1;

	/** The outdegree of the cached node, if any. */
	private int outdegree ;

	/** The successors of the cached node, if any; note that the array might be larger. */
	private int cache[];

	/** Creates the union of two given graphs.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 */
	public UnionImmutableGraph(final ImmutableGraph g0, final ImmutableGraph g1) {
		this.g0 = g0;
		this.g1 = g1;
		n0 = g0.numNodes();
		n1 = g1.numNodes();
		numNodes = Math.max(n0, n1);
	}

	@Override
	public UnionImmutableGraph copy() {
		return new UnionImmutableGraph(g0.copy(), g1.copy());
	}

	private static class InternalNodeIterator extends NodeIterator {
		/** If outdegree is nonnegative, the successors of the current node (this array may be, however, larger). */
		private int cache[];
		/** The outdegree of the current node, or -1 if the successor array for the current node has not been computed yet. */
		private int outdegree = -1;
		private NodeIterator i0;
		private NodeIterator i1;

		public InternalNodeIterator(final NodeIterator i0, final NodeIterator i1) {
			this(i0, i1, -1, IntArrays.EMPTY_ARRAY);
		}

		public InternalNodeIterator(final NodeIterator i0, final NodeIterator i1, final int outdegree, final int[] cache) {
			this.i0 = i0;
			this.i1 = i1;
			this.outdegree = outdegree;
			this.cache = cache;
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
				return cache = i1.successorArray();
			}
			if (i1 == null) {
				outdegree = i0.outdegree();
				return cache = i0.successorArray();
			}

			final MergedIntIterator merge = new MergedIntIterator(i0.successors(), i1.successors());
			outdegree = LazyIntIterators.unwrap(merge, cache);
			int upto, t;
			while ((t = merge.nextInt()) != -1) {
				upto = cache.length;
				cache = IntArrays.grow(cache, upto + 1);
				cache[upto++] = t;
				outdegree++;
				outdegree += LazyIntIterators.unwrap(merge, cache, upto, cache.length - upto);
			}
			return cache;
		}

		@Override
		public int outdegree() {
			successorArray(); // So that the cache is filled up
			return outdegree;
		}

		@Override
		public NodeIterator copy(final int upperBound) {
			return new InternalNodeIterator(i0 == null ? null : i0.copy(upperBound), i1 == null ? null : i1.copy(upperBound), outdegree, Arrays.copyOf(cache, Math.max(outdegree, 0)));
		}

	}

	@Override
	public NodeIterator nodeIterator(final int from) {
		return new InternalNodeIterator(from < n0 ? g0.nodeIterator(from) : null, from < n1 ? g1.nodeIterator(from) : null);
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

	private void fillCache(final int x) {
		if (x == cachedNode) return;
		final MergedIntIterator merge = new MergedIntIterator(x < n0? g0.successors(x) : LazyIntIterators.EMPTY_ITERATOR, x < n1? g1.successors(x) : LazyIntIterators.EMPTY_ITERATOR);
		int[] cache = new int[INITIAL_ARRAY_SIZE];
		outdegree = LazyIntIterators.unwrap(merge, cache);
		int upto, t;
		while ((t = merge.nextInt()) != -1) {
			upto = cache.length;
			cache = IntArrays.grow(cache, upto + 1);
			cache[upto++] = t;
			outdegree++;
			outdegree += LazyIntIterators.unwrap(merge, cache, upto, cache.length - upto);
		}

		this.cache = cache;
		cachedNode = x;
	}

	@Override
	public int[] successorArray(final int x) {
		fillCache(x);
		return cache;
	}

	@Override
	public int outdegree(final int x) {
		fillCache(x);
		return outdegree;
	}
}
