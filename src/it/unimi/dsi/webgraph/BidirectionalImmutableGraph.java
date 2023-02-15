/*
 * Copyright (C) 2021-2023 Antoine Pietri and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.ints.IntIterator;

/**
 * A wrapper class exhibiting a {@linkplain #forward graph} and its {@linkplain #backward transpose}
 * as a bidirectional graph. Methods such as {@link #predecessors(int)}, {@link #indegrees()}, etc.
 * are implemented using the transpose.
 */
public class BidirectionalImmutableGraph extends ImmutableGraph {
	/** A graph. */
	public final ImmutableGraph forward;
	/** The transpose of {@link #forward}. */
	public final ImmutableGraph backward;

	/**
	 * Creates a bidirectional immutable graph.
	 *
	 * @param graph a graph.
	 * @param transpose its transpose.
	 */
	public BidirectionalImmutableGraph(final ImmutableGraph graph, final ImmutableGraph transpose) {
		this.forward = graph;
		this.backward = transpose;
		if (graph.numNodes() != transpose.numNodes()) throw new IllegalArgumentException("The graph and its transpose have a different number of nodes");
		try {
			if (graph.numArcs() != transpose.numArcs()) throw new IllegalArgumentException("The graph and its transpose have a different number of arcs");
		} catch (final UnsupportedOperationException e) {
			// Ignore, the graph does not support numArcs()
		}
	}

	@Override
	public int numNodes() {
		return this.forward.numNodes();
	}

	@Override
	public long numArcs() {
		return this.forward.numArcs();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This methods returns true if both {@link #forward} and {@link #backward} provide random
	 *		   access.
	 */
	@Override
	public boolean randomAccess() {
		return this.forward.randomAccess() && this.backward.randomAccess();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This methods returns true if both {@link #forward} and {@link #backward} have copiable
	 *		   iterators.
	 */
	@Override
	public boolean hasCopiableIterators() {
		return forward.hasCopiableIterators() && backward.hasCopiableIterators();
	}

	@Override
	public BidirectionalImmutableGraph copy() {
		return new BidirectionalImmutableGraph(this.forward.copy(), this.backward.copy());
	}

	/**
	 * Returns a view on the transpose of this bidirectional graph. Successors become predecessors, and
	 * vice-versa.
	 *
	 * @apiNote Note that the returned {@link BidirectionalImmutableGraph} is just a view. Thus, it
	 *		  cannot be accessed concurrently with this bidirectional graph. If you need concurrent
	 *		  access, please make a {@linkplain #copy() copy}.
	 *
	 * @return a view on the transpose of this bidirectional graph.
	 */
	public BidirectionalImmutableGraph transpose() {
		return new BidirectionalImmutableGraph(backward, forward);
	}

	/**
	 * Returns a view on the symmetrized version of this bidirectional graph.
	 *
	 * @apiNote Note that the returned {@link BidirectionalImmutableGraph} is just a view. Thus, it
	 *		  cannot be accessed concurrently with this bidirectional graph. If you need concurrent
	 *		  access, please make a {@linkplain #copy() copy}.
	 *
	 * @implSpec This methods returns the (lazy)
	 *		   {@linkplain Transform#union(ImmutableGraph, ImmutableGraph) union} of {@link #forward}
	 *		   and {@link #backward}. This is equivalent to forgetting the directionality of the arcs:
	 *		   the successors of a node are also its predecessors.
	 *
	 * @return the symmetrized version of this bidirectional graph.
	 */
	public BidirectionalImmutableGraph symmetrize() {
		final ImmutableGraph symmetric = Transform.union(forward, backward);
		return new BidirectionalImmutableGraph(symmetric, symmetric);
	}

	/**
	 * Returns a view on the simple (loopless and symmetric) version of this bidirectional graph.
	 *
	 * @apiNote Note that the returned {@link BidirectionalImmutableGraph} is just a view. Thus, it
	 *		  cannot be accessed concurrently with this bidirectional graph. If you need concurrent
	 *		  access, please make a {@linkplain #copy() copy}.
	 *
	 * @implSpec This methods returns the (lazy) result of
	 *		   {@linkplain Transform#simplify(ImmutableGraph, ImmutableGraph)} on {@link #forward} and
	 *		   {@link #backward}. Beside forgetting directionality of the arcs, as in
	 *		   {@link #symmetrize()}, loops are removed.
	 *
	 * @return the simple (symmetric and loopless) version of this bidirectional graph.
	 */
	public BidirectionalImmutableGraph simplify() {
		final ImmutableGraph simplified = Transform.simplify(forward, backward);
		return new BidirectionalImmutableGraph(simplified, simplified);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#outdegree(int)} on
	 *           {@link #forward}.
	 */
	@Override
	public int outdegree(final int l) {
		return forward.outdegree(l);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#successors(int)} on
	 *           {@link #forward}.
	 */
	@Override
	public LazyIntIterator successors(final int nodeId) {
		return forward.successors(nodeId);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#successorArray(int)} on
	 *           {@link #forward}.
	 */

	@Override
	public int[] successorArray(final int x) {
		return forward.successorArray(x);
	}

	/**
	 * Returns the indegree of a node
	 *
	 * @param x a node.
	 * @return the indegree of {@code x}.
	 */

	public int indegree(final int x) {
		return backward.outdegree(x);
	}

	/**
	 * Returns a lazy iterator over the successors of a given node. The iteration terminates when -1 is
	 * returned.
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#successors(int)} on
	 *           {@link #backward}.
	 *
	 * @param x a node.
	 * @return a lazy iterator over the predecessors of the node.
	 */
	public LazyIntIterator predecessors(final int x) {
		return backward.successors(x);
	}

	/**
	 * Returns a reference to a big array containing the predecessors of a given node.
	 *
	 * <P>
	 * The returned big array may contain more entries than the outdegree of <code>x</code>. However,
	 * only those with indices from 0 (inclusive) to the indegree of <code>x</code> (exclusive) contain
	 * valid data.
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#successorArray(int)} on
	 *           {@link #backward}.
	 *
	 * @param x a node.
	 * @return a big array whose first elements are the successors of the node; the array must not be
	 *         modified by the caller.
	 */
	public int[] predecessorArray(final int x) {
		return backward.successorArray(x);
	}

	/**
	 * Returns an iterator enumerating the outdegrees of the nodes of this graph.
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#outdegrees()} on
	 *		   {@link #backward}.
	 *
	 * @return an iterator enumerating the outdegrees of the nodes of this graph.
	 */
	public IntIterator indegrees() {
		return backward.outdegrees();
	}
}
