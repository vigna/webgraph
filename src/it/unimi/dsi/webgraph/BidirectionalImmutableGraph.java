/*
 * Copyright (C) 2021 Antoine Pietri
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
 * A directed immutable graph which can be iterated in both directions (forward and backward). It
 * exposes the backward equivalents of the ImmutableGraph primitives (indegree() and
 * predecessors()). This is implemented by passing two graphs, one in the forward and one in the
 * backward direction.
 */
public class BidirectionalImmutableGraph extends ImmutableGraph {
    private final ImmutableGraph forwardGraph;
    private final ImmutableGraph backwardGraph;

    /**
     * Creates a bidirectional immutable graph
     *
     * @param forwardGraph The graph in the forward direction
     * @param backwardGraph The graph in the backward direction
     */
    public BidirectionalImmutableGraph(ImmutableGraph forwardGraph, ImmutableGraph backwardGraph) {
        this.forwardGraph = forwardGraph;
        this.backwardGraph = backwardGraph;
    }

    @Override
    public int numNodes() {
        assert forwardGraph.numNodes() == backwardGraph.numNodes();
        return this.forwardGraph.numNodes();
    }

    @Override
    public long numArcs() {
        assert forwardGraph.numArcs() == backwardGraph.numArcs();
        return this.forwardGraph.numArcs();
    }

    @Override
    public boolean randomAccess() {
        return this.forwardGraph.randomAccess() && this.backwardGraph.randomAccess();
    }

    @Override
    public boolean hasCopiableIterators() {
        return forwardGraph.hasCopiableIterators() && backwardGraph.hasCopiableIterators();
    }

    @Override
    public BidirectionalImmutableGraph copy() {
        return new BidirectionalImmutableGraph(this.forwardGraph.copy(), this.backwardGraph.copy());
    }

    /**
     * Returns the transposed version of the bidirectional graph. Successors become predecessors, and
     * vice-versa.
     */
    public BidirectionalImmutableGraph transpose() {
        return new BidirectionalImmutableGraph(backwardGraph, forwardGraph);
    }

    /**
     * Returns the symmetric version of the bidirectional graph. It returns the (lazy) union of the
     * forward graph and the backward graph. This is equivalent to removing the directionality of the
     * edges: the successors of a node are also its predecessors.
     *
     * @return a symmetric, undirected BidirectionalImmutableGraph.
     */
    public BidirectionalImmutableGraph symmetrize() {
        ImmutableGraph symmetric = Transform.union(forwardGraph, backwardGraph);
        return new BidirectionalImmutableGraph(symmetric, symmetric);
    }

    /**
     * Returns the simplified version of the bidirectional graph. Works like symmetrize(), but also
     * removes the loop edges.
     *
     * @return a simplified (loopless and symmetric) BidirectionalImmutableGraph
     */
    public BidirectionalImmutableGraph simplify() {
        ImmutableGraph simplified = Transform.simplify(forwardGraph, backwardGraph);
        return new BidirectionalImmutableGraph(simplified, simplified);
    }

    /** Returns the outdegree of a node */
    @Override
    public int outdegree(int l) {
        return forwardGraph.outdegree(l);
    }

    /** Returns the indegree of a node */
    public int indegree(int l) {
        return backwardGraph.outdegree(l);
    }

    /** Returns a lazy iterator over the successors of a given node. */
    @Override
    public LazyIntIterator successors(int nodeId) {
        return forwardGraph.successors(nodeId);
    }

    /** Returns a lazy iterator over the predecessors of a given node. */
    public LazyIntIterator predecessors(int nodeId) {
        return backwardGraph.successors(nodeId);
    }

    /** Returns a reference to an array containing the predecessors of a given node. */
    public int[] predecessorArray(int x) {
        return backwardGraph.successorArray(x);
    }

    /** Returns an iterator enumerating the indegrees of the nodes of this graph. */
    public IntIterator indegrees() {
        return backwardGraph.outdegrees();
    }

    /** Returns the underlying ImmutableGraph in the forward direction. */
    public ImmutableGraph getForwardGraph() {
        return forwardGraph;
    }

    /** Returns the underlying ImmutableGraph in the backward direction. */
    public ImmutableGraph getBackwardGraph() {
        return backwardGraph;
    }
}
