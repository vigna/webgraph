/*
 * Copyright (C) 2006-2021 Sebastiano Vigna
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

import java.util.ConcurrentModificationException;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.lang.MutableString;

/** A very simple mutable graph class based on {@link it.unimi.dsi.fastutil.ints.IntArrayList}s.
 *
 * <p>When creating examples for test cases or everyday usage, this class offers practical constructors.
 * For instance, a 3-cycle is easily built as
 * <pre>
 *     new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 } })
 * </pre>
 *
 * <p>Moreover, methods like {@link #addNodes(int)} and {@link #addArc(int, int)} allow to change
 * the graph structure after construction, and several static factory methods provides ready-made
 * common graphs (see, e.g., {@link #newCompleteBinaryIntree(int)}).
 *
 * <p>A mutable graph is <em>not</em> an {@link it.unimi.dsi.webgraph.ImmutableGraph}. However,
 * it is possible to obtain an {@linkplain #immutableView() immutable view} of a mutable graph.
 * The view is valid until the exposed mutable graph is modified. A modification counter is used
 * to cause a <em>fail-fast</em> behaviour in case the immutable view is used after modifications.
 *
 * <p><strong>Warning</strong>: obtaining a {@link it.unimi.dsi.webgraph.NodeIterator} and using it
 * while modifying the graph will lead to unpredictable results.
 */

public class ArrayListMutableGraph {
	/** Current number of nodes. */
	protected int n;
	/** Current number of arcs. */
	protected long m;
	/** Current list of successor lists. The backing array might be longer than {@link #n}. */
	protected IntArrayList successors[];

	private final static IntArrayList[] EMPTY_INTARRAYLIST_ARRAY = {};

	/** Guarantees that a node index is valid.
	 *
	 * @param x a node index.
	 */
	protected void ensureNode(final int x) {
		if (x < 0) throw new IllegalArgumentException("Illegal node index " + x);
		if (x >= n) throw new IllegalArgumentException("Node index " + x + " is larger than graph order (" + n + ")");
	}

	/** Creates a new empty mutable graph. */
	public ArrayListMutableGraph() {
		successors = EMPTY_INTARRAYLIST_ARRAY;
	}

	/** Creates a new disconnected mutable graph with specified number of nodes.
	 * @param numNodes the number of nodes in the graph.
	 */
	public ArrayListMutableGraph(final int numNodes) {
		n = numNodes;
		successors = new IntArrayList[n];
		for(int i = n; i-- != 0;) successors[i] = new IntArrayList();
	}

	/** Creates a new mutable graph using a given number of nodes and a given list of arcs.
	 *
	 * @param numNodes the number of nodes in the graph.
	 * @param arc an array of arrays of length 2, specifying the arcs; no sanity checks are performed..
	 */
	public ArrayListMutableGraph(final int numNodes, final int[][] arc) {
		this(numNodes);
		m = arc.length;
		// Sanitize
		for(int i = arc.length; i-- != 0;) {
			if (arc[i].length != 2) throw new IllegalArgumentException("The arc of index " + i + " has length " + arc[i].length);
			if (arc[i][0] < 0 || arc[i][1] < 0 || arc[i][0] >= numNodes || arc[i][1] >= numNodes) throw new IllegalArgumentException("The arc of index " + i + " (" + arc[i][0] + ", " + arc[i][1] + ") is illegal");
		}
		for (final int[] element : arc) successors[element[0]].add(element[1]);
	}

	/** Creates a new mutable graph copying a given immutable graph.
	 *
	 * <p>This method will not invoke {@link ImmutableGraph#numNodes()}, but rather just create a {@link NodeIterator} and exhaust it.
	 *
	 * @param g an immutable graph.
	 */
	public ArrayListMutableGraph(final ImmutableGraph g) {
		this();
		int d, s = -1;
		long numArcs = 0;
		for(final NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
			s = nodeIterator.nextInt();
			d = nodeIterator.outdegree();
			numArcs += d;
			successors = ObjectArrays.grow(successors, s + 1);
			successors[s] = new IntArrayList(nodeIterator.successorArray(), 0, d);
		}
		n = s + 1;
		m = numArcs;
	}

	/** Creates a new mutable graph using a given number of nodes and a given arc filter.
	 *
	 * @param numNodes the number of nodes in the graph.
	 * @param arcFilter an arc filter which will specify which arcs go into the graph.
	 */
	public ArrayListMutableGraph(final int numNodes, final Transform.ArcFilter arcFilter) {
		this(numNodes);
		for(int i = n; i-- != 0;) {
			for(int j = 0; j < n; j++)
				if (arcFilter.accept(i, j)) {
					successors[i].add(j);
					m++;
				}
		}
	}


	/** Returns a new mutable graph containing a directed cycle.
	 *
	 * @param numNodes the number of nodes in the cycle.
	 */
	public static ArrayListMutableGraph newDirectedCycle(final int numNodes) {
		return new ArrayListMutableGraph(numNodes, (i, j) -> (i + 1) % numNodes == j);
	}

	/** Returns a new mutable graph containing a bidirectional cycle.
	 *
	 * @param numNodes the number of nodes in the cycle.
	 */
	public static ArrayListMutableGraph newBidirectionalCycle(final int numNodes) {
		return new ArrayListMutableGraph(numNodes, (i, j) -> (i + 1) % numNodes == j || (j + 1) % numNodes == i);
	}

	/** Returns a new mutable graph containing a complete graph.
	 *
	 * @param numNodes the number of nodes in the graph.
	 * @param loops true if you want loops, too.
	 */
	public static ArrayListMutableGraph newCompleteGraph(final int numNodes, final boolean loops) {
		return new ArrayListMutableGraph(numNodes, (i, j) -> i != j || loops);
	}

	/** Returns a new mutable graph containing a complete binary in-tree of given height.
	 *
	 * <strong>Warning</strong>: starting from version 1.7, the spurious loop
	 * at the root has been removed.
	 *
	 * @param height the height of the tree (0 for the root only).
	 */
	public static ArrayListMutableGraph newCompleteBinaryIntree(final int height) {
		return new ArrayListMutableGraph((1 << (height + 1)) - 1, (i, j) -> i != j && (i - 1) / 2 == j);
	}

	/** Returns a new mutable graph containing a complete binary out-tree of given height.
	 *
	 * <strong>Warning</strong>: starting from version 1.7, the spurious loop
	 * at the root has been removed.
	 *
	 * @param height the height of the tree (0 for the root only).
	 */
	public static ArrayListMutableGraph newCompleteBinaryOuttree(final int height) {
		return new ArrayListMutableGraph((1 << (height + 1)) - 1, (i, j) -> i != j && (j - 1) / 2 == i);
	}

	private static class ImmutableView extends ImmutableGraph {
		/** Cached number of nodes. */
		private final int n;
		/** Cached number of arcs. */
		private final long m;
		/** Cached successors. */
		private final IntArrayList[] successors;
		/** A reference to the mutable graph we expose. */
		private final ArrayListMutableGraph g;

		public ImmutableView(final ArrayListMutableGraph g) {
			this.g = g;
			this.n = g.n;
			this.m = g.m;
			this.successors = g.successors;
		}
		@Override
		public ImmutableView copy() { return this; }
		private void ensureUnmodified() { if (g.modificationCount != g.lastModificationCount) throw new ConcurrentModificationException(); }
		@Override
		public int numNodes() { ensureUnmodified(); return n; }
		@Override
		public int outdegree(final int x) { ensureUnmodified(); return successors[x].size(); }
		@Override
		public long numArcs() { ensureUnmodified(); return m; }
		@Override
		public boolean randomAccess() { return true; }
		@Override
		public int[] successorArray(final int x) { ensureUnmodified(); return successors[x].toIntArray(); }
		@Override
		public LazyIntIterator successors(final int x) { ensureUnmodified(); return LazyIntIterators.lazy(successors[x].iterator()); }
	}

	/** A cached copy of the immutable view, if it has ever been requested. */
	protected ImmutableView immutableView;
	/** The current modification count. */
	protected int modificationCount = 0;
	/** The modification count at the last call to {@link #immutableView()}. */
	protected int lastModificationCount = -1;

	/** Returns an immutable view of this mutable graph.
	 *
	 * <P>The view can be used until this mutable graph is modified. Attempt to use
	 * the view after modifying this mutable graph will cause a {@link ConcurrentModificationException}.
	 * After modification, a new call to this method will return a new immutable view.
	 *
	 * @return an immutable view of this mutable graph.
	 */
	public ImmutableGraph immutableView() {
		if (modificationCount != lastModificationCount) {
			for(int i = n; i-- != 0;) IntArrays.quickSort(successors[i].elements(), 0, successors[i].size());
			immutableView = new ImmutableView(this);
		}
		lastModificationCount = modificationCount;
		return immutableView;
	}

	public int numNodes() {
		return n;
	}

	public int outdegree(final int x) {
		ensureNode(x);
		return successors[x].size();
	}

	public long numArcs() {
		return m;
	}

	public int[] successorArray(final int x) {
		ensureNode(x);
		return successors[x].toIntArray();
	}

	public IntIterator successors(final int x) {
		ensureNode(x);
		return successors[x].iterator();
	}

	/** Adds the given number of nodes, numbering them from {@link #numNodes()} onwards. The new nodes have no successors.
	 *
	 * @param numNewNodes the number of new nodes.
	 */
	public void addNodes(final int numNewNodes) {
		if (numNewNodes != 0) {
			modificationCount++;
			final int newN = n + numNewNodes;
			successors = ObjectArrays.ensureCapacity(successors, newN, n);
			while(n < newN) successors[n++] = new IntArrayList();
		}
	}

	/** Removes the given node. All arcs incident on the node are removed, too.
	 *
	 * @param x the node to be removed.
	 */
	public void removeNode(final int x) {
		ensureNode(x);
		modificationCount++;
		System.arraycopy(successors, x + 1, successors, x, --n - x);
		int t;
		for(int i = n; i-- != 0;)
			for(int j = successors[i].size(); j-- != 0;) {
				t = successors[i].getInt(j);
				if (t == x) successors[i].removeInt(j);
				else if (t > x) successors[i].set(j, t - 1);
			}
	}

	/** Adds the given arc.
	 *
	 * @param x the start of the arc.
	 * @param y the end of the arc.
	 */
	public void addArc(final int x, final int y) {
		ensureNode(x);
		ensureNode(y);
		if (successors[x].indexOf(y) != -1) throw new IllegalArgumentException("Node " + y + " is already a successor of node " + x);
		modificationCount++;
		successors[x].add(y);
		m++;
	}

	/** Removes the given arc.
	 *
	 * @param x the start of the arc.
	 * @param y the end of the arc.
	 */
	public void removeArc(final int x, final int y) {
		ensureNode(x);
		ensureNode(y);
		final int pos = successors[x].indexOf(y);
		if (pos == -1) throw new IllegalArgumentException("Node " + y + " is not a successor of node " + x);
		modificationCount++;
		successors[x].removeInt(pos);
		m--;
	}

	/** Compare this mutable graph to another object.
	 *
	 * @return true iff the given object is a mutable graph the same size, and
	 * the successor list of every node of this graph is equal to the successor list of the corresponding node of <code>o</code>.
	 */

	@Override
	public boolean equals(final Object o) {
		if (! (o instanceof ArrayListMutableGraph)) return false;
		final ArrayListMutableGraph g = (ArrayListMutableGraph) o;
		int n = numNodes();
		if (n != g.numNodes()) return false;
		int[] s, t;
		int d;
		while(n-- != 0) {
			if ((d = outdegree(n)) != g.outdegree(n)) return false;
			s = successorArray(n);
			t = g.successorArray(n);
			while(d-- != 0) if (s[d] != t[d]) return false;
		}

		return true;
	}

	/** Returns a hash code for this mutable graph.
	 *
	 * @return a hash code for this mutable graph.
	 */

	@Override
	public int hashCode() {
		final int n = numNodes();
		int h = -1;
		int[] s;
		int d;
		for(int i = 0; i < n; i++) {
			h = h * 31 + i;
			s = successorArray(i);
			d = outdegree(i);
			while(d-- != 0) h = h * 31 + s[d];
		}

		return h;
	}

	@Override
	public String toString() {
		final MutableString ms = new MutableString();
		IntIterator ii;

		ms.append("Nodes: " + numNodes() + "\nArcs: " + numArcs() + "\n");
		for (int i = 0; i < numNodes(); i++) {
			ms.append("Successors of " + i + " (degree " + outdegree(i) + "):");
			ii = successors(i);
			while (ii.hasNext())
				ms.append(" " + ii.nextInt());
			ms.append("\n");
		}
		return ms.toString();
	}

}
