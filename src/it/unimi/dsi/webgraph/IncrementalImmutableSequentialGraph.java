/*
 * Copyright (C) 2013-2021 Sebastiano Vigna
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
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;


/** An adapter exposing an {@link ImmutableGraph} that can be filled incrementally using
 * a family of {@linkplain #add(int[], int, int) addition methods} that make it possible to specify
 * the list of successors of each node in increasing order. At the end of the process, the user
 * must add the special marker list {@link #END_OF_GRAPH}.
 *
 * <p>The class provides a single
 * call to {@link #nodeIterator()}: once the returned {@link NodeIterator} has been exhausted, {@link #numNodes()} will return the number of nodes,
 * which will be equal to the number of calls to addition methods.
 *
 * <p>The class works using a producer/consumer patten: in a typical usage, the thread invoking the
 * addition method will be different from the thread performing the traversal, as in
 * <pre class= code>
 *	final IncrementalImmutableSequentialGraph g = new IncrementalImmutableSequentialGraph();
 *	ExecutorService executor = Executors.newSingleThreadExecutor();
 *	final Future&lt;Void&gt; future = executor.submit(new Callable&lt;Void&gt;() {
 *		public Void call() throws IOException {
 *			BVGraph.store(g, basename);
 *			return null;
 *		}
 *	});
 *
 *	// Do one add() for each node, to specify the successors
 *
 *	g.add(IncrementalImmutableSequentialGraph.END_OF_GRAPH);
 *	future.get();
 *	executor.shutdown();
 *</pre>
 */

public class IncrementalImmutableSequentialGraph extends ImmutableSequentialGraph {
	/** A marker for the end of the graph. */
	public static int[] END_OF_GRAPH = new int[0];

	/** The number of nodes (known after a traversal). */
	private int n;
	/** The queue connecting the add methods and node iterator successor mehotds. */
	private final ArrayBlockingQueue<int[]> successorQueue;

	public IncrementalImmutableSequentialGraph() {
		n = -1;
		this.successorQueue = new ArrayBlockingQueue<>(100);
	}

	@Override
	public int numNodes() {
		if (n == -1) throw new UnsupportedOperationException("The number of nodes is unknown (you need to complete a traversal)");
		return n;
	}

	@Override
	public NodeIterator nodeIterator() {
		if (n != -1) throw new IllegalStateException();
		return new NodeIterator() {
			int i = 0;
			private int[] currentSuccessor;
			private int[] nextSuccessor;
			@Override
			public boolean hasNext() {
				if (nextSuccessor == END_OF_GRAPH) return false;
				if (nextSuccessor != null) return true;

				try {
					nextSuccessor = successorQueue.take();
				}
				catch (final InterruptedException e) {
					throw new RuntimeException(e.getMessage(), e);
				}

				final boolean end = nextSuccessor == END_OF_GRAPH;
				if (end) n = i;
				return ! end;
			}

			@Override
			public int nextInt() {
				if (! hasNext()) throw new NoSuchElementException();
				currentSuccessor = nextSuccessor;
				nextSuccessor = null;
				return i++;
			}

			@Override
			public int outdegree() {
				if (currentSuccessor == null) throw new IllegalStateException();
				return currentSuccessor.length;
			}

			@Override
			public int[] successorArray() {
				if (currentSuccessor == null) throw new IllegalStateException();
				return currentSuccessor;
			}

			@Override
			public NodeIterator copy(final int k) {
				throw new UnsupportedOperationException();
			}
		};
	}

	/** Adds a new node having as successors contained in the specified array fragment.
	 *
	 *
	 * <p>The array must be sorted in increasing order.
	 *
	 * @param successor an array.
	 * @param offset the first valid entry in <code>successor</code>.
	 * @param length the number of valid entries.
	 */
	public void add(final int[] successor, final int offset, final int length) throws InterruptedException {
		successorQueue.put(Arrays.copyOfRange(successor, offset, offset + length));
	}

	/** Adds a new node having as successors contained in the specified array.
	 *
	 * <p>The array must be sorted in increasing order.
	 *
	 * @param successor an array.
	 */
	public void add(final int[] successor) throws InterruptedException {
		successorQueue.put(successor);
	}
}
