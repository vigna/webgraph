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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

/** Performs breadth-firsts visits of a graph exploiting multicore parallelism.
 *
 * <p>To use this class you first create an instance, and then invoke {@link #visit(int)}. If you want perform
 * more visits preserving the {@link #marker} state you can invoke {@link #visit(int)} again.
 * By calling {@link #clear()}, instead, you can reset {@link #marker} (i.e., forget about visited nodes).
 *
 * <p>Alternatively, {@link #visitAll()} will start a visit from all the nodes of the graph in a more efficient way.
 *
 * <p>After the visit, you can peek at the field {@link #marker} to discover details about the visit.
 * Depending on the {@link #parent} value provided at construction time, the array {@link #marker}
 * will be filled with parent information (e.g., with the index
 * of the parent node in the visit tree) or with a <em>{@linkplain #round round number}</em> increased at each nonempty visit,
 * which act as a connected-component index if the graph is symmetric.
 *
 * <p>Observe that in the former case (if {@link #parent} is <code>true</code>), the array {@link #marker} will
 * contain the value -1 for the nodes that have not been reached by the visit, the parent of the node in the BFS tree
 * if the node was not the root, or the node itself for the root.
 *
 * <p>In the case of {@link #visit(int, int)}, {@link #queue} and {@link #cutPoints}, too, provide useful information. In
 * particular, the nodes in {@link #queue} from the <var>d</var>-th to the (<var>d</var>&nbsp;+1)-th cutpoint
 * are exactly the nodes at distance <var>d</var> from the source.
 *
 * <h2>Performance issues</h2>
 *
 * <p>This class needs three integers per node.
 * If there are several available cores, breadth-first visits will be <em>decomposed</em> into relatively
 * small tasks (small blocks of nodes in the queue at the same distance from the starting node)
 * and each task will be assigned to the first available core. Since all tasks are completely
 * independent, this ensures a very high degree of parallelism. However, on very sparse graphs the cost
 * of keeping the threads synchronised can be extremely high, and even end up <em>increasing</em> the visit time.
 *
 * <p>Note that if the degree distribution is extremely skewed some cores might get stuck
 * in the enumeration of the successors of some nodes with a very high degree.
 */

public class ParallelBreadthFirstVisit {
	/** The graph under examination. */
	public final ImmutableGraph graph;
	/** The queue of visited nodes. */
	public final IntArrayList queue;
	/** At the end of a visit, the cutpoints of {@link #queue}. The <var>d</var>-th cutpoint is the first node in the queue at distance <var>d</var>. The
	 * last cutpoint is the queue size. */
	public final IntArrayList cutPoints;
	/** Whether {@link #marker} contains parent nodes or round numbers. */
	public final boolean parent;
	/** The marker array; contains -1 for nodes that have not still been enqueued, the parent of the visit tree if
	 * {@link #parent} is true, or an index increased at each visit if {@link #parent} is false, which in the symmetric case is the index
	 * of the connected component of the node. */
	public final AtomicIntegerArray marker;
	/** The global progress logger. */
	private final ProgressLogger pl;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The number of nodes visited. */
	private final AtomicInteger progress;
	/** The next node position to be picked from the last segment of {@link #queue}. */
	private final AtomicLong nextPosition;
	/** If true, the current visit is over. */
	private volatile boolean completed;
	/** The barrier used to synchronize visiting threads. */
	private volatile CyclicBarrier barrier;
	/** Keeps track of problems in visiting threads. */
	private volatile Throwable threadThrowable;
	/** A number increased at each nonempty visit (used to mark {@link #marker} if {@link #parent} is false). */
	public int round;

	/** Creates a new class for keeping track of the state of parallel breadth-first visits.
	 *
	 * @param graph a graph.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param parent if true, {@link #marker} will contain parent nodes; otherwise, it will contain {@linkplain #round round numbers}.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public ParallelBreadthFirstVisit(final ImmutableGraph graph, final int requestedThreads, final boolean parent, final ProgressLogger pl) {
		this.graph = graph;
		this.parent = parent;
		this.pl = pl;
		this.marker = new AtomicIntegerArray(graph.numNodes());
		this.queue = new IntArrayList(graph.numNodes());
		this.progress = new AtomicInteger();
		this.nextPosition = new AtomicLong();
		this.cutPoints = new IntArrayList();
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
		clear();
	}

	/** Clears the internal state of the visit, setting all {@link #marker} entries and {@link #round} to -1. */
	public void clear() {
		round = -1;
		for(int i = marker.length(); i-- != 0;) marker.set(i, -1);
	}

	private final class IterationThread extends Thread {
		private static final int GRANULARITY = 1000;

		@Override
		public void run() {
			try {
				// We cache frequently used fields.
				final AtomicIntegerArray marker = ParallelBreadthFirstVisit.this.marker;
				final ImmutableGraph graph = ParallelBreadthFirstVisit.this.graph.copy();
				final boolean parent = ParallelBreadthFirstVisit.this.parent;

				for(;;) {
					barrier.await();
					if (completed) return;
					final IntArrayList out = new IntArrayList();
					final int first = cutPoints.getInt(cutPoints.size() - 2);
					final int last = cutPoints.getInt(cutPoints.size() - 1);
					int mark = round;
					for(;;) {
						// Try to get another piece of work.
						final long start = first + nextPosition.getAndAdd(GRANULARITY);
						if (start >= last) {
							nextPosition.getAndAdd(-GRANULARITY);
							break;
						}

						final int end = (int)(Math.min(last, start + GRANULARITY));
						out.clear();

						for(int pos = (int)start; pos < end; pos++) {
							final int curr = queue.getInt(pos);
							if (parent == true) mark = curr;
							final LazyIntIterator successors = graph.successors(curr);
							for(int s; (s = successors.nextInt()) != -1;)
								if (marker.compareAndSet(s, -1, mark)) out.add(s);
						}

						progress.addAndGet(end - (int)start);

						if (! out.isEmpty()) synchronized(queue) {
							queue.addAll(out);
						}
					}
				}
			}
			catch(final Throwable t) {
				threadThrowable = t;
			}
		}
	}


	/** Performs a breadth-first visit of the given graph starting from the given node.
	 *
	 * <p>This method will increment {@link #round}.
	 *
	 * @param start the starting node.
	 * @return the number of visited nodes.
	 * @see #visit(int,int)
	 */
	public int visit(final int start) {
		return visit(start, -1);
	}


	/** Performs a breadth-first visit of the given graph starting from the given node.
	 *
	 * <p>This method will increment {@link #round} if at least one node is visited.
	 *
	 * @param start the starting node.
	 * @param expectedSize the expected size (number of nodes) of the visit (for logging), or -1 to use the number of nodes of the graph.
	 * @return the number of visited nodes.
	 */
	public int visit(final int start, final int expectedSize) {
		if (marker.get(start) != -1) return 0;
		round++;
		completed = false;
		queue.clear();
		cutPoints.clear();
		queue.add(start);
		cutPoints.add(0);
		marker.set(start, parent ? start : round);
		final IterationThread[] thread = new IterationThread[numberOfThreads];
		for(int i = thread.length; i-- != 0;) thread[i] = new IterationThread();
		progress.set(0);

		if (pl != null) {
			pl.start("Starting visit...");
			pl.expectedUpdates = expectedSize != -1 ? expectedSize : graph.numNodes();
			pl.itemsName = "nodes";
		}

		barrier = new CyclicBarrier(numberOfThreads, () -> {
			if (pl != null) pl.set(progress.get());

			if (queue.size() == cutPoints.getInt(cutPoints.size() - 1)) {
				completed = true;
				return;
			}

			cutPoints.add(queue.size());
			nextPosition.set(0);
		}
		);

		for(int i = thread.length; i-- != 0;) thread[i].start();
		for(int i = thread.length; i-- != 0;)
			try {
				thread[i].join();
			}
			catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}

		if (threadThrowable != null) throw new RuntimeException(threadThrowable);
		if (pl != null) pl.done();
		return queue.size();
	}

	/** Visits all nodes. Calls {@link #clear()} initially.
	 *
	 * <p>This method is more efficient than invoking {@link #visit(int, int)} on all nodes as threads are created just once.
	 */
	public void visitAll() {
		final IterationThread[] thread = new IterationThread[numberOfThreads];
		for(int i = thread.length; i-- != 0;) thread[i] = new IterationThread();
		final int n = graph.numNodes();
		completed = false;
		clear();
		queue.clear();
		cutPoints.clear();
		progress.set(0);

		if (pl != null) {
			pl.start("Starting visits...");
			pl.expectedUpdates = graph.numNodes();
			pl.displayLocalSpeed = true;
			pl.itemsName = "nodes";
		}

		barrier = new CyclicBarrier(numberOfThreads, new Runnable() {
			int curr = -1;
			@Override
			public void run() {
				if (pl != null) pl.set(progress.get());
				// Either first call, or queue did not grow from the last call.
				if (curr == -1 || queue.size() == cutPoints.getInt(cutPoints.size() - 1)) {
					if (pl != null) pl.set(progress.get());
					// Look for the first nonterminal node not yet visited.
					for(;;) {
						while(++curr < n && marker.get(curr) != -1);

						if (curr == n) {
							completed = true;
							return;
						}
						else {
							round++;
							marker.set(curr, parent ? curr : round);

							final int d = graph.outdegree(curr);
							if (d != 0 && ! (d == 1 && graph.successors(curr).nextInt() == curr)) {
								queue.clear();
								queue.add(curr);

								cutPoints.clear();
								cutPoints.add(0);
								break;
							}
						}
					}
				}

				cutPoints.add(queue.size());
				nextPosition.set(0);
			}
		}
		);

		for(int i = thread.length; i-- != 0;) thread[i].start();
		for(int i = thread.length; i-- != 0;)
			try {
				thread[i].join();
			}
			catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}

		if (threadThrowable != null) throw new RuntimeException(threadThrowable);
		if (pl != null) pl.done();
	}


	/** Returns a node at maximum distance during the last visit (e.g., a node realising the positive eccentricity of the starting node).
	 *
	 * @return the maximum distance computed during the last visit.
	 */
	public int nodeAtMaxDistance() {
		return queue.getInt(queue.size() - 1);
	}

	/** Returns the maximum distance computed during the last visit (e.g., the eccentricity of the source).
	 *
	 * @return the maximum distance computed during the last visit.
	 */

	public int maxDistance() {
		return cutPoints.size() - 2;
	}
}
