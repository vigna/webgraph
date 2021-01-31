package it.unimi.dsi.webgraph.scratch;

/*
 * Copyright (C) 2012-2021 Paolo Boldi and Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.algo.ParallelBreadthFirstVisit;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

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

/** Computes the betweenness centrality using a parallel implementation of Brandes's algorithm
 * (Ulrik Brandes, &ldquo;A Faster Algorithm for Betweenness Centrality&rdquo;, <i>Journal of Mathematical Sociology</i> 25(2):163&minus;177, 2001).
 *
 * <p>To use this class you first create an instance, and then invoke {@link #compute()}.
 * After that, you can peek at the field {@link #betweenness} to discover the betweenness of each node.
 *
 * <p>For every three distinct nodes <var>s</var>, <var>t</var> and <var>v</var>, let <var>&sigma;</var><sub><var>s</var><var>t</var></sub> be
 * the number of shortest paths from <var>s</var> to <var>t</var>, and <var>&sigma;</var><sub><var>s</var><var>t</var></sub>(<var>v</var>) the
 * number of such paths on which <var>v</var> lies. The betweenness centrality of node <var>v</var> is defined to be the sum of
 * <var>&delta;</var><sub><var>s</var><var>t</var></sub>(<var>v</var>)=<var>&sigma;</var><sub><var>s</var><var>t</var></sub>(<var>v</var>) / <var>&sigma;</var><sub><var>s</var><var>t</var></sub> over all
 * pairs of distinct nodes <var>s</var>, <var>t</var> different from <var>v</var> (the summand is assumed to be zero whenever the denominator
 * is zero).
 *
 * <p>Brandes's approach consists in performing a breadth-first visit from every node, recording (in {@link #distance}) the
 * distance of the node from the current source. After each visit, nodes are considered in decreasing order of
 * distance, and for each of them we consider the arcs (<var>v</var>,<var>w</var>) such that the distance of <var>w</var>
 * is exactly one plus the distance of <var>v</var>: in this case we say that <var>v</var> is a parent of <var>w</var>.
 * Such parents are used to compute the values of <var>&delta;</var> (exactly as in the original algorithm, but without
 * any need to keep an explicit set of parents).
 *
 * <p>For more information about the way the visit is implemented in parallel, we refer the reader to {@link ParallelBreadthFirstVisit}.
 *
 * <h2>Performance issues</h2>
 *
 * <p>This class needs two integers and two doubles per node.
 * If there are several available cores, breadth-first visits will be <em>decomposed</em> into relatively
 * small tasks (small blocks of nodes in the queue at the same distance from the starting node)
 * and each task will be assigned to the first available core. Since all tasks are completely
 * independent, this ensures a very high degree of parallelism. However, on very sparse graphs the cost
 * of keeping the threads synchronised can be extremely high, and even end up <em>increasing</em> the visit time.
 *
 * <p>Note that if the degree distribution is extremely skewed some cores might get stuck
 * in the enumeration of the successors of some nodes with a very high degree.
 */

public class ParallelBreadthFirstBetweennessCentrality {
	private final static Logger LOGGER = LoggerFactory.getLogger(ParallelBreadthFirstBetweennessCentrality.class);

	/** An exception telling that the path count exceeded 64-bit integer arithmetic. */
	public static final class PathCountOverflowException extends RuntimeException {
		public PathCountOverflowException() {}

		public PathCountOverflowException(String s) {
			super(s);
		}

		private static final long serialVersionUID = 1L;
	}

	/** The graph under examination. */
	public final ImmutableGraph graph;
	/** The queue of visited nodes. */
	public final IntArrayList queue;
	/** At the end of a visit, the cutpoints of {@link #queue}. The <var>d</var>-th cutpoint is the first node in the queue at distance <var>d</var>. The
	 * last cutpoint is the queue size. */
	public final IntArrayList cutPoints;
	/** The array containing the distance of each node from the current source (or -1 if the node has not yet been reached by the visit). */
	public final AtomicIntegerArray distance;
	/** The array containing the values of &sigma; incremented for each parent/child pair during each visit, as explained in Brandes's algorithm. */
	public final AtomicLongArray sigma;
	/** The array of dependencies (computed at the end of each visit). */
	public final double[] delta;
	/** The array of betweenness value. */
	public final double[] betweenness;
	/** The global progress logger. */
	private final ProgressLogger pl;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node position to be picked from the last segment of {@link #queue}. */
	private final AtomicLong nextPosition;
	/** If true, the current visit is over. */
	private volatile boolean completed;
	/** The barrier used to synchronize visiting threads. */
	private volatile CyclicBarrier barrier;
	/** Possible states: whether threads are visiting the graph, making the final computations or clearing the variables. */
	private static enum ThreadState {
		VISIT, LAST_STAGE, CLEAR
	};
	/** The current state of the threads. */
	private volatile ThreadState threadState;
	/** If the current thread state is {@link ThreadState#LAST_STAGE}, the current block to be considered. */
	private volatile int currentBlockForLastStage;
	/** Whether to stop abruptly the visiting process. */
	protected volatile boolean stop;

	/** Creates a new class for keeping track of the state of parallel breadth-first visits.
	 *
	 * @param graph a graph.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param pl a progress logger, or {@code null}.
	 */
	public ParallelBreadthFirstBetweennessCentrality(final ImmutableGraph graph, final int requestedThreads, final ProgressLogger pl) {
		this.graph = graph;
		this.pl = pl;
		this.distance = new AtomicIntegerArray(graph.numNodes());
		this.sigma = new AtomicLongArray(graph.numNodes());
		this.delta = new double[graph.numNodes()];
		this.betweenness = new double[graph.numNodes()];
		this.queue = new IntArrayList(graph.numNodes());
		this.nextPosition = new AtomicLong();
		this.cutPoints = new IntArrayList();
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
	}

	/** Creates a new class for keeping track of the state of parallel breadth-first visits, using as many threads as
	 *  the number of available processors.
	 *
	 * @param graph a graph.
	 * @param pl a progress logger, or {@code null}.
	 */
	public ParallelBreadthFirstBetweennessCentrality(final ImmutableGraph graph, final ProgressLogger pl) {
		this(graph, 0, pl);
	}

	/** Creates a new class for keeping track of the state of parallel breadth-first visits.
	 *
	 * @param graph a graph.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 */
	public ParallelBreadthFirstBetweennessCentrality(final ImmutableGraph graph, final int requestedThreads) {
		this(graph, requestedThreads, null);
	}

	/** Creates a new class for keeping track of the state of parallel breadth-first visits, using as many threads as
	 *  the number of available processors.
	 *
	 * @param graph a graph.
	 */
	public ParallelBreadthFirstBetweennessCentrality(final ImmutableGraph graph) {
		this(graph, 0);
	}

	private final class IterationThread implements Callable<Void> {

		private boolean checkOverflow(final int curr, final long currSigma, int s) {
			if (sigma.get(s) > Long.MAX_VALUE - currSigma) throw new PathCountOverflowException(sigma.get(s) + " > " + (Long.MAX_VALUE - currSigma) + " (" + curr + " -> " + s + ")");
			return true;
		}

		@Override
		public Void call() throws InterruptedException, BrokenBarrierException {
			// We cache frequently used fields.
			final AtomicIntegerArray distance = ParallelBreadthFirstBetweennessCentrality.this.distance;
			final ImmutableGraph graph = ParallelBreadthFirstBetweennessCentrality.this.graph.copy();

			for(;;) {
				if (stop) return null;
				barrier.await();
				if (completed) return null;
				final IntArrayList out = new IntArrayList();
				int first, last, currentDistance;
				switch (threadState) {
				case VISIT:
					first = cutPoints.getInt(cutPoints.size() - 2);
					last = cutPoints.getInt(cutPoints.size() - 1);
					currentDistance = cutPoints.size() - 2;
					break;
				case LAST_STAGE:
					first = cutPoints.getInt(currentBlockForLastStage);
					last = cutPoints.getInt(currentBlockForLastStage + 1);
					currentDistance = currentBlockForLastStage;
					break;
				case CLEAR:
					first = 0;
					last = queue.size();
					currentDistance = -1; // Distance does not apply
					break;
				default:
					first = last = 0; // Should never happen
					throw new IllegalStateException();
				}

				//System.err.println(cutPoints.getInt(cutPoints.size() - 1) -  cutPoints.getInt(cutPoints.size() - 2));

				final int granularity = 1000;

				for(;;) {
					// Try to get another piece of work.
					final long start = first + nextPosition.getAndAdd(granularity);
					if (start >= last) {
						nextPosition.getAndAdd(-granularity);
						break;
					}

					final int end = (int)(Math.min(last, start + granularity));
					if (threadState == ThreadState.VISIT) out.clear();
					int node;
					boolean overflow = false;

					switch(threadState) {
					case VISIT:
						for(int pos = (int)start; pos < end; pos++) {
							final int curr = queue.getInt(pos);
							final long currSigma = sigma.get(curr);
							final LazyIntIterator successors = graph.successors(curr);
							for(int s; (s = successors.nextInt()) != -1;) {
								if (distance.compareAndSet(s, -1, currentDistance + 1)) {
									out.add(s);
									assert checkOverflow(curr, currSigma, s);
									overflow |= sigma.get(s) > Long.MAX_VALUE - currSigma;
									sigma.addAndGet(s, currSigma);
								}
								else if (distance.get(s) == currentDistance + 1) {
									assert checkOverflow(curr, currSigma, s);
									overflow |= sigma.get(s) > Long.MAX_VALUE - currSigma;
									sigma.addAndGet(s, currSigma);
								}
							}
						}

						if (overflow) throw new PathCountOverflowException();
						break;
					case LAST_STAGE:
						for(int pos = (int)start; pos < end; pos++) {
							node = queue.getInt(pos);
							delta[node] = 0;
							// Update delta's for all blocks but the last
							if (currentDistance < cutPoints.size() - 2) {
								double sigmaNode = sigma.get(node);
								final LazyIntIterator succ = graph.successors(node);
								for(int s; (s = succ.nextInt()) != -1;)
									if (distance.get(s) == currentDistance + 1) delta[node] += (1 + delta[s]) * sigmaNode / sigma.get(s);
							}
							// Update betweenness for all blocks but the first
							if (currentDistance > 0) betweenness[node] += delta[node];
						}
						break;
					case CLEAR:
						for(int pos = (int)start; pos < end; pos++) {
							node = queue.getInt(pos);
							distance.set(node, -1);
							sigma.set(node, 0);
						}
					}

					if (threadState == ThreadState.VISIT && ! out.isEmpty())
						synchronized(queue) {
							queue.addAll(out);
						}
				}
			}
		}
	}


	/** Performs a full computation. */
	public void compute() throws InterruptedException {
		final IterationThread[] thread = new IterationThread[numberOfThreads];
		for(int i = thread.length; i-- != 0;) thread[i] = new IterationThread();
		final int n = graph.numNodes();
		completed = false;
		threadState = ThreadState.VISIT;
		queue.clear();
		cutPoints.clear();
		for(int i = distance.length(); i-- != 0;) {
			distance.set(i, -1);
			sigma.set(i, 0);
		}

		if (pl != null) {
			pl.start("Starting visits...");
			pl.expectedUpdates = graph.numNodes();
			pl.itemsName = "nodes";
		}

		barrier = new CyclicBarrier(numberOfThreads, new Runnable() {
			int curr = -1;
			@Override
			public void run() {
				nextPosition.set(0);
				// Thread state transitions
				if (threadState != ThreadState.VISIT || (curr != -1 && queue.size() == cutPoints.getInt(cutPoints.size() - 1))) {
					switch (threadState) {
					case VISIT:
						threadState = ThreadState.LAST_STAGE;
						currentBlockForLastStage = cutPoints.size() - 2;
						return;
					case LAST_STAGE:
						if (currentBlockForLastStage > 0)
							currentBlockForLastStage--;
						else threadState = ThreadState.CLEAR;
						return;
					case CLEAR:
						if (curr == n - 1) {
							completed = true;
							return;
						}
						else threadState = ThreadState.VISIT;

					}
				}

				// Either first call, or queue did not grow from the last call.
				if (threadState == ThreadState.VISIT && (curr == -1 || queue.size() == cutPoints.getInt(cutPoints.size() - 1))) {
					// Look for the first non-sink node not visited yet.
					do {
						if (pl != null) pl.update();
						curr++;
					} while(curr < n && graph.outdegree(curr) == 0);

					if (curr == n) {
						completed = true;
						return;
					}

					queue.clear();
					queue.add(curr);

					cutPoints.clear();
					cutPoints.add(0);

					distance.set(curr, 0);
					sigma.set(curr, 1);
				}

				if (threadState == ThreadState.VISIT) cutPoints.add(queue.size());
			}
		}
		);

		final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		final ExecutorCompletionService<Void> executorCompletionService = new ExecutorCompletionService<>(executorService);

		for(int i = thread.length; i-- != 0;) executorCompletionService.submit(thread[i]);

		try {
			for(int i = thread.length; i-- != 0;) executorCompletionService.take().get();
		}
		catch(ExecutionException e) {
			stop = true;
			Throwable cause = e.getCause();
			throw cause instanceof RuntimeException ? (RuntimeException)cause : new RuntimeException(cause.getMessage(), cause);
		}
		finally {
			executorService.shutdown();
		}
		if (pl != null) pl.done();
	}


	public static void main(final String[] arg) throws IOException, JSAPException, InterruptedException {

		SimpleJSAP jsap = new SimpleJSAP(ParallelBreadthFirstBetweennessCentrality.class.getName(), "Computes the betweenness centrality a graph using a parallel implementation of Brandes's algorithm.",
			new Parameter[] {
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("rankFilename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final String graphBasename = jsapResult.getString("graphBasename");
		final String rankFilename = jsapResult.getString("rankFilename");
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");

		ImmutableGraph graph = mapped? ImmutableGraph.loadMapped(graphBasename, progressLogger) : ImmutableGraph.load(graphBasename, progressLogger);
		if (jsapResult.userSpecified("expand")) graph = new ArrayListMutableGraph(graph).immutableView();

		ParallelBreadthFirstBetweennessCentrality betweennessCentrality = new ParallelBreadthFirstBetweennessCentrality(graph, threads, progressLogger);
		betweennessCentrality.compute();

		BinIO.storeDoubles(betweennessCentrality.betweenness, rankFilename);
	}


}
