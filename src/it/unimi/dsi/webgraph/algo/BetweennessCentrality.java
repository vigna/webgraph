/*
 * Copyright (C) 2012-2023 Sebastiano Vigna
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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

/** Computes the betweenness centrality using an implementation of Brandes's algorithm
 * (Ulrik Brandes, &ldquo;A Faster Algorithm for Betweenness Centrality&rdquo;, <i>Journal of
 * Mathematical Sociology</i> 25(2):163&minus;177, 2001)
 * that uses multiple parallel breadth-first visits.
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
 * <p>Brandes's approach consists in performing a breadth-first visit from every node, recording the
 * distance of the node from the current source. After each visit, nodes are considered in decreasing order of
 * distance, and for each of them we consider the arcs (<var>v</var>,<var>w</var>) such that the distance of <var>w</var>
 * is exactly one plus the distance of <var>v</var>: in this case we say that <var>v</var> is a parent of <var>w</var>.
 * Such parents are used to compute the values of <var>&delta;</var> (exactly as in the original algorithm, but without
 * any need to keep an explicit set of parents, which is important since this class is memory intensive).
 *
 * <p>Every visit is independent and is carried out by a separate thread. The only contention point
 * is the update of the array accumulating the betweenness score, which is negligible. The downside is
 * that running on <var>k</var> cores requires approximately <var>k</var> times the memory of the
 * sequential algorithm, as only the graph and the betweenness array will be shared.
 *
 * <p>This class keeps carefully track of overflows in path counters, and will throw an exception in case they happen.
 * Thanks to David Gleich for making me note this serious problem, which is often overlooked.
 */

public class BetweennessCentrality {
	private final static Logger LOGGER = LoggerFactory.getLogger(BetweennessCentrality.class);

	/** An exception telling that the path count exceeded 64-bit integer arithmetic. */
	public static final class PathCountOverflowException extends RuntimeException {
		public PathCountOverflowException() {}

		public PathCountOverflowException(final String s) {
			super(s);
		}

		private static final long serialVersionUID = 1L;
	}

	/** The graph under examination. */
	private final ImmutableGraph graph;
	/** The global progress logger. */
	private final ProgressLogger pl;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node to be visited. */
	protected final AtomicInteger nextNode;
	/** Whether to stop abruptly the visiting process. */
	protected volatile boolean stop;
	/** The array of betweenness value. */
	public final double[] betweenness;

	/** Creates a new class for computing betweenness centrality.
	 *
	 * @param graph a graph.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param pl a progress logger, or {@code null}.
	 */
	public BetweennessCentrality(final ImmutableGraph graph, final int requestedThreads, final ProgressLogger pl) {
		this.pl = pl;
		this.graph = graph;
		this.betweenness = new double[graph.numNodes()];
		this.nextNode = new AtomicInteger();
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
	}

	/** Creates a new class for computing betweenness centrality, using as many threads as
	 *  the number of available processors.
	 *
	 * @param graph a graph.
	 * @param pl a progress logger, or {@code null}.
	 */
	public BetweennessCentrality(final ImmutableGraph graph, final ProgressLogger pl) {
		this(graph, 0, pl);
	}

	/** Creates a new class for computing betweenness centrality.
	 *
	 * @param graph a graph.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 */
	public BetweennessCentrality(final ImmutableGraph graph, final int requestedThreads) {
		this(graph, 1, null);
	}

	/** Creates a new class for computing betweenness centrality, using as many threads as
	 *  the number of available processors.
	 *
	 * @param graph a graph.
	 */
	public BetweennessCentrality(final ImmutableGraph graph) {
		this(graph, 0);
	}

	private final class IterationThread implements Callable<Void> {
		/** The queue of visited nodes. */
		private final IntArrayList queue;
		/** At the end of a visit, the cutpoints of {@link #queue}. The <var>d</var>-th cutpoint is the first node in the queue at distance <var>d</var>. The
		 * last cutpoint is the queue size. */
		private final IntArrayList cutPoints;
		/** The array containing the distance of each node from the current source (or -1 if the node has not yet been reached by the visit). */
		private final int[] distance;
 		/** The array containing the values of &sigma; incremented for each parent/child pair during each visit, as explained in Brandes's algorithm. */
		private final long[] sigma;
		/** The array of dependencies (computed at the end of each visit). */
		private final double[] delta;

		private IterationThread() {
			this.distance = new int[graph.numNodes()];
			this.sigma = new long[graph.numNodes()];
			this.delta = new double[graph.numNodes()];
			this.queue = new IntArrayList(graph.numNodes());
			this.cutPoints = new IntArrayList();
		}

		private boolean checkOverflow(final long[] sigma, final int node, final long currSigma, final int s) {
			if (sigma[s] > Long.MAX_VALUE - currSigma) throw new PathCountOverflowException(sigma[s] + " > " + (Long.MAX_VALUE - currSigma) + " (" + node + " -> " + s + ")");
			return true;
		}

		@Override
		public Void call() {
			// We cache frequently used fields.
			final int[] distance = this.distance;
			final double[] delta = this.delta;
			final long[] sigma = this.sigma;
			final IntArrayList queue = this.queue;
			final ImmutableGraph graph = BetweennessCentrality.this.graph.copy();

			for(;;) {
				final int curr = nextNode.getAndIncrement();
				if (BetweennessCentrality.this.stop || curr >= graph.numNodes()) return null;
				queue.clear();
				queue.add(curr);
				cutPoints.clear();
				cutPoints.add(0);
				Arrays.fill(distance, -1);
				Arrays.fill(sigma, 0);
				distance[curr] = 0;
				sigma[curr] = 1;
				boolean overflow = false;

				int d;
				for(d = 0; queue.size() != cutPoints.getInt(cutPoints.size() - 1); d++) {
					cutPoints.add(queue.size());
					final int start = cutPoints.getInt(d);
					final int end = cutPoints.getInt(d + 1);

					for(int pos = start; pos < end; pos++) {
						final int node = queue.getInt(pos);
						final long currSigma = sigma[node];
						final LazyIntIterator successors = graph.successors(node);
						for(int s; (s = successors.nextInt()) != -1;) {
							if (distance[s] == -1) {
								distance[s] = d + 1;
								delta[s] = 0;
								queue.add(s);
								assert checkOverflow(sigma, node, currSigma, s);
								overflow |= sigma[s] > Long.MAX_VALUE - currSigma;
								sigma[s] += currSigma;
							}
							else if (distance[s] == d + 1) {
								assert checkOverflow(sigma, node, currSigma, s);
								overflow |= sigma[s] > Long.MAX_VALUE - currSigma;
								sigma[s] += currSigma;
							}
						}
					}
				}

				if (overflow) throw new PathCountOverflowException();

				while(--d > 0) {
					final int start = cutPoints.getInt(d);
					final int end = cutPoints.getInt(d + 1);

					for(int pos = start; pos < end; pos++) {
						final int node = queue.getInt(pos);
						final double sigmaNode = sigma[node];
						final LazyIntIterator succ = graph.successors(node);
						for(int s; (s = succ.nextInt()) != -1;)
							if (distance[s] == d + 1) delta[node] += (1 + delta[s]) * sigmaNode / sigma[s];
					}

					synchronized (BetweennessCentrality.this) {
						for(int pos = start; pos < end; pos++) {
							final int node = queue.getInt(pos);
							betweenness[node] += delta[node];
						}
					}
				}

				if (BetweennessCentrality.this.pl != null)
					synchronized (BetweennessCentrality.this.pl) {
						BetweennessCentrality.this.pl.update();
					}
			}
		}
	}


	/** Computes betweenness centrality. Results can be found in {@link BetweennessCentrality#betweenness}. */
	public void compute() throws InterruptedException {
		final IterationThread[] thread = new IterationThread[numberOfThreads];
		for(int i = 0; i < thread.length; i++) thread[i] = new IterationThread();

		if (pl != null) {
			pl.start("Starting visits...");
			pl.expectedUpdates = graph.numNodes();
			pl.itemsName = "nodes";
		}

		final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		final ExecutorCompletionService<Void> executorCompletionService = new ExecutorCompletionService<>(executorService);

		for(int i = thread.length; i-- != 0;) executorCompletionService.submit(thread[i]);

		try {
			for(int i = thread.length; i-- != 0;) executorCompletionService.take().get();
		}
		catch(final ExecutionException e) {
			stop = true;
			final Throwable cause = e.getCause();
			throw cause instanceof RuntimeException ? (RuntimeException)cause : new RuntimeException(cause.getMessage(), cause);
		}
		finally {
			executorService.shutdown();
		}

		if (pl != null) pl.done();
	}


	public static void main(final String[] arg) throws IOException, InterruptedException, JSAPException {

		final SimpleJSAP jsap = new SimpleJSAP(BetweennessCentrality.class.getName(), "Computes the betweenness centrality a graph using an implementation of Brandes's algorithm based on multiple parallel breadth-first visits.",
			new Parameter[] {
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("rankFilename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final String graphBasename = jsapResult.getString("graphBasename");
		final String rankFilename = jsapResult.getString("rankFilename");
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");
		progressLogger.displayFreeMemory = true;
		progressLogger.displayLocalSpeed = true;

		ImmutableGraph graph = mapped? ImmutableGraph.loadMapped(graphBasename, progressLogger) : ImmutableGraph.load(graphBasename, progressLogger);
		if (jsapResult.userSpecified("expand")) graph = new ArrayListMutableGraph(graph).immutableView();

		final BetweennessCentrality betweennessCentralityMultipleVisits = new BetweennessCentrality(graph, threads, progressLogger);
		betweennessCentralityMultipleVisits.compute();

		BinIO.storeDoubles(betweennessCentralityMultipleVisits.betweenness, rankFilename);
	}
}
