/*
 * Copyright (C) 2013-2023 Sebastiano Vigna
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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntToDoubleFunction;

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

import it.unimi.dsi.fastutil.ints.Int2DoubleFunction;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

/** Computes exactly a <em>positive</em> linear geometric centrality with specified coefficient vector
 * and the number of reachable nodes using multiple parallel breadth-first visits.
 * A survey about linear geometric centralities can be found in: Boldi, Paolo, Flavio Furia, and 
 * Chiara Prezioso. <em>Linear Geometric Centralities</em>. International Workshop on Modelling and Mining Networks. Springer, Cham, 2025.
 * 
 * 
 * <p>Note that usually one is interested in the <em>negative</em> version of a centrality measure, that is, the version
 * that depends on the <em>incoming</em> arcs. This class can compute only <em>positive</em> centralities: if you are
 * interested (as it usually happens) in the negative version, you must pass to this class the <em>transpose</em> of the graph.
 *
 * <p>Every visit is independent and is carried out by a separate thread. The downside is
 * that running on <var>k</var> cores requires approximately <var>k</var> times the memory of the
 * sequential algorithm, as only the graph and the betweenness array will be shared.
 *
 * <p>To use this class you first create an instance, and then invoke {@link #compute()}.
 * After that, you can peek at the fields {@link #centrality} and {@link #reachable}.
 */

public class LinearGeometricCentrality {
	private final static Logger LOGGER = LoggerFactory.getLogger(LinearGeometricCentrality.class);

	final static public class HarmonicCoefficients implements Int2DoubleFunction {
		@Override
		public double get(int key) {
			if (key == 0) {
				return 0;
			}
			return 1. / key;
		}
	}
	
	/** The graph under examination. */
	private final ImmutableGraph graph;
	/** The function returning for every natural number the corresponding coefficient. */
	private IntToDoubleFunction coeffs;
	/** Linear centrality. */
	public final double[] centrality;
	/** Number of reachable nodes. */
	public final long[] reachable;
	/** The global progress logger. */
	private final ProgressLogger pl;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node to be visited. */
	protected final AtomicInteger nextNode;
	/** Whether to stop abruptly the visiting process. */
	protected volatile boolean stop;

	/** Creates a new class for computing a positive linear geometric centrality and the number reachable nodes.
	 *
	 * @param graph a graph.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param pl a progress logger, or {@code null}.
	 */
	public LinearGeometricCentrality(final ImmutableGraph graph, final IntToDoubleFunction coeffs, final int requestedThreads, final ProgressLogger pl) {
		this.pl = pl;
		this.graph = graph;
		this.coeffs = coeffs;
		this.centrality = new double[graph.numNodes()];
		this.reachable = new long[graph.numNodes()];
		this.nextNode = new AtomicInteger();
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
	}

	/** Creates a new class for computing a positive linear geometric centrality and the number of reachable nodes, using as many threads as
	 *  the number of available processors.
	 *
	 * @param graph a graph.
	 * @param pl a progress logger, or {@code null}.
	 */
	public LinearGeometricCentrality(final ImmutableGraph graph, final IntToDoubleFunction coeffs, final ProgressLogger pl) {
		this(graph, coeffs, 0, pl);
	}

	/** Creates a new class for computing a positive linear geometric centrality and the number of reachable nodes.
	 *
	 * @param graph a graph.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 */
	public LinearGeometricCentrality(final ImmutableGraph graph, final IntToDoubleFunction coeffs, final int requestedThreads) {
		this(graph, coeffs, 1, null);
	}

	/** Creates a new class for computing a positive linear geometric centralities and the number of reachable nodes, using as many threads as
	 *  the number of available processors.
	 *
	 * @param graph a graph.
	 */
	public LinearGeometricCentrality(final ImmutableGraph graph, final IntToDoubleFunction coeffs) {
		this(graph, coeffs, 0);
	}

	private final class IterationThread implements Callable<Void> {
		/** The queue of visited nodes. */
		private final IntArrayFIFOQueue queue;
		/** The array containing the distance of each node from the current source (or -1 if the node has not yet been reached by the visit). */
		private final int[] distance;

		private IterationThread() {
			this.distance = new int[graph.numNodes()];
			this.queue = new IntArrayFIFOQueue();
		}

		@Override
		public Void call() {
			// We cache frequently used fields.
			final int[] distance = this.distance;
			final IntArrayFIFOQueue queue = this.queue;
			final ImmutableGraph graph = LinearGeometricCentrality.this.graph.copy();
			final IntToDoubleFunction coeffs = LinearGeometricCentrality.this.coeffs;

			for(;;) {
				final int curr = nextNode.getAndIncrement();
				if (LinearGeometricCentrality.this.stop || curr >= graph.numNodes()) return null;
				queue.clear();
				queue.enqueue(curr);
				Arrays.fill(distance, -1);
				distance[curr] = 0;
				int reachable = 0;

				while(! queue.isEmpty()) {
					final int node = queue.dequeueInt();
					reachable++;
					final int d = distance[node] + 1;
					final double coeff = coeffs.applyAsDouble(d);
					final LazyIntIterator successors = graph.successors(node);
					for(int s; (s = successors.nextInt()) != -1;) {
						if (distance[s] == -1) {
							queue.enqueue(s);
							distance[s] = d;
							centrality[curr] += coeff;
						}
					}
				}

				if (LinearGeometricCentrality.this.pl != null)
					synchronized (LinearGeometricCentrality.this.pl) {
						LinearGeometricCentrality.this.pl.update();
					}

				LinearGeometricCentrality.this.reachable[curr] = reachable;
			}
		}
	}


	/** Computes a linear geometric centralitu and the number of reachable nodes.
	 * Results can be found in {@link LinearGeometricCentrality#centrality} and {@link LinearGeometricCentrality#reachable}. */
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


	public static void main(final String[] arg) throws IOException, JSAPException, InterruptedException, NoSuchMethodException, SecurityException, ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		final SimpleJSAP jsap = new SimpleJSAP(LinearGeometricCentrality.class.getName(), "Computes a positive linear geometric centrality of a graph using multiple parallel breadth-first visits.\n\nPlease note that to compute negative centralities on directed graphs (which is usually what you want) you have to compute positive centralities on the transpose.",
			new Parameter[] {
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("coefficientsSpec", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The coefficient class spec (CLASSNAME or CLASSNAME(arg,...)."),
			new UnflaggedOption("centralityFilename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where centrality scores (doubles in binary form) will be stored."),
			new UnflaggedOption("reachableFilename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the number of reachable nodes (longs in binary form) will be stored.")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String coefficientsSpec = jsapResult.getString("coefficientsSpec");
		final IntToDoubleFunction coeffs;
		if (coefficientsSpec.contains("(")) {
			int opp = coefficientsSpec.indexOf("(");
			String className = coefficientsSpec.substring(0, opp);
			String[] args = coefficientsSpec.substring(opp, coefficientsSpec.length() - 1).split(",");
			double[] doubleArgs = java.util.Arrays.stream(args).mapToDouble(Double::parseDouble).toArray();
			Class<?>[] parameterTypes = new Class<?>[doubleArgs.length];
			Arrays.fill(parameterTypes, double.class);
			Constructor<?> constructor = Class.forName(className).getConstructor(parameterTypes);
			coeffs = (IntToDoubleFunction) constructor.newInstance(doubleArgs);
		} else {
			Constructor<?> constructor = Class.forName(coefficientsSpec).getConstructor();
			coeffs = (IntToDoubleFunction) constructor.newInstance();
		}
		
		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final String graphBasename = jsapResult.getString("graphBasename");
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");
		progressLogger.displayFreeMemory = true;
		progressLogger.displayLocalSpeed = true;

		ImmutableGraph graph = mapped? ImmutableGraph.loadMapped(graphBasename, progressLogger) : ImmutableGraph.load(graphBasename, progressLogger);
		if (jsapResult.userSpecified("expand")) graph = new ArrayListMutableGraph(graph).immutableView();

		final LinearGeometricCentrality centralities = new LinearGeometricCentrality(graph, coeffs, threads, progressLogger); 
		centralities.compute();

		BinIO.storeDoubles(centralities.centrality, jsapResult.getString("centralityFilename"));
		BinIO.storeLongs(centralities.reachable, jsapResult.getString("reachableFilename"));
	}
}
