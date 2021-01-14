package it.unimi.dsi.webgraph.scratch;


import java.io.IOException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

public class ReachabilityStats {
	private static void visit(final ImmutableGraph graph, final int x, final IntOpenHashSet seen) {
		final LazyIntIterator s = graph.successors(x);
		seen.add(x);
		int y;
		while((y = s.nextInt()) != -1)
			if (!seen.contains(y))
				visit(graph, y, seen);
		return;
	}

	private static int reachable(final ImmutableGraph graph, final int x) {
		final IntOpenHashSet seen = new IntOpenHashSet();
		visit(graph, x, seen);
		return seen.size();
	}

	public static void main(final String[] args) throws JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(ReachabilityStats.class.getName(), "Prints on standard output the size of reachable set for each node and the time to compute it in nanoseconds.", new Parameter[] {
				new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the input graph"),
		});

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final int numberOfThreads = Runtime.getRuntime().availableProcessors();
		final ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads, new ThreadFactoryBuilder().setNameFormat("ProcessingThread-%d").build());
		final ExecutorCompletionService<Void> executorCompletionService = new ExecutorCompletionService<>(executorService);
		final ImmutableGraph graph = ImmutableGraph.load(jsapResult.getString("graph"));
		final int n = graph.numNodes();
		final ProgressLogger pl = new ProgressLogger();
		pl.itemsName = "nodes";
		pl.expectedUpdates = n;
		pl.logInterval = ProgressLogger.ONE_MINUTE;

		final int[] reachable = new int[n];
		final long[] time = new long[n];
		final int step = (n + numberOfThreads - 1) / numberOfThreads;

		pl.start("Visiting...");
		for(int i = numberOfThreads; i-- != 0;) {
			final int start = i * step;
			final int end = Math.min(start + step, n);
			executorCompletionService.submit(() ->  {
				final ImmutableGraph g = graph.copy();
				for(int x = start; x < end; x++) {
					final long startTime = - System.nanoTime();
					reachable[x] = reachable(g, x);
					time[x] = startTime + System.nanoTime();
					synchronized(pl) {
						pl.update();
					}
				}
				return null;
			});
		}

		@SuppressWarnings("unused")
		Throwable readingProblem = null; // TODO: throw it conditionally to an option
		for(int i = numberOfThreads; i-- != 0;)
			try {
				executorCompletionService.take().get();
			}
			catch(final Exception e) {
				System.err.println("Unexpected exception in parallel thread\n" + e.getCause());
				readingProblem = e.getCause(); // We keep only the last one. They will be logged anyway.
			}

		executorService.shutdown();
		pl.done();

		for(int x = 0; x < n; x++) {
			final int r = reachable[x];
			System.out.println(r + "\t" + r * 100. / n + "\t" + time[x]);
		}
	}
}
