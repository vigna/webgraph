package it.unimi.dsi.webgraph.scratch;


import java.io.IOException;
import java.util.Arrays;
import java.util.SplittableRandom;

import org.slf4j.Logger;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

public class OrderLabel {
	private final ImmutableGraph graph;
	private final int n;
	private final int[] pre, post;
	private int counter;
	private final int[] topologicalOrder;
	private final Logger logger;
	private int enqueueCount;

	public OrderLabel(final ImmutableGraph graph, final ProgressLogger pl) {
		this.graph = graph;
		n = graph.numNodes();
		pre = new int[n];
		post = new int[n];

		if (pl != null) {
			pl.start("Starting visit");
			pl.expectedUpdates = n;
			pl.itemsName = "nodes";
		}
		for (int i = 0; i < n; i++) {
			if (pre[i] == 0) visit(i);
			if (pl != null) pl.update();
		}
		if (pl != null) pl.done();
		topologicalOrder = Util.identity(n);
		if (pl != null) pl.start("Ordering nodes");
		IntArrays.quickSort(topologicalOrder, (x,y) -> Integer.compare(post[y], post[x]));
		Util.invertPermutationInPlace(topologicalOrder);
		if (pl != null) pl.done();
		logger = pl.logger;
	}

	private void visit(final int x) {
		final LazyIntIterator s = graph.successors(x);
		pre[x] = ++counter;

		int y;
		while((y = s.nextInt()) != -1)
			if (pre[y] == 0)
				visit(y);
		post[x] = ++counter;
		return;
	}

	public boolean canReach(final int u, final int v, final boolean full) {
		if (!full && pre[u] <= pre[v] && post[v] <= post[u]) {
			logger.debug("Using SIT codes");
			return true;
		}
		if (!full && topologicalOrder[v] < topologicalOrder[u]) {
			logger.debug("Using topological order");
			return false;
		}
		logger.debug("Performing a BFS");
		return bfs(u, v, full);
	}

	public boolean bfs(final int u, final int v, final boolean full) {
		final IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
		final boolean[] seen = new boolean[n];
		LazyIntIterator successors;
		int d;

		queue.enqueue(u);
		enqueueCount++;
		seen[u] = true;
		while (!queue.isEmpty()) {
			final int x = queue.dequeueInt();
			if (x == v) return true;
			successors = graph.successors(x);
			d = graph.outdegree(x);
			while (d-- != 0) {
				final int y = successors.nextInt();
				if (!seen[y]) {
					if (!full && pre[y] <= pre[v] && post[v] <= post[y]) {
						logger.debug("Cutting (true)");
						return true; // Can reach v from here
					}
					if (!full && topologicalOrder[v] < topologicalOrder[y]) {
						logger.debug("Cutting (false)");
						continue; // Cannot reach v from here
					}
					queue.enqueue(y);
					enqueueCount++;
					seen[y] = true;
				}
			}
		}
		return false;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		for(int i = 0; i < n; i++)
			sb.append("Node " + i + ": [" + pre[i] + ".." + post[i] + "]\n");
		sb.append(Arrays.toString(Util.invertPermutation(topologicalOrder)));
		return sb.toString();
	}


	public static void main(final String[] args) throws JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(OrderLabel.class.getName(), "Prints on standard output the size of reachable set for each node and the time to compute it in nanoseconds.", new Parameter[] {
				new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the input graph"),
				new UnflaggedOption("k", JSAP.INTEGER_PARSER, JSAP.REQUIRED, "Number of pairs to sample"),
		});

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final ImmutableGraph graph = ImmutableGraph.load(jsapResult.getString("graph"));
		final OrderLabel orderLabel = new OrderLabel(graph, new ProgressLogger());
		final int n = graph.numNodes();

		final SplittableRandom r = new SplittableRandom();
		final int k = jsapResult.getInt("k");
		for (int i = 0; i < k; i++) {
			final int u = r.nextInt(n);
			final int v = r.nextInt(n);
			orderLabel.enqueueCount = 0;
			final boolean fullResult = orderLabel.canReach(u, v, true);
			final int fullCount = orderLabel.enqueueCount;
			orderLabel.enqueueCount = 0;
			final boolean nonfullResult = orderLabel.canReach(u, v, false);
			final int nonfullCount = orderLabel.enqueueCount;
			assert fullResult == nonfullResult;
			System.out.println(nonfullCount + "\t" + fullCount);
		}
	}
}
