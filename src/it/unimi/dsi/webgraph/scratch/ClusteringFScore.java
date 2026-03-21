package it.unimi.dsi.webgraph.scratch;


import java.io.IOException;
import java.util.Scanner;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;

public class ClusteringFScore {
	private final ImmutableGraph graph;
	private final Scanner scanner;

	public ClusteringFScore(final ImmutableGraph graph, final ProgressLogger pl) {
		this.graph = graph;
		this.scanner = new Scanner(System.in);
		final long m = graph.numArcs();
		final int n = graph.numNodes();

		if (pl != null) {
			pl.start("Starting scan");
			pl.expectedUpdates = m;
			pl.itemsName = "arcs";
		}
		
		NodeIterator nodeIterator = graph.nodeIterator();
		int tp = 0, fn = 0;
		int clusterStart = scanner.nextInt();
		int nextCluster = scanner.hasNextInt()? scanner.nextInt() : n; 
		
		while (nodeIterator.hasNext()) {
			int currentNode = nodeIterator.nextInt();
			int currentDegree = nodeIterator.outdegree();
			if (currentNode == nextCluster) {
				clusterStart = nextCluster;
				nextCluster = scanner.hasNextInt()? scanner.nextInt() : n; 
			}
			LazyIntIterator successors = nodeIterator.successors();
			int t = 0; // Number of successors of currentNode within the current cluster
			
			for (;;) {
				int succ = successors.nextInt();
				if (pl != null) pl.update();
				if (succ < 0) break;
				if (clusterStart <= succ && succ < nextCluster) t++;
				if (succ >= nextCluster) break;
			}
			tp += t;
			fn += (nextCluster - clusterStart - 1) - t;
			
		}
		if (pl != null) pl.done();
		// f1 = 2 * tp / (2 * tp + fp + fn)
		// since tp + fp = m we have fp = m-tp
		// f1 = 2 * tp / (2 * tp + m - tp + fn) = 2 * tp / (tp + m + fn)
		System.out.println((double)2 * tp / (tp + m + fn));
	}

	public static void main(final String[] args) throws JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(ClusteringFScore.class.getName(), "Computes the F-score of an ordered clustering of a graph. " 
				+ "It reads from stdin the ordered list of nodes starting a cluster.", new Parameter[] {
				new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the input graph"),
		});

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final ImmutableGraph graph = ImmutableGraph.loadOffline(jsapResult.getString("graph"));
		final ClusteringFScore cfs = new ClusteringFScore(graph, new ProgressLogger());
	}
}
