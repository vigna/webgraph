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

public class ClusteringModularity {
	private final ImmutableGraph graph;
	private final Scanner scanner;

	public ClusteringModularity(final ImmutableGraph graph, final double gamma, final ProgressLogger pl) {
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
		int intraClusterArcs = 0, kc2 = 0, d = 0; // d=sum of degrees of current cluster, kc2=sum of d^2 over all clusters
		int clusterStart = scanner.nextInt();
		int nextCluster = scanner.hasNextInt()? scanner.nextInt() : n; 
		
		while (nodeIterator.hasNext()) {
			int currentNode = nodeIterator.nextInt();
			int currentDegree = nodeIterator.outdegree();
			if (currentNode == nextCluster) {
				clusterStart = nextCluster;
				nextCluster = scanner.hasNextInt()? scanner.nextInt() : n;
				kc2 += d * d;
				d = currentDegree;
			} else d += currentDegree;
			LazyIntIterator successors = nodeIterator.successors();
			int t = 0; // Number of successors of currentNode within the current cluster
			
			for (;;) {
				int succ = successors.nextInt();
				if (pl != null) pl.update();
				if (succ < 0) break;
				if (clusterStart <= succ && succ < nextCluster) t++;
				if (succ >= nextCluster) break;
			}
			intraClusterArcs += t;
			
		}
		kc2 += d * d; // Last cluster
		if (pl != null) pl.done();
		double modularity = intraClusterArcs / m - gamma * kc2 / (4 * m * m);
		System.out.println(modularity);
	}

	public static void main(final String[] args) throws JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(ClusteringModularity.class.getName(), "Computes the modularity of an ordered clustering of a graph. " 
				+ "It reads from stdin the ordered list of nodes starting a cluster.", new Parameter[] {
				new FlaggedOption("gamma", JSAP.DOUBLE_PARSER, "1", JSAP.NOT_REQUIRED, 'g', "gamma", "Resolution"),
				new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the input graph"),
		});

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final ImmutableGraph graph = ImmutableGraph.loadOffline(jsapResult.getString("graph"));
		final double gamma = jsapResult.getDouble("gamma");
		final ClusteringModularity cfs = new ClusteringModularity(graph, gamma, new ProgressLogger());
	}
}
