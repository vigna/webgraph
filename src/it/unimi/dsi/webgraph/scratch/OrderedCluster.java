package it.unimi.dsi.webgraph.scratch;


import java.io.IOException;

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

public class OrderedCluster {
	private final ImmutableGraph graph;
	private final long m;
	private final double threshold;
	private int granularity;

	public OrderedCluster(final ImmutableGraph graph, final double threshold, final int granularity, final ProgressLogger pl) {
		this.graph = graph;
		this.threshold = threshold;
		this.granularity = granularity;
		m = graph.numArcs();

		if (pl != null) {
			pl.start("Starting scan");
			pl.expectedUpdates = m;
			pl.itemsName = "arcs";
		}
		
		int clusterStart = 0;
		int clusterSize = 0;
		int ones = 0;
		
		NodeIterator nodeIterator = graph.nodeIterator();
		while (nodeIterator.hasNext()) {
			int currentNode = nodeIterator.nextInt();
			LazyIntIterator successors = nodeIterator.successors();
			int t = 0; // Number of successors of currentNode within the current cluster
			for (;;) {
				int succ = successors.nextInt();
				if (succ < 0) break;
				if (succ > currentNode) break;
				if (succ >= clusterStart) t++;
				if (pl != null) pl.update();
			}
			ones += 2 * t;
			double ratio = 1 - ((double)ones / ((clusterSize + 1) * (clusterSize + 1) - clusterSize - 1) );
			//System.out.printf("%d\t%d\n", ones, ((clusterSize + 1) * (clusterSize + 1) - clusterSize - 1));
			//System.out.printf("%d\t%d\t%d\t%f\t%s\n", clusterStart, currentNode, clusterSize, ratio,  ratio > threshold);
			if (clusterSize > 0 && clusterSize % granularity == 0 && ratio > threshold) {
				System.out.println(clusterStart);
				clusterStart = currentNode;
				clusterSize = 0;
				ones = 0;
			} else {
				clusterSize++;
			}
		}
		System.out.println(clusterStart);
		if (pl != null) pl.done();
	}

	public static void main(final String[] args) throws JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(OrderedCluster.class.getName(), "TODO.", new Parameter[] {
				new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the input graph"),
				new FlaggedOption("t", JSAP.DOUBLE_PARSER, "1E-4", JSAP.NOT_REQUIRED, 't', "t", "Threshold"),
				new FlaggedOption("g", JSAP.INTEGER_PARSER, "5", JSAP.NOT_REQUIRED, 'g', "g", "Granularity")
		});

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final ImmutableGraph graph = ImmutableGraph.loadOffline(jsapResult.getString("graph"));
		final double threshold = jsapResult.getDouble("t");
		final int granularity = jsapResult.getInt("g");
		final OrderedCluster orderLabel = new OrderedCluster(graph, threshold, granularity, new ProgressLogger());
	}
}
