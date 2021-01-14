package it.unimi.dsi.webgraph.scratch;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

import java.io.IOException;
import java.util.Arrays;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

public class ConvertToClusteringFormat {

	public static void main(String[] args) throws IOException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP(ConvertToClusteringFormat.class.getName(), "Prints a graph in the GraClus format, with weights taken from a file.\n" +
				"More precisely, the graph must be a symmetric loopless graph; the edge file is a file that contains as many longs as there are edges " +
				"in the graph; the longs are sorted in increasing order, and the edge (x,y) (where x<y) is represented by the long x<<32 | y. \n" +
				"Finally, the weight file contains weights for the edges, where a weight is an integer (and there are as many weights as there are " +
				"edges",
				new Parameter[] {
						new UnflaggedOption("type", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "Type of output: metis / infomap"),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the input graph"),
						new UnflaggedOption("edge", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the edge (.source/.target) files"),
						new UnflaggedOption("weight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the weight file"),
					}
				);

		JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		String basename = jsapResult.getString("basename");
		String edgeBasename = jsapResult.getString("edge");
		String weight = jsapResult.getString("weight");
		String type = jsapResult.getString("type");

		if (!type.equals("metis") && !type.equals("infomap")) throw new JSAPException("Convertion type " + type + " unknown");

		ImmutableGraph g = ImmutableGraph.loadOffline(basename);
		int n = g.numNodes();
		ProgressLogger pl = new ProgressLogger();
		pl.itemsName = "nodes";
		pl.expectedUpdates = n;
		pl.start("Converting");
		if (type.equals("metis")) System.out.println(g.numNodes() + " " + g.numArcs() / 2 + " " + 1);
		int[] tr = BinIO.loadInts(weight);
		int[] n2source = BinIO.loadInts(edgeBasename + ".source");
		int[] n2target = BinIO.loadInts(edgeBasename + ".target");
		long[] edge = new long[tr.length];
		for (int i = 0; i < tr.length; i++) edge[i] = ((long)n2source[i] << 32) | n2target[i];
		NodeIterator ni = g.nodeIterator();
		while(ni.hasNext()) {
			int x = ni.nextInt();
			int d = ni.outdegree();
			int[] s = ni.successorArray();
			for(int i = 0 ; i < d; i++) {
				int y=s[i];
				if (x == y)
					throw new IllegalArgumentException("A loop at node " + x);
				long edgeValue = ((long)Math.min(x,y) << 32) | Math.max(x,y);
				int j = Arrays.binarySearch(edge, edgeValue);
				if (type.equals("metis")) System.out.print((y+1) + " " + tr[j] + " ");
				if (type.equals("infomap"))  System.out.println((x+1) + " " + (y+1) + " " + tr[j]);
			}
			if (type.equals("metis")) System.out.println();
			pl.lightUpdate();
		}
		pl.done();
	}

}
