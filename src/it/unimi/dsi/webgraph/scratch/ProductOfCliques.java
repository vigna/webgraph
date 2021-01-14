package it.unimi.dsi.webgraph.scratch;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import it.unimi.dsi.webgraph.Transform;

import java.io.File;
import java.io.IOException;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

public class ProductOfCliques {


	public static int[] nodeCoordinates(int x, int s, int t) {
		int[] res = new int[s];
		for (int i = s - 1; i >= 0; i--) {
			res[i] = x % t;
			x /= t;
		}
		return res;
	}

	/** Given the node (represented by s coordinates, each between 0 and t-1) produce a single node identifier.
	 *
	 * @param x the node coordinates.
	 * @param s the number of cliques.
	 * @param t the size of each clique.
	 * @return the node identifier.
	 */
	public static int nodeName(int[] x, int s, int t) {
		int res = 0;
		for (int i = 0; i < x.length; i++) res = t * res + x[i];
		return res;
	}

	public static void main(String[] args) throws IOException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP(ProductOfCliques.class.getName(), "Produces a graph that is the cartesian product of s copies of a graph made of " +
				"a clique of size t",
				new Parameter[] {
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the output graph"),
						new UnflaggedOption("s", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The number of copies of the graph"),
						new UnflaggedOption("t", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The number of elements per clique"),
					}
				);

		JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		String basename = jsapResult.getString("basename");
		int s = jsapResult.getInt("s");
		int t = jsapResult.getInt("t");
		final int batchSize = ScatteredArcsASCIIGraph.DEFAULT_BATCH_SIZE;

		ProgressLogger pl = new ProgressLogger();


		final int[] source = new int[batchSize] , target = new int[batchSize];
		int currBatch = 0, pairs = 0;
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		int lastCoordinates[] = new int[s];
		for (int i = 0; i < s; i++) lastCoordinates[i] = t - 1;
		int lastId = nodeName(lastCoordinates, s, t);

		pl.itemsName = "nodes";
		pl.expectedUpdates = lastId;
		pl.start("Building batches for product graph");

		for (int from = 0; from <= lastId; from++) {
			int[] fromCoordinates = nodeCoordinates(from, s, t);
			for (int i = 0; i < s; i++)
				for (int v = 0; v < t; v++)
					if (v != fromCoordinates[i]) {
						int[] toCoordinates = new int[s];
						System.arraycopy(fromCoordinates, 0, toCoordinates, 0, s);
						toCoordinates[i] = v;
						int to = nodeName(toCoordinates, s, t);
						if (currBatch == batchSize) {
							pairs += Transform.processBatch(batchSize, source, target, null, batches);
							currBatch = 0;
						}
						source[currBatch] = from;
						target[currBatch++] = to;
						if (from < to) System.out.println(from + "\t" + to + "\t" + i);
					}
			pl.lightUpdate();
		}

		if (currBatch > 0) {
			pairs += Transform.processBatch(currBatch, source, target, null, batches);
			currBatch = 0;
		}

		pl.done();
		ImmutableGraph.store(BVGraph.class, new Transform.BatchGraph(lastId + 1, pairs, batches), basename, pl);

	}

}
