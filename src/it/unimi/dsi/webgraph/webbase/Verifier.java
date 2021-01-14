package it.unimi.dsi.webgraph.webbase;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** The main method of this class scans TAB-separated list of URLs and matches
 * it against a given graph.
 */
public class Verifier {

	private Verifier() {}

	/** The immutable graph. */
	private static BVGraph graph;
	/** The map from CRCs to graph nodes It is generated with <code>.crc</code> suffix by {@link Converter}. */
	private static Long2IntMap crc2node;
	/** The map associating URLs to nodes. */
	private static Object2LongFunction<CharSequence> mph;

	@SuppressWarnings({ "unchecked", "resource" })
	public static void main(String arg[]) throws IOException, ClassNotFoundException, JSAPException {

		SimpleJSAP jsap = new SimpleJSAP(Verifier.class.getName(), "Reads from standard input a TAB-separated list of URLs and checks it against the given graph",
				new Parameter[] {
						new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph"),
						new UnflaggedOption("mph", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "A minimal perfect hash"),
						new UnflaggedOption("crcs", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "A map from CRC's to nodes"),
					}
				);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		FastBufferedReader in = new FastBufferedReader(new InputStreamReader(System.in, "ISO-8859-1"), 16 * 1024 * 1024);

		String graphName = jsapResult.getString("graph");
		String mphName = jsapResult.getString("mph");;
		String crcsName = jsapResult.getString("crcs");;

		ProgressLogger pl = new ProgressLogger();

		graph = BVGraph.load(graphName, 2, pl);

		System.err.print("Loading CRC map...");
		crc2node = (Long2IntMap) (new ObjectInputStream(new FileInputStream(crcsName))).readObject();
		System.err.println(" done.");

		System.err.print("Loading minimal perfect hash...");
		mph = (Object2LongFunction<CharSequence>)BinIO.loadObject(mphName);
		System.err.println(" done.");

		crc2node.defaultReturnValue(-1);

		IntOpenHashSet successors = new IntOpenHashSet(), links = new IntOpenHashSet();
		int i, end, node, crcNode, n = 0;
		URL2 u, v;
		long h;
		String t;
		MutableString s = new MutableString();
		pl.itemsName = "URLs";
		System.err.print("Verifying graph...");
		pl.start();

		while ((in.readLine(s)) != null) {
			pl.update();

			// We recover the first URL on the line.
			i = s.indexOf('\t');
			u = new URL2(t = i != -1 ? new String(s.array(), 0, i) : s.toString());
			h = u.hashCode64();
			node = (int)mph.getLong(t);
			crcNode = crc2node.get(h);
			// To work out a URL, it must be in hashes and its index must be equal to our current index.
			if (crcNode == n) {
				if (node == -1) System.out.println("WARNING: URL " + n + "(" + u + ") is in CRC map, but it is not hashable.");
				else if (node != n) System.out.println("WARNING: URL " + n + "(" + u + ") has hash " + node);

				LazyIntIterator it = graph.successors(n);
				successors.clear();
				links.clear();
				int temp;
				while((temp = it.nextInt()) != -1) successors.add(temp);

				end = s.length();

				while((i = s.lastIndexOf('\t', end - 1)) != -1) {
					h = (v = new URL2(new String(s.array(), i + 1, end - i - 1))).hashCode64();
					node = crc2node.get(h);

					if (node != -1) {
						if(! successors.contains(node)) System.out.println("WARNING: Link " + v + " of node " + n + " (URL " + u + ") not in successor list.");
						else links.add(node);
					}

					end = i;
				}

				successors.removeAll(links);
				if (successors.size() > 0) System.out.println("WARNING: Successors " + successors + " not among links of node " + n + " (URL " + u + ").");
				n++;
			}
			else if (node != -1 && crcNode == -1) System.out.println("WARNING: URL " + u + " is not in CRC map, but it is hashed to " + node + ".");
		}

		if (n != crc2node.size()) System.err.println("The CRC map contains " + crc2node.size() + " pairs, but the file provided " + n + ".");
		if (n != graph.numNodes()) System.err.println("The graph contains " + graph.numNodes() + " nodes, but the file provided " + n + ".");
		if (n != mph.size()) System.err.println("The minimal perfect hash contains " + mph.size() + " pairs, but the file provided " + n + ".");

		System.err.println(" done.");
		pl.stop();
	}
}

