package it.unimi.dsi.webgraph.webbase;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.Properties;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** Converts from textual to binary representation a web graph.
 *
 * <P>This class reads from standard input a sequence of lines. Each line contains URLs
 * separated by TABs. The first URL is assumed to be the URL of a page, and the remaining
 * the URLs of successor pages. An accompanying CRC file specifies for each URL the corresponding
 * node. The list may have duplicate pages, which will be skipped.
 *
 * <P>The graph output by this class is compatible with {@link it.unimi.dsi.webgraph.BVGraph}.
 */

public class Converter {

	private Converter() {}

	/** A reasonable format for real numbers. */
	private static java.text.NumberFormat formatDouble = new java.text.DecimalFormat("#.00");

	/** Formats a number.
	 *
	 * <P>This method formats a double printing just two fractional digits.
	 * @param d a number.
	 * @return a string containing a pretty print of the number.
	 */
	private static String format(final double d) {
		final StringBuffer s = new StringBuffer();
		return formatDouble.format(d, s, new java.text.FieldPosition(0)).toString();
	}

	public static void main(String arg[]) throws IOException, ClassNotFoundException, JSAPException  {
		SimpleJSAP jsap = new SimpleJSAP(Converter.class.getName(), "Launch the Converter",
				new Parameter[] {
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph"),
					}
				);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		String basename = jsapResult.getString("basename");

		@SuppressWarnings("resource")
		FastBufferedReader in = new FastBufferedReader(new InputStreamReader(System.in, "ISO-8859-1"), 16 * 1024 * 1024);

		MutableString s = new MutableString();

		// We read CRC map.
		final Long2IntOpenHashMap hashes;
		final ObjectInputStream hashDos = new ObjectInputStream(new FastBufferedInputStream(new FileInputStream(basename + ".crc")));
		System.err.print("Reading CRC map...");
		hashes = (Long2IntOpenHashMap)hashDos.readObject();
		System.err.println(" done.");
		hashDos.close();

		OutputBitStream obs = new OutputBitStream(new FileOutputStream(basename + BVGraph.GRAPH_EXTENSION), 1 * 1024 * 1024);
		OutputBitStream offsets = new OutputBitStream(new FileOutputStream(basename + BVGraph.OFFSETS_EXTENSION), 1 * 1024 * 1024);

		long h, bitOffset = 0;
		int i, prev, end;
		int outd;
		URL2 u;
		int n = 0, l = 0, t;
		IntRBTreeSet outlinks = new IntRBTreeSet();
		IntIterator j;
		long totLinks = 0;
		long brokenPages = 0, brokenLinks = 0, invalidLinks = 0, doubleLinks = 0;

		ProgressLogger pl = new ProgressLogger();
		pl.itemsName = "URLs";
		System.err.print("Generating graph...");
		pl.start();

		while ((in.readLine(s)) != null) {
			// We recover the first URL on the line.

			if (++l % 1000000 == 0) System.err.print("[Pages: " + l + "; broken pages: " + brokenPages + "; links: " + totLinks + " broken links: " + brokenLinks + "; double links: " + doubleLinks + "; invalidLinks: " + invalidLinks + "]");
			i = s.indexOf('\t');
			u = new URL2(i == -1 ? s.toString() : new String(s.array(), 0, i));
			h = u.hashCode64();

			// To work out a URL, it must be in hashes and its index must be equal to our current index.
			if (hashes.get(h) == n) {
				outlinks.clear();

				// We add all URLs on the line to the outlinks sorted map.
				end = s.length();

				while((i = s.lastIndexOf('\t', end - 1)) != -1) {
					h = CRC64.compute(s.array(), i + 1, end - i - 1);

					if (hashes.containsKey(h)) {
						if (! outlinks.add(hashes.get(h))) doubleLinks++;
					}
					else if (hashes.containsKey(h = (new URL2(new String(s.array(), i + 1, end - i - 1))).hashCode64())) {
						System.err.println(l + ": the URL " + new URL2(new String(s.array(), i + 1, end - i - 1)) + " has been been obtained from " +
							(new String(s.array(), i + 1, end - i - 1)) + " by normalisation");
						if (! outlinks.add(hashes.get(h))) doubleLinks++;
					}
					else {
						System.err.println(l + ": no hope for " + (new String(s.array(), i + 1, end - i - 1)));
						brokenLinks++;
					}

					end = i;
				}

				// Now we record the graph entry. First of all we output the bit offset.
				offsets.writeGamma((int)(obs.writtenBits() - bitOffset));
				bitOffset = obs.writtenBits();

				// Then, we write the outdegree.
				obs.writeGamma(outd = outlinks.size());

				if (outd != 0) {
					// No reference and no intervals.
					totLinks += outd;

					j = outlinks.iterator();

					obs.writeDelta(Fast.int2nat((prev = j.nextInt()) - n));

					// We already wrote one outlink, so we use predecrement.
					while(--outd != 0) {
						t = j.nextInt();
						obs.writeDelta(t - prev - 1);
						prev = t;
					}
				}

				n++;
				pl.update();
			}
			else {
				// If a URL is NOT in the map, it is BAD.
				if (! hashes.containsKey(h)) {
					System.err.println();
					System.err.println(l + ": WARNING: Map does not contain " + u);
					brokenPages++;
				}
				else {}
			}

			// We write the closing delta.
			offsets.writeGamma((int)(obs.writtenBits() - bitOffset));
		}

		obs.close();
		offsets.close();

		System.err.print("[Pages: " + l + "; broken pages: " + brokenPages + "; links: " + totLinks + " broken links: " + brokenLinks + "; double links: " + doubleLinks + "; invalidLinks: " + invalidLinks + "]");

		pl.stop();
		System.err.println(" done.");
		System.err.println("Lines read: " + l + " " + pl);

		final Properties properties = new Properties();
		properties.setProperty("nodes", String.valueOf(n));
		properties.setProperty("arcs", String.valueOf(totLinks));
		properties.setProperty("zetak", "3");
		properties.setProperty("windowsize", "0");
		properties.setProperty("maxrefcount", "0");
		properties.setProperty("minintervallength", "0");
		properties.setProperty("compressionflags", "RESIDUALS_DELTA");
		properties.setProperty("bitsperlink", format((double)obs.writtenBits() / totLinks));
		properties.setProperty("bitspernode", format((double)obs.writtenBits() / n));
		properties.setProperty(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY, "class it.unimi.dsi.webgraph.BVGraph");
		properties.setProperty("version", "0");
		final FileOutputStream propertyFile = new FileOutputStream(basename + ImmutableGraph.PROPERTIES_EXTENSION);
		properties.store(propertyFile, "Converter properties");

		propertyFile.close();
	}
}
