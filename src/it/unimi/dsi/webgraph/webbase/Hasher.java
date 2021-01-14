package it.unimi.dsi.webgraph.webbase;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

public class Hasher {

	private Hasher() {}

	public static void main(String arg[]) throws IOException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP(Hasher.class.getName(), "Launch the Hasher",
				new Parameter[] {
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph"),
					}
				);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		String basename = jsapResult.getString("basename");

		FastBufferedReader in = new FastBufferedReader(new InputStreamReader(System.in, "ISO-8859-1"), 16 * 1024 * 1024);
		PrintWriter urls = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(basename + ".urls"), "ISO-8859-1"), 1024 * 1024));

		ProgressLogger pl = new ProgressLogger();
		pl.itemsName = "URLs";
		MutableString s = new MutableString();
		Long2IntOpenHashMap hashes = new Long2IntOpenHashMap();
		hashes.defaultReturnValue(-1);

		long h, l = 0, doubles = 0;
		int n = 0;
		URL2 u;

		System.err.print("Reading URLs...");
		pl.start();
		while ((in.readLine(s)) != null) {
			l++;

			if (! (u = new URL2(s.toString())).isValid()) {
				System.err.print("[WARNING: invalid URL " + s + " found at line " + l + "]");
				continue;
			}

			if (! s.startsWith("http")) {
				System.err.print("[WARNING: URL " + s + " at line " + l + " does not start with \"http\"]");
				continue;
			}

			h = u.hashCode64();
			if (! hashes.containsKey(h)) {
				hashes.put(h, n++);
				s.println(urls);
			}
			else doubles++;
			//else if (s.hashCode() != hashUrls.get(h)) System.err.println("Clash on URL " + s);
			pl.update();

			if (n % 1000000 == 0) System.err.print("[URLs: " + n + "; doubles: " + doubles + "; broken: " + (l - n - doubles) + "]");
		}

		pl.stop();
		urls.close();
		System.err.println(" done.");
		System.err.println(pl);
		in.close();

		ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(basename + ".crc"), 1024 * 1024));
		out.writeObject(hashes);
		out.close();
	}
}


// Local Variables:
// mode: jde
// tab-width: 4
// End:
