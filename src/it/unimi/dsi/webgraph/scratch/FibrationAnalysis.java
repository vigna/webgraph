/*
 * Created on Feb 2, 2006
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package it.unimi.dsi.webgraph.scratch;

import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

public class FibrationAnalysis {

	/**
	 * @param args
	 * @throws JSAPException
	 * @throws IOException
	 */
	public static void main(String[] args) throws JSAPException, IOException {
		SimpleJSAP jsap = new SimpleJSAP(FibrationAnalysis.class.getName(), "Analyses a given equivalence relation, typically a minimum base.",
				new Parameter[] {
					new UnflaggedOption("labelFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The file name where the labels are stored (int in binary form)"),
				}
			);

		JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		String labelFile = jsapResult.getString("labelFile");

		int label[] = BinIO.loadInts(labelFile);
		int n = label.length, x, nfib = 0;
		int fib2card[] = new int[n];
		int card2freq[] = new int[n];


		for (x = 0; x < n; x++) {
			if (fib2card[label[x]] == 0) nfib++;
			fib2card[label[x]]++;
		}
		for (x = 0; x < n; x++) card2freq[fib2card[x]]++;

		System.out.println("Nodes: " + n);
		System.out.println("Fibres: " + nfib);
		for (x = 0; x < n; x++) if (card2freq[x] > 0) System.out.println("Cardinality " + x + " frequency " + card2freq[x]);
	}

}
