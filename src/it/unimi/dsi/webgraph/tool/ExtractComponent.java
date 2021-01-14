package it.unimi.dsi.webgraph.tool;

/*
 * Copyright (C) 2015-2020 Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.io.FileLinesCollection.FileLinesIterator;
import it.unimi.dsi.logging.ProgressLogger;

/** A tool that extracts a component from a graph, possibly building an associated identifier list. */

public class ExtractComponent {
	private final static Logger LOGGER = LoggerFactory.getLogger(ExtractComponent.class);

	public static void main(final String[] arg) throws IOException, JSAPException {

		final SimpleJSAP jsap = new SimpleJSAP(ExtractComponent.class.getName(), "Extracts a component from a graph, creating a map for Trasform and possibly creating an associated identifier (e.g., URL) list. Every file is read exactly once sequentially.",
			new Parameter[] {
			new FlaggedOption("n", JSAP.INTEGER_PARSER, "0", JSAP.REQUIRED, 'n', "n", "The chosen component."),
			new UnflaggedOption("components", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The components of the graph as a list of integers in DataInput format."),
			new UnflaggedOption("map", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The Transform map extracting the specified component as list of integers in DataInput format."),
			new UnflaggedOption("inIds", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The identifier list of the original graph in UTF-8 encoding."),
			new UnflaggedOption("outIds", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The identifier list of the extracted component in UTF-8 encoding."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		if (jsapResult.userSpecified("inIds") != jsapResult.userSpecified("outIds")) throw new IllegalArgumentException("You must either both or neither identifier lists");
		final int n = jsapResult.getInt("n");

		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");
		progressLogger.displayLocalSpeed = true;

		FileLinesIterator inIds = null;
		PrintWriter outIds = null;

		if (jsapResult.userSpecified("inIds")) {
			inIds = new FileLinesCollection(jsapResult.getString("inIds"), "UTF-8").iterator();
			outIds = new PrintWriter(new OutputStreamWriter(new FileOutputStream(jsapResult.getString("outIds")), Charsets.ISO_8859_1));
		}

		final DataOutputStream map = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(jsapResult.getString("map"))));

		int c = 0;

		progressLogger.start("Extracting... ");
		for(final IntIterator components = BinIO.asIntIterator(jsapResult.getString("components")); components.hasNext();) {
			if (components.nextInt() == n) {
				map.writeInt(c++);
				if (inIds != null) inIds.next().println(outIds);
			}
			else {
				if (inIds != null) inIds.next();
				map.writeInt(-1);
			}
			progressLogger.lightUpdate();
		}

		map.close();
		if (outIds != null) outIds.close();

		if (inIds != null) {
			if (inIds.hasNext()) throw new IllegalArgumentException("The number of components and the number of input identifiers must be the same");
			inIds.close();
		}
		progressLogger.done();
	}
}
