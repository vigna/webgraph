package it.unimi.dsi.webgraph.scratch;

/*
 * Copyright (C) 2007-2020 Sebastiano Vigna
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


import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** Outputs (onto stdout) the graph adjacency matrix in SMAT format.
 *  The SMAT format is as follows:
 *
 *  <pre>
 *    n_nodes n_nodes n_arcs
 *    from to 1
 *    from to 1
 *    ...
 *  </pre>
 */


public class AdjMatrixSmatOut {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	private static final Logger LOGGER = LoggerFactory.getLogger(AdjMatrixSmatOut.class);


	@SuppressWarnings("boxing")
	public static void main(String arg[]) throws IOException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP(AdjMatrixSmatOut.class.getName(),
				"Outputs the adjacency matrix of a given graph in SMAT format. ",
				new Parameter[] {
			new FlaggedOption("scale", JSAP.DOUBLE_PARSER, "1.0", JSAP.NOT_REQUIRED, 's', "scale", "Scale (e.g.: 0.5 produces a matrix of half the size of the original)."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
		}
		);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final ImmutableGraph graph = ImmutableGraph.loadOffline(basename);
		int n = graph.numNodes();

		ProgressLogger pl = new ProgressLogger(LOGGER);
		pl.expectedUpdates = n;
		pl.itemsName = "nodes";

		double scale = jsapResult.getDouble("scale");

		if (scale == 1.0) {
			System.out.printf("%d %d %d\n", n, n, graph.numArcs());

			NodeIterator nodeIterator = graph.nodeIterator();

			while (nodeIterator.hasNext()) {
				pl.update();
				int x = nodeIterator.nextInt(), y;
				LazyIntIterator successors = nodeIterator.successors();
				while ((y = successors.nextInt()) >= 0) {
					System.out.printf("%d %d 1\n", x, y);
				}
			}
		} else {
			int realN = (int)(n * scale);
			int mat[][] = new int[realN + 1][realN + 1];
			NodeIterator nodeIterator = graph.nodeIterator();
			while (nodeIterator.hasNext()) {
				pl.update();
				int x = nodeIterator.nextInt(), y;
				LazyIntIterator successors = nodeIterator.successors();
				while ((y = successors.nextInt()) >= 0) {
					mat[(int)(x * scale)][(int)(y *scale)]++;
				}
			}
			int cnz=0;
			final double realNSquare = realN * realN;
			for (int x = 0; x < realN; x++)
				for (int y = 0; y < realN; y++)
					if (mat[x][y] > 0) cnz++;

			System.out.printf("%d %d %d\n", realN, realN, cnz);
			for (int x = 0; x < realN; x++)
				for (int y = 0; y < realN; y++)
					if (mat[x][y] > 0) System.out.printf("%d %d %f\n", x, y, mat[x][y] / realNSquare);

		}

	}
}
