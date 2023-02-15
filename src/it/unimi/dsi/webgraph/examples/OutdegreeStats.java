/*
 * Copyright (C) 2003-2023 Paolo Boldi and Sebastiano Vigna
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.webgraph.examples;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

/** The main method of this class loads an arbitrary {@link it.unimi.dsi.webgraph.ImmutableGraph}
 * and performs a sequential scan to establish the minimum, maximum and average outdegree.
 */

public class OutdegreeStats {

	private OutdegreeStats() {}

	static public void main(final String arg[]) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(OutdegreeStats.class.getName(), "Prints on standard error the maximum, minimum and average degree of a graph, and outputs on standard output the numerosity of each outdegree value (first line is the number of nodes with outdegree 0).",
				new Parameter[] {
						new FlaggedOption("graphClass", GraphClassParser.getParser(), null, JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java class for the source graph."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final Class<?> graphClass = jsapResult.getClass("graphClass");
		final String basename = jsapResult.getString("basename");

		final ProgressLogger pl = new ProgressLogger();
		pl.logInterval = jsapResult.getLong("logInterval");
		final ImmutableGraph graph;
		// We fetch by reflection the class specified by the user
		if (graphClass != null) graph = (ImmutableGraph)graphClass.getMethod("loadOffline", CharSequence.class).invoke(null, basename);
		else graph = ImmutableGraph.loadOffline(basename, pl);

		final NodeIterator nodeIterator = graph.nodeIterator();
		int count[] = IntArrays.EMPTY_ARRAY;
		int curr, d, maxd = 0, maxNode = 0, mind = Integer.MAX_VALUE, minNode = 0;
		long totd = 0;

		pl.expectedUpdates = graph.numNodes();
		pl.start("Scanning...");

		for(int i = graph.numNodes(); i-- != 0;) {
			curr = nodeIterator.nextInt();
			d = nodeIterator.outdegree();

			if (d < mind) {
				mind = d;
				minNode = curr;
			}

			if (d > maxd){
				maxd = d;
				maxNode = curr;
			}

			totd += d;

			if (d >= count.length) count = IntArrays.grow(count, d + 1);
			count[d]++;

			pl.lightUpdate();
		}

		pl.done();

		System.err.println("The minimum outdegree is " + mind + ", attained by node " + minNode);
		System.err.println("The maximum outdegree is " + maxd + ", attained by node " + maxNode);
		System.err.println("The average outdegree is " + (double)totd / graph.numNodes());

		TextIO.storeInts(count, 0, maxd + 1, System.out);
	}
}
