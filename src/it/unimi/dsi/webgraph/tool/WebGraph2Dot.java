package it.unimi.dsi.webgraph.tool;

/*
 * Copyright (C) 2018-2023 Sebastiano Vigna
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

import java.io.IOException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;

/** A tool converting  a graph into <a href="https://www.graphviz.org/">GraphViz</a>'s .dot format. */

public class WebGraph2Dot {
	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(WebGraph2Dot.class.getName(), "Converts a graph into GraphViz's .dot format.", new Parameter[] {
			new Switch("undirected", 'u', "undirected", "Draw as an undirected graph (forget orientation)."),
			new FlaggedOption( "option", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "option", "Text that will be inserted before the node list. It can be specified multiple times and makes it possible to specify any .dot option (e.g., \"node[shape=box]\")" ).setAllowMultipleDeclarations( true ),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final ImmutableGraph graph = ImmutableGraph.loadMapped(basename);
		final int n = graph.numNodes();

		System.out.println("digraph G {");
		if (jsapResult.userSpecified("undirected")) System.out.println("edge[dir=none]");
		for(final String s: jsapResult.getStringArray("option")) System.out.println(s);
		for(int i = 0; i < n; i++) System.out.println("N" + i + "[label=" + i + "];");
		for(final NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext(); ) {
			final int x = nodeIterator.nextInt();
			final LazyIntIterator successors = nodeIterator.successors();
			for(int s; (s = successors.nextInt()) != -1; ) System.out.println("N" + x + " -> N" + s + ";");
		}

		System.out.println("}");
	}
}
