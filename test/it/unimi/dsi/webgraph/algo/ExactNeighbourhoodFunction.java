/*
 * Copyright (C) 2010-2020 Sebastiano Vigna
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

package it.unimi.dsi.webgraph.algo;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

public class ExactNeighbourhoodFunction {
	private static final Logger LOGGER = LoggerFactory.getLogger(NeighbourhoodFunction.class);
	private final ImmutableGraph graph;
	private final ProgressLogger pl;
	private final int n;
	private LongArrayBitVector[] neighbours;

	public ExactNeighbourhoodFunction(final ImmutableGraph graph, final ProgressLogger pl) {
		this.graph = graph;
		// TODO Auto-generated constructor stub
		this.pl = pl;
		this.n = graph.numNodes();
		this.neighbours = new LongArrayBitVector[n];
		for(int i = n; i-- != 0;) (neighbours[i] = LongArrayBitVector.ofLength(n)).set(i);
	}

	public double[] neighbourhoodFunction() {
		final DoubleArrayList neighbourhoodFunction = new DoubleArrayList();
		neighbourhoodFunction.add(n);
		long prevCount = -1, count;
		for(;;) {
			final LongArrayBitVector[] newNeighbours = new LongArrayBitVector[n];
			for(int i = n; i-- != 0;) newNeighbours[i] = LongArrayBitVector.copy(neighbours[i]);
			final NodeIterator nodeIterator = graph.nodeIterator();
			for(int i = 0; i < n; i++) {
				nodeIterator.nextInt();
				final int d = nodeIterator.outdegree();
				final int[] successor = nodeIterator.successorArray();

				for(int j = 0; j < d; j++) newNeighbours[i].or(neighbours[successor[j]]);

			}

			neighbours = newNeighbours;

			count = 0;
			for(int j = 0; j < n; j++) count += newNeighbours[j].count();
			if (prevCount == count) break;
			prevCount = count;

			neighbourhoodFunction.add(count);

			if (pl != null) {
				pl.update();
				pl.logger().info("Pairs: " + count);
			}
		}

		return neighbourhoodFunction.toDoubleArray();
	}

	public static void main(final String arg[]) throws IOException, JSAPException, IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
		final SimpleJSAP jsap = new SimpleJSAP(ExactNeighbourhoodFunction.class.getName(), "Prints the neighbourhood function.",
			new Parameter[] {
				new Switch("spec", 's', "spec", "The source is not a basename but rather a specification of the form <ImmutableGraphImplementation>(arg,arg,...)."),
				new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean spec = jsapResult.getBoolean("spec");
		final String basename = jsapResult.getString("basename");
		final ProgressLogger pl = new ProgressLogger(LOGGER);

		final ImmutableGraph graph = spec ? ObjectParser.fromSpec(basename, ImmutableGraph.class, GraphClassParser.PACKAGE) : ImmutableGraph.loadOffline(basename);

		final ExactNeighbourhoodFunction neighbourhoodFunction = new ExactNeighbourhoodFunction(graph, pl);
		pl.start("Computing...");
		TextIO.storeDoubles(neighbourhoodFunction.neighbourhoodFunction(), System.out);
		pl.done();
	}
}
