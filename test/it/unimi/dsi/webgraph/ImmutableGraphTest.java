/*
 * Copyright (C) 2010-2020 Paolo Boldi, Sebastiano Vigna
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

package it.unimi.dsi.webgraph;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.util.XoRoShiRo128PlusRandomGenerator;
import it.unimi.dsi.webgraph.Transform.ArcFilter;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;



public class ImmutableGraphTest {

	public final static boolean DEBUG = false;

	public void assertSplitIterators(final String graphFilename) throws IOException {
		final XoRoShiRo128PlusRandomGenerator r = new XoRoShiRo128PlusRandomGenerator(0);
		int i = 0;
		for (;;) {
			final ImmutableGraph g = ImmutableGraph.loadOffline(graphFilename);
			switch (i) {
			case 0:
				WebGraphTestCase.assertSplitIterator(g, 4);
				i++; break;
			case 1:
				WebGraphTestCase.assertSplitIterator(g, 1);
				i++; break;
			case 2:
				if (g.numNodes() / 4 > 0) WebGraphTestCase.assertSplitIterator(g, Math.max(1, g.numNodes() / 4));
				i++; break;
			case 3:
				WebGraphTestCase.assertSplitIterator(g, g.numNodes());
				i++; break;
			case 4:
				WebGraphTestCase.assertSplitIterator(g, Math.max(1, g.numNodes() / (r.nextInt(10) + 1)));
				i++; break;
			case 5:
				WebGraphTestCase.assertSplitIterator(g, r.nextInt(10) + 1);
				i++; break;
			default:
				return;
			}
		}
	}

	@Test
	public void testBVGraphSplitIteratorsOffline() throws IllegalArgumentException, SecurityException, IOException {
		for (final int size: new int[] { 5, 10, 100 })
			for (final double p: new double[] { .1, .3, .5, .9 }) {
				final ErdosRenyiGraph eg = new ErdosRenyiGraph(size, p, true);
				final File graphFile = BVGraphTest.storeTempGraph(eg);
				ImmutableGraph graph;
				graph = ImmutableGraph.load(graphFile.getAbsolutePath());
				WebGraphTestCase.assertGraph(graph);
				graph = ImmutableGraph.loadOffline(graphFile.getAbsolutePath());
				WebGraphTestCase.assertGraph(graph);
				assertSplitIterators(graphFile.getAbsolutePath());
				graphFile.delete();
			}
	}

	@Test
	public void testTransformFilterSplitIterators() throws IllegalArgumentException, SecurityException, IOException {
		final XoRoShiRo128PlusRandomGenerator r = new XoRoShiRo128PlusRandomGenerator(0);
		for (final int size: new int[] { 5, 10, 100 })
			for (final double p: new double[] { .1, .3, .5, .9 }) {
				final ErdosRenyiGraph eg = new ErdosRenyiGraph(size, p, true);
				final File graphFile = BVGraphTest.storeTempGraph(eg);
				ImmutableGraph graph;
				graph = ImmutableGraph.load(graphFile.getAbsolutePath());
				final ImmutableGraph filteredArcs = Transform.filterArcs(graph, (ArcFilter) (i, j) -> i % 3 == 1 && j % 5 > 3);
				WebGraphTestCase.assertSplitIterator(filteredArcs, Math.max(1, r.nextInt(size)));
			}
	}

	@Test
	public void testImmutableSubgraphSplitIterators() throws IllegalArgumentException, SecurityException, IOException {
		final XoRoShiRo128PlusRandomGenerator r = new XoRoShiRo128PlusRandomGenerator(2);
		for (final int size: new int[] { 5, 10, 100 })
			for (final double p: new double[] { .1, .3, .5, .9 }) {
				final ErdosRenyiGraph eg = new ErdosRenyiGraph(size, p, true);
				final File graphFile = BVGraphTest.storeTempGraph(eg);
				ImmutableGraph graph;
				graph = ImmutableGraph.load(graphFile.getAbsolutePath());

				final IntSet nodeSet = new IntOpenHashSet();
				for (int i = 0; i < size; i++) if (r.nextBoolean()) nodeSet.add(i);
				if (nodeSet.isEmpty()) nodeSet.add(r.nextInt(size));
				final int[] nodeArray = nodeSet.toIntArray();
				Arrays.sort(nodeArray);
				WebGraphTestCase.assertSplitIterator(new ImmutableSubgraph(graph, nodeArray), Math.max(1, r.nextInt(nodeArray.length)));
			}
	}

	@Test
	public void testUnionImmutableGraphSplitIterators() throws IllegalArgumentException, SecurityException, IOException {
		final XoRoShiRo128PlusRandomGenerator r = new XoRoShiRo128PlusRandomGenerator(0);
		for (final int size: new int[] { 5, 10, 100 })
			for (final double p: new double[] { .1, .3, .5, .9 }) {
				final ErdosRenyiGraph eg0 = new ErdosRenyiGraph(size, p, true);
				final File graphFile0 = BVGraphTest.storeTempGraph(eg0);
				ImmutableGraph graph0;
				graph0 = ImmutableGraph.load(graphFile0.getAbsolutePath());
				final ErdosRenyiGraph eg1 = new ErdosRenyiGraph(size, p, true);
				final File graphFile1 = BVGraphTest.storeTempGraph(eg1);
				ImmutableGraph graph1;
				graph1 = ImmutableGraph.load(graphFile1.getAbsolutePath());
				WebGraphTestCase.assertSplitIterator(new UnionImmutableGraph(graph0, graph1), Math.max(1, r.nextInt(graph0.numNodes() + graph1.numNodes())));
			}
	}
}
