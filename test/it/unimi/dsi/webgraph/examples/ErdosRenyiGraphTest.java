/*
 * Copyright (C) 2003-2021 Paolo Boldi and Sebastiano Vigna
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;



public class ErdosRenyiGraphTest {

	@Test
	public void test() {
		final ImmutableGraph graph = new ErdosRenyiGraph(10000, 1000000, 0, false);
		long arcs = 0;
		for(final NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext();) {
			final int curr = nodeIterator.nextInt();
			final int outdegree = nodeIterator.outdegree();
			arcs += outdegree;
			final int[] s = nodeIterator.successorArray();
			if (outdegree != 0) assertTrue("Node " + curr, s[0] != curr);
			for(int i = 1; i < outdegree; i++) {
				assertTrue(s[i] > s[i - 1]);
				assertTrue(s[i] !=  curr);
			}
		}

		assertEquals((1000000.0 - arcs) / 1000000.0, 0, 1E-2);
	}

	@Test
	public void testBinomialWithoutLoops() {
		final ImmutableGraph g = new ErdosRenyiGraph(5, .5, 0, false);
		new ArrayListMutableGraph(g).immutableView();
	}

	@Test
	public void testCopy() {
		final ImmutableGraph graph = new ErdosRenyiGraph(10000, 1000000, 0, false);
		assertEquals(graph, graph.copy());
		assertEquals(graph.copy(), graph);
	}
}
