/*
 * Copyright (C) 2011-2020 Sebastiano Vigna
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.CliqueGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.WebGraphTestCase;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

@Deprecated
public class FourSweepIterativeFringeDiameterTest extends WebGraphTestCase {

	@Test
	public void testSmall() {
		final ImmutableGraph g = ArrayListMutableGraph.newBidirectionalCycle(40).immutableView();
		assertEquals(20, FourSweepIterativeFringeDiameter.run(g, 0, new ProgressLogger(), 0));
	}

	@Test
	public void testCycleOfCliques() {
		final ArrayListMutableGraph mg = new ArrayListMutableGraph();
		// Creates a bidirectional cycle of k n-cliques, each connected with the next by 2*b<n arcs
		// Expected diameter: k + 1
		final int n = 20, k = 100, b = 6;
		mg.addNodes(n * k);
		for (int i = 0; i < k; i++)
			for (int j = 0; j < n; j++)
				for (int h = 0; h < n; h++)
					mg.addArc(n * i + j, n * i + h);
		for (int i = 0; i < k; i++)
			for (int j = 0; j < b; j++) {
				mg.addArc(n * i + j, n * ((i + 1) % k) + n - 1 - j);
				mg.addArc(n * ((i + 1) % k) + n - 1 - j, n * i + j);
			}
		final ImmutableGraph g = mg.immutableView();

		assertEquals(k + 1, FourSweepIterativeFringeDiameter.run(g, 0, null, 0));
	}

	@Test
	public void testCliqueGraph() {
		assertEquals(12, FourSweepIterativeFringeDiameter.run(new CliqueGraph(100, 5), 0, new ProgressLogger(), 0));
	}

	@Test
	public void testBinaryTree() {
		assertEquals(20, FourSweepIterativeFringeDiameter.run(Transform.symmetrize(ArrayListMutableGraph.newCompleteBinaryIntree(10).immutableView()), 0, new ProgressLogger(), 0));
	}

	@Test
	public void testErdosRenyi() {
		assertEquals(2, FourSweepIterativeFringeDiameter.run(Transform.symmetrize(new ArrayListMutableGraph(new ErdosRenyiGraph(1000, .5, 0, false)).immutableView()), 0, new ProgressLogger(), 0));
	}

	@Test
	public void testLarge() throws IOException {
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = Transform.symmetrize(ImmutableGraph.load(path));
		assertEquals(34, FourSweepIterativeFringeDiameter.run(g, 0, new ProgressLogger(), 0));
	}
}
