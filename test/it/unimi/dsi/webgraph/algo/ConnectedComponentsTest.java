/*
 * Copyright (C) 2011-2021 Sebastiano Vigna
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

import org.junit.Test;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.WebGraphTestCase;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;


public class ConnectedComponentsTest extends WebGraphTestCase {
	public static void sameComponents(final ImmutableGraph g) {
		final StronglyConnectedComponentsTarjan stronglyConnectedComponents = StronglyConnectedComponentsTarjan.compute(g, false, new ProgressLogger());
		final int[] size2 = stronglyConnectedComponents.computeSizes();
		stronglyConnectedComponents.sortBySize(size2);

		for(int t = 0; t < 3; t++) {
			final ConnectedComponents connectedComponents = ConnectedComponents.compute(g, t, new ProgressLogger());
			final int[] size = connectedComponents.computeSizes();
			connectedComponents.sortBySize(size);
			for(int i = g.numNodes(); i-- != 0;)
				for(int j = i; j-- != 0;)
					assert((connectedComponents.component[i] == connectedComponents.component[j]) == (stronglyConnectedComponents.component[i] == stronglyConnectedComponents.component[j]));
		}
	}

	@Test
	public void testSmall() {
		sameComponents(ArrayListMutableGraph.newBidirectionalCycle(40).immutableView());
	}

	@Test
	public void testBinaryTree() {
		sameComponents(Transform.symmetrize(ArrayListMutableGraph.newCompleteBinaryIntree(10).immutableView()));
	}

	@Test
	public void testErdosRenyi() {
		for(final int size: new int[] { 10, 100, 1000 })
			for(int attempt = 0; attempt < 5; attempt++)
				sameComponents(Transform.symmetrize(new ArrayListMutableGraph(new ErdosRenyiGraph(size, .001, attempt + 1, true)).immutableView()));
	}
}
