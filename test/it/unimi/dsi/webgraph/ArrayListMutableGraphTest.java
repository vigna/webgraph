/*
 * Copyright (C) 2007-2023 Sebastiano Vigna
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntIterator;

public class ArrayListMutableGraphTest extends WebGraphTestCase {

	@Test
	public void testConstructor() throws IllegalArgumentException, SecurityException {
		for(int n = 1; n < 8; n++) {
			for(int type = 0; type < 3; type++) {
				System.err.println("Testing type " + type + ", n=" + n + "...");
				final ArrayListMutableGraph g = type == 0 ? ArrayListMutableGraph.newCompleteGraph(n, false) :
					type == 1 ? ArrayListMutableGraph.newCompleteBinaryIntree(n) :
						ArrayListMutableGraph.newCompleteBinaryOuttree(n);
				final ImmutableGraph immutableView = g.immutableView();
				assertGraph(immutableView);
				assertEquals(g, new ArrayListMutableGraph(immutableView));
				final int[][] arc = new int[(int)g.numArcs()][2];
				for(int i = 0, k = 0; i < g.numNodes(); i++)
					for(final IntIterator successors = g.successors(i); successors.hasNext();)
						arc[k++] = new int[] { i, successors.nextInt() };

				assertEquals(g, new ArrayListMutableGraph(g.numNodes(), arc));
			}
		}
	}

	@Test
	public void testHashCode() {
		final ArrayListMutableGraph g = ArrayListMutableGraph.newCompleteGraph(10, false);
		assertEquals(g.immutableView().hashCode(), g.hashCode());

	}
}
