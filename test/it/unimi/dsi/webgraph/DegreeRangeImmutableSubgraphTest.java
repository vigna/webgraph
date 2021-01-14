/*
 * Copyright (C) 2003-2020 Sebastiano Vigna
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

import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

public class DegreeRangeImmutableSubgraphTest extends WebGraphTestCase {

	@Test
	public void test() {
		for(int i = 10; i < 100000; i *= 10) {
			final double p = 5. / i;
			final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(i, p, 0, false)).immutableView();
			final int[] map = new int[g.numNodes()];
			final int min = 2;
			final int max = 4;
			for(int j = 0, k = 0; j < g.numNodes(); j++)
				map[j] = g.outdegree(j) >= min && g.outdegree(j) < max ? k++ : -1;
			DegreeRangeImmutableSubgraph s = new DegreeRangeImmutableSubgraph(g, min, max);
			assertGraph(s);
			assertEquals(Transform.map(g, map), s);
			s = new DegreeRangeImmutableSubgraph(g, 0, i);
			assertGraph(s);
			assertEquals(g, s);
		}
	}
}
