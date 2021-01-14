/*
 * Copyright (C) 2010-2020 Paolo Boldi & Sebastiano Vigna
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

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.WebGraphTestCase;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;


public class EliasFanoCumulativeOutdegreeListTest extends WebGraphTestCase {

	@Test
	public void testEliasFano() {
		final ImmutableGraph graph =new ArrayListMutableGraph(new ErdosRenyiGraph(10000, .001, 0, false)).immutableView();
		for(final int mask: new int[] { 0, 1, 3 }) {
			final EliasFanoCumulativeOutdegreeList eliasFanoMonotoneLongBigList = new EliasFanoCumulativeOutdegreeList(graph, graph.numArcs(), mask);
			final int n = graph.numNodes();
			final long m = graph.numArcs();

			for(long i = 1; i < m;) {
				final long s = eliasFanoMonotoneLongBigList.skipTo(i);
				assertEquals(0, eliasFanoMonotoneLongBigList.currentIndex() & mask);
				int j = 0;
				long c = 0;
				while(j < n) if ((c += graph.outdegree(j++)) >= i && (j & mask) == 0) break;
				assertEquals(j, eliasFanoMonotoneLongBigList.currentIndex());
				assertEquals(c, s);
				i = c + 1;
			}

			for(long i = 1; i < m;) {
				final long s = eliasFanoMonotoneLongBigList.skipTo(i);
				assertEquals(0, eliasFanoMonotoneLongBigList.currentIndex() & mask);
				int j = 0;
				long c = 0;
				while(j < n) if ((c += graph.outdegree(j++)) >= i && (j & mask) == 0) break;
				assertEquals(j, eliasFanoMonotoneLongBigList.currentIndex());
				assertEquals(c, s);
				i = c + (m - c) / 2;
			}

			if (mask == 0) {
				long c = 0;
				for(int i = 0; i < n - 1; i++) {
					c += graph.outdegree(i);
					if (graph.outdegree(i) != 0) {
						long s = eliasFanoMonotoneLongBigList.skipTo(c);
						assertEquals(i + 1, eliasFanoMonotoneLongBigList.currentIndex());
						assertEquals(c, s);
						if (graph.outdegree(i + 1) != 0) {
							s = eliasFanoMonotoneLongBigList.skipTo(c + 1);
							assertEquals(i + 2, eliasFanoMonotoneLongBigList.currentIndex());
						}
					}
				}
			}
		}
	}

	@Test
	public void testZeroLength() {
		final ImmutableGraph graph = new ArrayListMutableGraph().immutableView();
		final EliasFanoCumulativeOutdegreeList eliasFanoMonotoneLongBigList = new EliasFanoCumulativeOutdegreeList(graph, graph.numArcs(), 0);
		assertEquals(-1, eliasFanoMonotoneLongBigList.currentIndex());
	}
}
