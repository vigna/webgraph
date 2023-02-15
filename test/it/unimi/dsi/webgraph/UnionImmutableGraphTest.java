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

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

public class UnionImmutableGraphTest extends WebGraphTestCase {

	private static final int[] SIZES = { 0, 1, 2, 3, 4, 7, 200 };

	@Test
	public void testUnion() throws IllegalArgumentException {
		for (final int n : SIZES) {
			final ImmutableGraph g = ArrayListMutableGraph.newCompleteGraph(n, false).immutableView();

			// Now split the graph g into two (possibly non-disjoint) graphs
			final ArrayListMutableGraph g0mut = new ArrayListMutableGraph();
			final ArrayListMutableGraph g1mut = new ArrayListMutableGraph();
			g0mut.addNodes(g.numNodes());
			g1mut.addNodes(g.numNodes());
			final it.unimi.dsi.webgraph.NodeIterator nit = g.nodeIterator();
			while (nit.hasNext()) {
				final int from = nit.nextInt();
				final LazyIntIterator succ = nit.successors();
				int d = nit.outdegree();
				while (d-- != 0) {
					final int to = succ.nextInt();
					if (Math.random() < .5) g0mut.addArc(from, to);
					else if (Math.random() < .5) g1mut.addArc(from, to);
					else {
						g0mut.addArc(from, to);
						g1mut.addArc(from, to);
					}
				}
			}
			final ImmutableGraph g0 = g0mut.immutableView();
			final ImmutableGraph g1 = g1mut.immutableView();

			final ImmutableGraph union = Transform.union(g0, g1);
			WebGraphTestCase.assertGraph(union);
			for (int i = 0; i < n; i++) assertArrayEquals(LazyIntIterators.unwrap(union.successors(i)), LazyIntIterators.unwrap(g.successors(i)));
		}
	}
}
