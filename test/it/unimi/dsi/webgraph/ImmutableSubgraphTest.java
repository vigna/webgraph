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

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class ImmutableSubgraphTest extends WebGraphTestCase {

	@Test
	public void testSubgraphs() {
		ImmutableGraph g, sg;
		final long seed = System.currentTimeMillis();
		System.err.println("Seed: " + seed);
		final Random random = new Random(seed);

		for(int n = 1; n < 10; n++) { // Graph construction parameter
			g = ArrayListMutableGraph.newCompleteGraph(n, false).immutableView();
			final int[] randPerm = new int[n];
			for(int i = n; i-- != 0;) randPerm[i] = i;
			Collections.shuffle(IntArrayList.wrap(randPerm), random);

			for(int s = 1; s <= n; s++) {
				Arrays.sort(randPerm, 0, s);
				final int nodes[] = new int[s];
				System.arraycopy(randPerm, 0, nodes, 0, s);
				sg = new ImmutableSubgraph(g, nodes);
				assertGraph(sg);
				final ArrayListMutableGraph completeGraph = ArrayListMutableGraph.newCompleteGraph(s, false);
				assertEquals(completeGraph.immutableView(), sg);
				assertEquals(sg, ImmutableSubgraph.asImmutableSubgraph(completeGraph.immutableView()));
				assertEquals(sg.hashCode(), completeGraph.hashCode());
			}

			g = ArrayListMutableGraph.newCompleteBinaryIntree(n).immutableView();
			for(int s = 1; s <= n; s++) {
				final int[] nodes = new int[(1 << s) - 1];
				for(int j = (1 << s) - 1; j-- != 0;) nodes[j] = j;
				sg = new ImmutableSubgraph(g, nodes);
				assertGraph(sg);
				final ArrayListMutableGraph completeBinaryIntree = ArrayListMutableGraph.newCompleteBinaryIntree(s - 1);
				final ImmutableGraph immutableView = completeBinaryIntree.immutableView();
				assertEquals(immutableView, sg);
				assertEquals(sg, ImmutableSubgraph.asImmutableSubgraph(immutableView));
				assertEquals(sg.hashCode(), completeBinaryIntree.hashCode());
			}

			g = ArrayListMutableGraph.newCompleteBinaryOuttree(n).immutableView();
			for(int s = 1; s <= n; s++) {
				final int[] nodes = new int[(1 << s) - 1];
				for(int j = (1 << s) - 1; j-- != 0;) nodes[j] = j;
				sg = new ImmutableSubgraph(g, nodes);
				assertGraph(sg);

				final ArrayListMutableGraph completeBinaryOuttree = ArrayListMutableGraph.newCompleteBinaryOuttree(s - 1);
				final ImmutableGraph immutableView = completeBinaryOuttree.immutableView();
				assertEquals(immutableView, sg);
				assertEquals(sg, ImmutableSubgraph.asImmutableSubgraph(immutableView));
				assertEquals(sg.hashCode(), completeBinaryOuttree.hashCode());
			}

		}
	}


}
