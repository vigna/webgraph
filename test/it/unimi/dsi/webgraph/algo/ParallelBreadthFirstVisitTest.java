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

import org.junit.Test;
import org.slf4j.helpers.NOPLogger;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;

public class ParallelBreadthFirstVisitTest {
	private final ProgressLogger pl = new ProgressLogger(NOPLogger.NOP_LOGGER);

	@Test
	public void testTree() {
		final ImmutableGraph graph = ArrayListMutableGraph.newCompleteBinaryOuttree(10).immutableView();
		final ParallelBreadthFirstVisit visit = new ParallelBreadthFirstVisit(graph, 0, false, pl);
		visit.visit(0);
		final int d[] = new int[graph.numNodes()];
		for(int i = 0; i < visit.cutPoints.size() - 1; i++)
			for(int j = visit.cutPoints.getInt(i); j < visit.cutPoints.getInt(i + 1); j++) d[visit.queue.getInt(j)] = i;
		for(int i = 0; i < graph.numNodes(); i++) assertEquals(Integer.toString(i), Fast.mostSignificantBit(i + 1), d[i]);
	}

	@Test
	public void testStar() {
		final ArrayListMutableGraph graph = new ArrayListMutableGraph(1 + 10 + 100 + 1000);
		for(int i = 1; i <= 10; i++) {
			graph.addArc(0, i);
			graph.addArc(i, 0);
			for(int j = 1; j <= 10; j++) {
				graph.addArc(i, i * 10 + j);
				graph.addArc(i * 10 + j, i);
				for(int k = 1; k <= 10; k++) {
					graph.addArc(i * 10 + j, (i * 10 + j) * 10 + k);
					graph.addArc((i * 10 + j) * 10 + k, i * 10 + j);
				}
			}
		}

		final ParallelBreadthFirstVisit visit = new ParallelBreadthFirstVisit(graph.immutableView(), 0, false, pl);
		final int componentSize = visit.visit(0);
		for(int i = 1; i < graph.numNodes(); i++) {
			visit.clear();
			assertEquals("Source: " + i, componentSize, visit.visit(i));
		}
	}
}
