/*
 * Copyright (C) 2013-2023 Sebastiano Vigna
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

public class IncrementalImmutableSequentialGraphTest extends WebGraphTestCase {

	@Test
	public void testErdosRenyi() throws IOException, InterruptedException, ExecutionException {
		final String basename = File.createTempFile(IncrementalImmutableSequentialGraph.class.getSimpleName() + "-", "-temp").toString();
		for(final int size: new int[] { 10, 100, 1000, 10000 }) {
			final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(size, .001, 0, false)).immutableView();
			final IncrementalImmutableSequentialGraph incrementalImmutableSequentialGraph = new IncrementalImmutableSequentialGraph();
			final Future<Void> future = Executors.newSingleThreadExecutor().submit(() -> {
				BVGraph.store(incrementalImmutableSequentialGraph, basename);
				return null;
			});

			for(final NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
				nodeIterator.nextInt();
				incrementalImmutableSequentialGraph.add(nodeIterator.successorArray(), 0, nodeIterator.outdegree());
			}

			incrementalImmutableSequentialGraph.add(IncrementalImmutableSequentialGraph.END_OF_GRAPH);

			future.get();
			assertEquals(g, ImmutableGraph.load(basename));
		}

		deleteGraph(basename);
	}

}
