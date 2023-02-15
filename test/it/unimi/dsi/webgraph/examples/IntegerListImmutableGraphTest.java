/*
 * Copyright (C) 2010-2023 Paolo Boldi and Sebastiano Vigna
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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.WebGraphTestCase;


public class IntegerListImmutableGraphTest extends WebGraphTestCase {

	@Test
	public void test() throws IOException {
		for (final int size: new int[] { 5, 10, 100 })
			for (final double p: new double[] { .1, .3, .5, .9 }) {

				final ErdosRenyiGraph eg = new ErdosRenyiGraph(size, p, true);
				final String filename = File.createTempFile(IntegerListImmutableGraphTest.class.getSimpleName(), "test").getAbsolutePath();
				final DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename));
				dos.writeInt(eg.numNodes());
				final NodeIterator nodeIterator = eg.nodeIterator();
				while (nodeIterator.hasNext()) {
					nodeIterator.nextInt();
					dos.writeInt(nodeIterator.outdegree());
					final LazyIntIterator successors = nodeIterator.successors();
					for (;;) {
						final int succ = successors.nextInt();
						if (succ < 0) break;
						dos.writeInt(succ);
					}
				}
				dos.close();

				final ImmutableGraph graph = IntegerListImmutableGraph.loadOffline(filename);
				assertGraph(graph);
			}
	}

}
