/*
 * Copyright (C) 2011-2023 Sebastiano Vigna
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

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.algo.StronglyConnectedComponents;
import it.unimi.dsi.webgraph.algo.StronglyConnectedComponentsTarjan;
import it.unimi.dsi.webgraph.algo.StronglyConnectedComponentsTest;

public class StronglyConnectedComponentsSlowTest extends WebGraphTestCase {

	@Test
	public void testLarge() throws IOException {
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = ImmutableGraph.load(path);
		final StronglyConnectedComponentsTarjan componentsRecursive = StronglyConnectedComponentsTarjan.compute(g, true, new ProgressLogger());
		final StronglyConnectedComponents componentsIterative = StronglyConnectedComponents.compute(g, true, new ProgressLogger());
		assertEquals(componentsRecursive.numberOfComponents, componentsIterative.numberOfComponents);
		StronglyConnectedComponentsTest.sameComponents(g.numNodes(), componentsRecursive, componentsIterative);
		deleteGraph(path);
	}

}
