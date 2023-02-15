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

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.webgraph.algo.ConnectedComponentsTest;

public class ConnectedComponentsSlowTest extends WebGraphTestCase {
	@Test
	public void testLarge() throws IOException {
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = Transform.symmetrize(ImmutableGraph.load(path));
		ConnectedComponentsTest.sameComponents(g);
	}
}
