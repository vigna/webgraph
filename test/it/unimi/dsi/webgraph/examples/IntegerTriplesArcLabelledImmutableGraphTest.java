/*
 * Copyright (C) 2010-2021 Paolo Boldi and Sebastiano Vigna
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

import org.junit.Test;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.WebGraphTestCase;

public class IntegerTriplesArcLabelledImmutableGraphTest extends WebGraphTestCase {

	@Test
	public void testEmpty() {
		final ImmutableGraph g = new IntegerTriplesArcLabelledImmutableGraph(new int[][] {});

		assertGraph(g);
	}

	@Test
	public void testCycle() {
		final ImmutableGraph g = new IntegerTriplesArcLabelledImmutableGraph(new int[][] {
				{ 0, 1, 2 },
				{ 1, 2, 0 },
				{ 2, 0, 1 },

		});

		assertGraph(g);
	}

}
