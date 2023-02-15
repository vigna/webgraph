/*
 * Copyright (C) 2010-2023 Paolo Boldi & Sebastiano Vigna
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

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.WebGraphTestCase;


public class EstimateEffectiveDiameterSlowTest extends WebGraphTestCase {

	@Test
	public void testLarge() throws IOException {
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = ImmutableGraph.load(path);
		final HyperBall hyperBall = new HyperBall(g, 8, 0);
		hyperBall.run(Integer.MAX_VALUE, -1);
		assertEquals(NeighbourhoodFunction.effectiveDiameter(.9, HyperBallSlowTest.cnr2000NF), NeighbourhoodFunction.effectiveDiameter(.9, hyperBall.neighbourhoodFunction.toDoubleArray()), 1);
		hyperBall.close();
		deleteGraph(path);
	}
}
