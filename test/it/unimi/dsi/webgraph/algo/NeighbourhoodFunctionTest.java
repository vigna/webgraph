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

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.WebGraphTestCase;


public class NeighbourhoodFunctionTest extends WebGraphTestCase {

	@Test
	public void testClique() {
		final ImmutableGraph g = ArrayListMutableGraph.newCompleteGraph(10, false).immutableView();
		final double[] computeNeighbourhoodFunction = NeighbourhoodFunction.compute(g);
		assertEquals(2, computeNeighbourhoodFunction.length);
		assertEquals(10, computeNeighbourhoodFunction[0], Double.MIN_VALUE);
		assertEquals(100, computeNeighbourhoodFunction[1], Double.MIN_VALUE);
	}

	@Test
	public void testCycle() {
		final ImmutableGraph g = ArrayListMutableGraph.newDirectedCycle(5).immutableView();
		final double[] computeNeighbourhoodFunction = NeighbourhoodFunction.compute(g);
		assertEquals(5, computeNeighbourhoodFunction.length);
		assertEquals(5, computeNeighbourhoodFunction[0], Double.MIN_VALUE);
		assertEquals(10, computeNeighbourhoodFunction[1], Double.MIN_VALUE);
		assertEquals(15, computeNeighbourhoodFunction[2], Double.MIN_VALUE);
		assertEquals(20, computeNeighbourhoodFunction[3], Double.MIN_VALUE);
		assertEquals(25, computeNeighbourhoodFunction[4], Double.MIN_VALUE);
	}

	@Test
	public void testTree() {
		ImmutableGraph g = ArrayListMutableGraph.newCompleteBinaryIntree(1).immutableView();
		double[] computeNeighbourhoodFunction = NeighbourhoodFunction.compute(g);
		assertEquals(2, computeNeighbourhoodFunction.length);
		assertEquals(3, computeNeighbourhoodFunction[0], Double.MIN_VALUE);
		assertEquals(5, computeNeighbourhoodFunction[1], Double.MIN_VALUE);
		 g = ArrayListMutableGraph.newCompleteBinaryIntree(3).immutableView();
		computeNeighbourhoodFunction = NeighbourhoodFunction.compute(g);
		assertEquals(4, computeNeighbourhoodFunction.length);
		assertEquals(15, computeNeighbourhoodFunction[0], Double.MIN_VALUE);
		assertEquals(29, computeNeighbourhoodFunction[1], Double.MIN_VALUE);
		assertEquals(41, computeNeighbourhoodFunction[2], Double.MIN_VALUE);
		assertEquals(49, computeNeighbourhoodFunction[3], Double.MIN_VALUE);
	}

	@Test
	public void testMedian() {
		assertEquals(1, NeighbourhoodFunction.medianDistance(2, new double[] { 2, 4 }), 0);
		assertEquals(Double.POSITIVE_INFINITY, NeighbourhoodFunction.medianDistance(3, new double[] { 3, 4 }), 0);
		assertEquals(1, NeighbourhoodFunction.medianDistance(3, new double[] { 3, 6, 8 }), 0);
		assertEquals(2, NeighbourhoodFunction.medianDistance(3, new double[] { 3, 4, 5, 6 }), 0);
		assertEquals(0, NeighbourhoodFunction.medianDistance(1, new double[] { 1 }), 0);
		assertEquals(Double.POSITIVE_INFINITY, NeighbourhoodFunction.medianDistance(2, new double[] { 2 }), 0);
		assertEquals(Double.POSITIVE_INFINITY, NeighbourhoodFunction.medianDistance(3, new double[] { 3 }), 0);
	}
}
