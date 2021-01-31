/*
 * Copyright (C) 2010-2021 Paolo Boldi & Sebastiano Vigna
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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.CliqueGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.WebGraphTestCase;


public class EstimateEffectiveDiameterTest extends WebGraphTestCase {

	@Test
	public void testSmall() throws IOException {
		final ImmutableGraph g = ArrayListMutableGraph.newBidirectionalCycle(40).immutableView();

		final HyperBall hyperBall = new HyperBall(g, 8, 0);
		hyperBall.run(Integer.MAX_VALUE, -1);
 		assertEquals(17, NeighbourhoodFunction.effectiveDiameter(.9, hyperBall.neighbourhoodFunction.toDoubleArray()), 1);
 		hyperBall.close();
	}

	@Test
	public void testCycleOfCliques() throws IOException {
		final ArrayListMutableGraph mg = new ArrayListMutableGraph();
		// Creates a bidirectional cycle of k n-cliques, each connected with the next by 2*b<n arcs
		// Expected diameter: k + 1
		final int n = 20, k = 100, b = 6;
		mg.addNodes(n * k);
		for (int i = 0; i < k; i++)
			for (int j = 0; j < n; j++)
				for (int h = 0; h < n; h++)
					mg.addArc(n * i + j, n * i + h);
		for (int i = 0; i < k; i++)
			for (int j = 0; j < b; j++) {
				mg.addArc(n * i + j, n * ((i + 1) % k) + n - 1 - j);
				mg.addArc(n * ((i + 1) % k) + n - 1 - j, n * i + j);
			}
		final ImmutableGraph g = mg.immutableView();

		final HyperBall hyperBall = new HyperBall(g, 8, 0);
		hyperBall.run(Integer.MAX_VALUE, -1);
		final double estimation = NeighbourhoodFunction.effectiveDiameter(1, hyperBall.neighbourhoodFunction.toDoubleArray());
		final double expected = k + 1;
		final double relativeError = Math.abs(estimation - expected) / expected;
		System.err.println("Estimate: " + estimation);
		System.err.println("Relative error in estimate (should be <0.05): " + relativeError);
		assertTrue(relativeError < 0.05); // Accept error within 5%

		hyperBall.close();
	}

	@Test
	public void testTwoCyclesOfCliques() throws IOException {
		final ArrayListMutableGraph mg = new ArrayListMutableGraph();
		// Creates two bidirectional cycles of k n-cliques (kx nx-cliques, resp.), each connected with the next by 2*b<n (2*bx<nx, resp.) arcs
		// We expect that more than 90% of the pairs are within distance k or kx, so the effective diameter should be k
		final int n = 16, k = 10, b = 6;
		final int firstNodeSecondClique = n * k;
		final int nx = 3, kx = 5, bx = 1;
		mg.addNodes(n * k + nx * kx);
		for (int i = 0; i < k; i++)
			for (int j = 0; j < n; j++)
				for (int h = 0; h < n; h++)
					mg.addArc(n * i + j, n * i + h);
		for (int i = 0; i < k; i++)
			for (int j = 0; j < b; j++) {
				mg.addArc(n * i + j, n * ((i + 1) % k) + n - 1 - j);
				mg.addArc(n * ((i + 1) % k) + n - 1 - j, n * i + j);
			}
		for (int i = 0; i < kx; i++)
			for (int j = 0; j < nx; j++)
				for (int h = 0; h < nx; h++)
					mg.addArc(firstNodeSecondClique + nx * i + j, firstNodeSecondClique + nx * i + h);
		for (int i = 0; i < kx; i++)
			for (int j = 0; j < bx; j++) {
				mg.addArc(firstNodeSecondClique + nx * i + j, firstNodeSecondClique + nx * ((i + 1) % kx) + nx - 1 - j);
				mg.addArc(firstNodeSecondClique + nx * ((i + 1) % kx) + nx - 1 - j, firstNodeSecondClique + nx * i + j);
			}
		final HyperBall hyperBall = new HyperBall(mg.immutableView(), 8, 0);
		hyperBall.run(Integer.MAX_VALUE, -1);

		assertEquals(k, NeighbourhoodFunction.effectiveDiameter(.99, hyperBall.neighbourhoodFunction.toDoubleArray()), 1);

		hyperBall.close();
	}


	@Test
	public void testCliqueGraph() throws IOException {
		final HyperBall hyperBall = new HyperBall(new CliqueGraph(100, 5), 8, 0);
		hyperBall.run(1000, 1E-3);
		hyperBall.close();
	}

	@Test
	public void testLarge() throws IOException {
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = ImmutableGraph.load(path);
		final HyperBall hyperBall = new HyperBall(g, 8, 0);
		hyperBall.run(Integer.MAX_VALUE, -1);
		assertEquals(NeighbourhoodFunction.effectiveDiameter(.9, HyperBallSlowTest.cnr2000NF), NeighbourhoodFunction.effectiveDiameter(.9, hyperBall.neighbourhoodFunction.toDoubleArray()), 1);
		hyperBall.close();
	}
}
