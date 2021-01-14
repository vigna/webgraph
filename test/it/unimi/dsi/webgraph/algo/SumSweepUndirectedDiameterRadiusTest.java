/*
 * Copyright (C) 2015-2020 Sebastiano Vigna
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

import org.junit.Test;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.algo.SumSweepUndirectedDiameterRadius.OutputLevel;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

public class SumSweepUndirectedDiameterRadiusTest {

	@Test
	public void testPath() {
		final ImmutableGraph graph = new ArrayListMutableGraph(3,
				new int[][] { { 0, 1 }, { 1, 2 }, { 2, 1 }, { 1, 0 } }).immutableView();

		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(graph, OutputLevel.ALL,
				new ProgressLogger());
		ss.compute();

		assertEquals(ss.getEccentricity(0), 2);
		assertEquals(ss.getEccentricity(1), 1);
		assertEquals(ss.getEccentricity(2), 2);
		assertEquals(ss.getDiameter(), 2);
		assertEquals(ss.getRadius(), 1);
		assertEquals(ss.getRadialVertex(), 1);
		assertTrue(ss.getDiametralVertex() == 2 || ss.getDiametralVertex() == 0);
	}

	@Test
	public void testStar() {
		final ImmutableGraph graph = Transform.symmetrize(new ArrayListMutableGraph(9,
				new int[][] { { 0, 1 }, { 1, 2 }, { 0, 3 }, { 3, 4 }, { 0, 5 }, { 5, 6 }, { 0, 7 }, { 7, 8 } })
						.immutableView());

		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(graph, OutputLevel.ALL,
				new ProgressLogger());
		ss.compute();

		assertEquals(ss.getEccentricity(0), 2);
		assertEquals(ss.getEccentricity(1), 3);
		assertEquals(ss.getEccentricity(2), 4);
		assertEquals(ss.getEccentricity(3), 3);
		assertEquals(ss.getEccentricity(4), 4);
		assertEquals(ss.getEccentricity(5), 3);
		assertEquals(ss.getEccentricity(6), 4);
		assertEquals(ss.getEccentricity(7), 3);
		assertEquals(ss.getEccentricity(8), 4);

		assertEquals(ss.getDiameter(), 4);
		assertEquals(ss.getRadius(), 2);
		assertEquals(ss.getRadialVertex(), 0);
	}

	@Test
	public void testLozenge() {
		final ImmutableGraph graph = Transform.symmetrize(
				new ArrayListMutableGraph(4, new int[][] { { 0, 1 }, { 1, 0 }, { 0, 2 }, { 1, 3 }, { 2, 3 } })
						.immutableView());

		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(graph, OutputLevel.RADIUS,
				new ProgressLogger());

		ss.compute();
		assertEquals(ss.getRadius(), 2);
		assertTrue(ss.getEccentricity(ss.getRadialVertex()) == ss.getRadius());
	}

	@Test
	public void testCycle() {
		for (final int size : new int[] { 3, 5, 7 }) {
			final ImmutableGraph graph = ArrayListMutableGraph.newBidirectionalCycle(size).immutableView();
			final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(graph,
					OutputLevel.RADIUSDIAMETER, new ProgressLogger());
			ss.compute();

			assertEquals(ss.getDiameter(), size / 2);
			assertEquals(ss.getRadius(), size / 2);

			assertTrue(ss.getEccentricity(ss.getRadialVertex()) == ss.getRadius());
			assertTrue(ss.getEccentricity(ss.getDiametralVertex()) == ss.getDiameter());
		}
	}

	@Test
	public void testClique() {
		for (final int size : new int[] { 10, 50, 100 }) {
			final ImmutableGraph graph = ArrayListMutableGraph.newCompleteGraph(size, false).immutableView();

			final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(graph, OutputLevel.ALL,
					new ProgressLogger());
			ss.compute();

			for (int i = 0; i < size; i++) {
				assertEquals(ss.getEccentricity(i), 1);
			}
			assertEquals(ss.getDiameter(), 1);
			assertEquals(ss.getRadius(), 1);
		}
	}

	@Test
	public void testSparse() {
		final ImmutableGraph emptygraph = new ArrayListMutableGraph(100, new int[][] {}).immutableView();

		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(emptygraph, OutputLevel.ALL,
				null);
		ss.compute();
		assertEquals(ss.getRadius(), 0);
		assertEquals(ss.getDiameter(), 0);

		final ImmutableGraph graphfewedges = Transform.symmetrize(
				new ArrayListMutableGraph(100, new int[][] { { 10, 32 }, { 10, 65 }, { 65, 10 }, { 21, 44 } })
						.immutableView());
		final SumSweepUndirectedDiameterRadius ss1 = new SumSweepUndirectedDiameterRadius(graphfewedges,
				OutputLevel.RADIUS, null);
		ss1.compute();
		assertEquals(ss1.getRadius(), 0);
	}

	public int[] computeAllEccentricities(final ImmutableGraph g) {
		final int[] ecc = new int[g.numNodes()];
		for (int v = 0; v < g.numNodes(); v++) {
			final ParallelBreadthFirstVisit bfs = new ParallelBreadthFirstVisit(g, v, true, null);
			bfs.visit(v, -1);
			ecc[v] = bfs.cutPoints.size() - 2;
		}
		return ecc;
	}

	@Test
	public void testRandomSumSweepHeuristic() {
		for (final double p : new double[] { .1, .2, .5, .7 }) {
			for (final int size : new int[] { 10, 30, 50 }) {
				final ImmutableGraph graph = Transform
						.symmetrize(new ArrayListMutableGraph(new ErdosRenyiGraph(size, p, 0, false)).immutableView());
				final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(graph, OutputLevel.ALL,
						new ProgressLogger());
				ss.sumSweepHeuristic((int) (Math.random() * size), Math.min(10, (int) (2 + Math.random() * size)));

				final int[] ecc = computeAllEccentricities(graph);

				for (int v = 0; v < size; v++) {
					assertTrue(ss.l[v] <= ecc[v]);
					assertTrue(ss.u[v] >= ecc[v]);
					assertTrue(ss.ecc[v] == -1 || ss.ecc[v] == ecc[v]);
				}
			}
		}
	}

	@Test
	public void testRandom() {
		for (int t = 0; t < 100; t++) {
			final double p = Math.random();
			if (p < 1.0E-12) {
				continue;
			}
			final int size = (int) (Math.random() * 50) + 2;

			final ImmutableGraph graph = Transform
					.symmetrize(new ArrayListMutableGraph(new ErdosRenyiGraph(size, p, 0, false)).immutableView());

			OutputLevel output;
			switch ((int) (Math.random() * 4)) {
			case 0:
				output = OutputLevel.RADIUS;
				break;
			case 1:
				output = OutputLevel.DIAMETER;
				break;
			case 2:
				output = OutputLevel.RADIUSDIAMETER;
				break;
			default:
				output = OutputLevel.ALL;
				break;
			}

			final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(graph, output, null);
			ss.compute();
			final int[] ecc = computeAllEccentricities(graph);
			int D = 0, R = size;
			for (int v = 0; v < size; v++) {
				D = Math.max(D, ecc[v]);
				R = Math.min(R, ecc[v]);
			}
			if (output == OutputLevel.RADIUS || output == OutputLevel.RADIUSDIAMETER || output == OutputLevel.ALL) {
				assertEquals(ss.getRadius(), R);
				assertEquals(ss.ecc[ss.getRadialVertex()], R);
			}
			if (output == OutputLevel.DIAMETER || output == OutputLevel.RADIUSDIAMETER || output == OutputLevel.ALL) {
				assertEquals(ss.getDiameter(), D);
				assertTrue(ss.ecc[ss.getDiametralVertex()] == D);
			}

			if (output == OutputLevel.ALL) {
				for (int v = 0; v < size; v++) {
					assertEquals(ss.ecc[v], ecc[v]);
				}
			}
		}
	}

	@Test
	public void testEmptyGraph() {
		final ImmutableGraph g = new ArrayListMutableGraph(0, new int[][] {}).immutableView();
		// @SuppressWarnings("unused")
		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(g, OutputLevel.ALL,
				new ProgressLogger());
		ss.compute();
		assertEquals(ss.getRadius(), Integer.MAX_VALUE);
		assertEquals(ss.getDiameter(), 0);

		final ImmutableGraph h = new ArrayListMutableGraph(2, new int[][] {}).immutableView();
		// @SuppressWarnings("unused")
		final SumSweepUndirectedDiameterRadius ssh = new SumSweepUndirectedDiameterRadius(h, OutputLevel.ALL,
				new ProgressLogger());
		ssh.compute();
		assertEquals(ssh.getRadius(), 0);
		assertEquals(ssh.getDiameter(), 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNonSymmetricGraph() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		@SuppressWarnings("unused")
		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(g, OutputLevel.RADIUS,
				new ProgressLogger());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotComputedR() {
		final ImmutableGraph g = Transform
				.symmetrize(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView());
		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(g, OutputLevel.RADIUS,
				new ProgressLogger());
		ss.sumSweepHeuristic(0, 1);
		ss.getRadius();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotComputedD() {
		final ImmutableGraph g = Transform
				.symmetrize(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView());
		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(g, OutputLevel.RADIUS,
				new ProgressLogger());
		ss.sumSweepHeuristic(0, 1);
		ss.getDiameter();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotComputedEcc() {
		final ImmutableGraph g = Transform
				.symmetrize(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView());
		final SumSweepUndirectedDiameterRadius ss = new SumSweepUndirectedDiameterRadius(g, OutputLevel.RADIUS,
				new ProgressLogger());
		ss.getEccentricity(2);
	}
}
