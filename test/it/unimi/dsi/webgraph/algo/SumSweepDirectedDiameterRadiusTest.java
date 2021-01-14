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
import it.unimi.dsi.webgraph.algo.SumSweepDirectedDiameterRadius.OutputLevel;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

public class SumSweepDirectedDiameterRadiusTest {

	@Test
	public void testPath() {
		final ImmutableGraph graph = new ArrayListMutableGraph(3,
				new int[][] { { 0, 1 }, { 1, 2 }, { 2, 1 }, { 1, 0 } }).immutableView();

		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph, OutputLevel.ALL, null,
				new ProgressLogger());
		ss.compute();

		assertEquals(ss.getEccentricity(0, true), 2);
		assertEquals(ss.getEccentricity(1, true), 1);
		assertEquals(ss.getEccentricity(2, true), 2);
		assertEquals(ss.getEccentricity(0, false), 2);
		assertEquals(ss.getEccentricity(0, false), 2);
		assertEquals(ss.getEccentricity(0, false), 2);
		assertEquals(ss.getDiameter(), 2);
		assertEquals(ss.getRadius(), 1);
		assertEquals(ss.getRadialVertex(), 1);
		assertTrue(ss.getDiametralVertex() == 2 || ss.getDiametralVertex() == 0);
	}

	@Test
	public void testManySCC() {
		final ImmutableGraph graph = new ArrayListMutableGraph(7,
				new int[][] { { 0, 1 }, { 1, 0 }, { 1, 2 }, { 2, 1 }, { 6, 2 }, { 2, 6 }, { 3, 4 }, { 4, 3 }, { 4, 5 },
						{ 5, 4 }, { 0, 3 }, { 0, 4 }, { 1, 5 }, { 1, 4 }, { 2, 5 } }).immutableView();
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph, OutputLevel.RADIUS, null,
				new ProgressLogger());

		ss.compute();
		assertEquals(ss.getRadius(), 2);
		assertEquals(ss.getRadialVertex(), 1);
	}

	@Test
	public void testLozenge() {
		final ImmutableGraph graph = new ArrayListMutableGraph(4,
				new int[][] { { 0, 1 }, { 1, 0 }, { 0, 2 }, { 1, 3 }, { 2, 3 } }).immutableView();
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph, OutputLevel.RADIUS, null,
				new ProgressLogger());

		ss.compute();
		assertEquals(ss.getRadius(), 2);
		assertTrue(ss.getRadialVertex() == 0 || ss.getRadialVertex() == 1);
		assertTrue(ss.getEccentricity(ss.getRadialVertex(), true) == ss.getRadius());
	}

	@Test
	public void testManyDirPath() {
		final ImmutableGraph graph = new ArrayListMutableGraph(19,
				new int[][] { { 0, 1 }, { 1, 2 }, { 2, 3 }, { 3, 4 }, { 5, 6 }, { 6, 7 }, { 7, 8 }, { 8, 9 }, { 9, 10 },
						{ 10, 18 }, { 11, 12 }, { 13, 14 }, { 14, 15 }, { 15, 16 }, { 16, 17 } }).immutableView();

		final boolean accRadial[] = new boolean[19];
		accRadial[16] = true;
		accRadial[8] = true;
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph, OutputLevel.ALL, accRadial,
				new ProgressLogger());
		ss.compute();
		assertEquals(ss.getDiameter(), 6);
		assertEquals(ss.getRadius(), 1);

		assertTrue(ss.getRadialVertex() == 16);
		assertTrue(ss.getDiametralVertex() == 5 || ss.getDiametralVertex() == 18);

	}

	@Test
	public void testCycle() {
		for (final int size : new int[] { 3, 5, 7 }) {
			final ImmutableGraph graph = ArrayListMutableGraph.newDirectedCycle(size).immutableView();
			final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph,
					OutputLevel.RADIUS_DIAMETER, null, new ProgressLogger());
			ss.compute();

			assertEquals(ss.getDiameter(), size - 1);
			assertEquals(ss.getRadius(), size - 1);

			assertTrue(ss.getEccentricity(ss.getRadialVertex(), true) == ss.getRadius());
			assertTrue(ss.getEccentricity(ss.getDiametralVertex(), true) == ss.getDiameter());
		}
	}

	@Test
	public void testClique() {
		for (final int size : new int[] { 10, 50, 100 }) {
			final ImmutableGraph graph = ArrayListMutableGraph.newCompleteGraph(size, false).immutableView();
			final boolean accRadius[] = new boolean[graph.numNodes()];
			accRadius[(int) (Math.random() * size)] = true;
			accRadius[(int) (Math.random() * size)] = true;
			accRadius[(int) (Math.random() * size)] = true;

			final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph, OutputLevel.ALL,
					accRadius, new ProgressLogger());
			ss.compute();

			for (int i = 0; i < size; i++) {
				assertEquals(ss.getEccentricity(i, true), 1);
			}
			assertTrue(accRadius[ss.getRadialVertex()]);
		}
	}

	@Test
	public void testSparse() {
		// Used to test the behavior if the centrality of only few vertices
		// is defined (in the extreme case, if the graph is empty).
		final ImmutableGraph emptygraph = new ArrayListMutableGraph(100, new int[][] {}).immutableView();

		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(emptygraph, OutputLevel.ALL, null,
				new ProgressLogger());
		ss.compute();
		assertEquals(ss.getRadius(), 0);
		assertEquals(ss.getDiameter(), 0);

		final ImmutableGraph graphfewedges = new ArrayListMutableGraph(100,
				new int[][] { { 10, 32 }, { 10, 65 }, { 65, 10 }, { 21, 44 } }).immutableView();
		final SumSweepDirectedDiameterRadius ss1 = new SumSweepDirectedDiameterRadius(graphfewedges, OutputLevel.RADIUS,
				null, null);
		ss1.compute();
		assertEquals(ss1.getRadius(), 1);
		assertEquals(ss1.getRadialVertex(), 10);
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
				final boolean[] accRadial = new boolean[size];
				for (int i = 0; i < size / 2; i++) {
					accRadial[(int) (Math.random() * size)] = true;
				}
				final ImmutableGraph graph = new ArrayListMutableGraph(new ErdosRenyiGraph(size, p, 0, false))
						.immutableView();
				final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph, OutputLevel.ALL,
						accRadial, new ProgressLogger());
				ss.sumSweepHeuristic((int) (Math.random() * size), Math.min(10, (int) (2 + Math.random() * size)));

				final int[] ecc = computeAllEccentricities(graph);
				final int[] eccRev = computeAllEccentricities(Transform.transpose(graph));

				for (int v = 0; v < size; v++) {
					assertTrue(ss.lF[v] <= ecc[v]);
					assertTrue(ss.lB[v] <= eccRev[v]);
					assertTrue(ss.uF[v] >= ecc[v]);
					assertTrue(ss.uB[v] >= eccRev[v]);
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
			final boolean[] accRadial = new boolean[size];
			for (int i = (int) (Math.random() * size + 1); i >= 0; i--) {
				accRadial[(int) (Math.random() * size)] = true;
			}
			final ImmutableGraph graph = new ArrayListMutableGraph(new ErdosRenyiGraph(size, p, 0, false))
					.immutableView();

			OutputLevel output;
			switch ((int) (Math.random() * 5)) {
			case 0:
				output = OutputLevel.RADIUS;
				break;
			case 1:
				output = OutputLevel.DIAMETER;
				break;
			case 2:
				output = OutputLevel.RADIUS_DIAMETER;
				break;
			case 3:
				output = OutputLevel.ALL_FORWARD;
				break;
			default:
				output = OutputLevel.ALL;
				break;
			}

			final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(graph, output, accRadial,
					null);
			ss.compute();
			final int[] ecc = computeAllEccentricities(graph);
			int D = 0, R = size;
			for (int v = 0; v < size; v++) {
				D = Math.max(D, ecc[v]);
				if (accRadial[v]) {
					R = Math.min(R, ecc[v]);
				}
			}
			if (output == OutputLevel.RADIUS || output == OutputLevel.RADIUS_DIAMETER || output == OutputLevel.ALL_FORWARD
					|| output == OutputLevel.ALL) {
				assertEquals(ss.getRadius(), R);
				assertEquals(ss.getEccentricity(ss.getRadialVertex(), true), R);
			}
			if (output == OutputLevel.DIAMETER || output == OutputLevel.RADIUS_DIAMETER || output == OutputLevel.ALL_FORWARD
					|| output == OutputLevel.ALL) {
				assertEquals(ss.getDiameter(), D);
				int maxEcc = -1;
				try {
					maxEcc = ss.getEccentricity(ss.getDiametralVertex(), true);
				} catch (final UnsupportedOperationException e) {
				}
				try {
					maxEcc = Math.max(maxEcc, ss.getEccentricity(ss.getDiametralVertex(), false));
				} catch (final UnsupportedOperationException e) {
				}
				assertEquals(maxEcc, D);
			}

			if (output == OutputLevel.ALL_FORWARD) {
				for (int v = 0; v < size; v++) {
					assertEquals(ss.getEccentricity(v, true), ecc[v]);
				}
			}
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidAccRadial() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final boolean accRadial[] = new boolean[4];
		@SuppressWarnings("unused")
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(g, OutputLevel.RADIUS, accRadial,
				new ProgressLogger());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidAccRadial1() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final boolean accRadial[] = new boolean[1];
		@SuppressWarnings("unused")
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(g, OutputLevel.RADIUS_DIAMETER,
				accRadial, new ProgressLogger());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidAccRadial2() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final boolean accRadial[] = new boolean[3];
		@SuppressWarnings("unused")
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(g, OutputLevel.ALL, accRadial,
				new ProgressLogger());
	}

	@Test
	public void testEmptyAccRadial() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final boolean accRadial[] = new boolean[2];
		// @SuppressWarnings("unused")
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(g, OutputLevel.ALL, accRadial,
				new ProgressLogger());
		ss.compute();
		assertEquals(ss.getRadius(), Integer.MAX_VALUE);
	}

	@Test
	public void testEmptyGraph() {
		final ImmutableGraph g = new ArrayListMutableGraph(0, new int[][] {}).immutableView();
		// @SuppressWarnings("unused")
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(g, OutputLevel.ALL, null,
				new ProgressLogger());
		ss.compute();
		assertEquals(ss.getRadius(), Integer.MAX_VALUE);
		assertEquals(ss.getDiameter(), 0);

		final ImmutableGraph h = new ArrayListMutableGraph(2, new int[][] {}).immutableView();
		// @SuppressWarnings("unused")
		final SumSweepDirectedDiameterRadius ssh = new SumSweepDirectedDiameterRadius(h, OutputLevel.ALL, null,
				new ProgressLogger());
		ssh.compute();
		assertEquals(ssh.getRadius(), 0);
		assertEquals(ssh.getDiameter(), 0);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotComputedR() {
		final ImmutableGraph g = new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView();
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(g, OutputLevel.RADIUS, null,
				new ProgressLogger());
		ss.sumSweepHeuristic(0, 1);
		ss.getRadius();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotComputedD() {
		final ImmutableGraph g = new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView();
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(g, OutputLevel.RADIUS, null,
				new ProgressLogger());
		ss.sumSweepHeuristic(0, 1);
		ss.getDiameter();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotComputedEcc() {
		final ImmutableGraph g = new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView();
		final SumSweepDirectedDiameterRadius ss = new SumSweepDirectedDiameterRadius(g, OutputLevel.RADIUS, null,
				new ProgressLogger());
		ss.getEccentricity(2, true);
	}
}
