/*
 * Copyright (C) 2015-2023 Sebastiano Vigna
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

import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.algo.TopKGeometricCentrality.Centrality;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

public class TopKGeometricCentralityTest {

	// The list of centralities to be tested.
	private final Centrality c[] = { Centrality.LIN, Centrality.HARMONIC, Centrality.EXPONENTIAL };

	@Test
	public void testPath() {
		final ImmutableGraph graph = new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 1 }, { 1, 0 } }).immutableView();

		final TopKGeometricCentrality cc1 = TopKGeometricCentrality.newLinCentrality(graph, 1, 0);
		cc1.compute();
		assertEquals(1, cc1.topK.length);
		assertEquals(3.0 * 3.0 / 2, cc1.centrality[1], 1E-13);
		assertEquals(cc1.topK[0], 1);

		final TopKGeometricCentrality cc2 = TopKGeometricCentrality.newLinCentrality(graph, 3, 0);

		cc2.compute();
		assertEquals(3, cc2.topK.length);
		int v = cc2.topK[0];
		assertTrue(v == 1);
		v = cc2.topK[1];
		final int w = cc2.topK[2];
		assertTrue(v == 0 || v == 2);
		assertTrue(w == 0 || w == 2);
		assertTrue(v != w);
		assertEquals(3.0 * 3.0 / 2, cc2.centrality[1], 1E-13);
		assertEquals(3.0 * 3.0 / 3, cc2.centrality[0], 1E-13);
		assertEquals(3.0 * 3.0 / 3, cc2.centrality[2], 1E-13);
	}

	@Test
	public void testLozenge() {
		final ImmutableGraph graph = new ArrayListMutableGraph(4, new int[][] { { 0, 1 }, { 0, 2 }, { 1, 3 }, { 2, 3 } }).immutableView();
		final TopKGeometricCentrality cc = TopKGeometricCentrality.newHarmonicCentrality(graph, 3, 0);

		cc.compute();
		assertEquals(3, cc.topK.length);
		int v = cc.topK[0];
		assertTrue(v == 0);
		v = cc.topK[1];
		final int w = cc.topK[2];
		assertTrue(v == 1 || v == 2);
		assertTrue(w == 1 || w == 2);
		assertTrue(v != w);
		assertEquals(2.5, cc.centrality[0], 1E-13);
		assertEquals(1.0, cc.centrality[1], 1E-13);
		assertEquals(1.0, cc.centrality[2], 1E-13);
	}

	@Test
	public void testLozengeModified() {
		final ImmutableGraph graph = new ArrayListMutableGraph(5, new int[][] { { 0, 1 }, { 0, 2 }, { 1, 3 }, { 2, 3 }, { 0, 4 }, { 4, 0 } }).immutableView();
		final TopKGeometricCentrality cc = TopKGeometricCentrality.newExponentialCentrality(graph, 2, 0.5, 0);

		cc.compute();
		assertEquals(2, cc.topK.length);
		int v = cc.topK[1];
		assertEquals(v, 4);
		v = cc.topK[0];
		assertEquals(v, 0);
		assertEquals((3 * 0.5 + 0.5 * 0.5), cc.centrality[0], 1E-13);
		assertEquals((0.5 + 2 * 0.5 * 0.5 + 0.5 * 0.5 * 0.5), cc.centrality[4], 1E-13);
	}

	@Test
	public void testCycle() {
		// In this test, we also check the behavior if k is bigger than the
		// number of nodes.
		for (final int size : new int[] { 3, 5, 7 }) {
			final int k = 5;
			final ImmutableGraph graph = ArrayListMutableGraph.newDirectedCycle(size).immutableView();
			final TopKGeometricCentrality cc = TopKGeometricCentrality.newExponentialCentrality(graph, k, 0.5, 0);
			cc.compute();

			final double expected = (1 - Math.pow(0.5, size - 1));
			int nFound = 0;
			for (int i = 0; i < size; i++) {
				if (Math.abs(expected - cc.centrality[i]) < 1E-12) {
					nFound++;
				}
			}
			assertTrue(nFound >= Math.min(size, k));
		}
	}

	@Test
	public void testClique() {
		// In this test, we also check the behavior if k is bigger than the
		// number of nodes.
		for (final int size : new int[] { 10, 50, 100 }) {
			final int k = 30;
			final ImmutableGraph graph = ArrayListMutableGraph.newCompleteGraph(size, false).immutableView();
			final TopKGeometricCentrality cc = TopKGeometricCentrality.newLinCentrality(graph, k, 0);
			cc.compute();
			final double expected = size * size / (size-1.0);
			int nFound = 0;
			for (int i = 0; i < size; i++) {
				if (Math.abs(expected - cc.centrality[i]) < 1E-12) {
					nFound++;
				}
			}
			assertTrue(nFound >= Math.min(size, k));
		}
	}

	@Test
	public void testSparse() {
		// Used to test the behavior if the centrality of only few vertices
		// is defined (in the extreme case, if the graph is empty).
		final ImmutableGraph emptygraph = new ArrayListMutableGraph(100, new int[][] {}).immutableView();

		for (final Centrality curC : c) {
			final TopKGeometricCentrality cc = new TopKGeometricCentrality(emptygraph, 1, curC, 0, 0.5, new ProgressLogger());
			cc.compute();
			assertEquals(cc.topK.length, 1);
		}
		final ImmutableGraph graphfewedges = new ArrayListMutableGraph(100, new int[][] { { 10, 32 }, { 21, 44 } }).immutableView();
		for (final Centrality curC : c) {
			final TopKGeometricCentrality cc = new TopKGeometricCentrality(graphfewedges, 30, curC, 0, 0.5, new ProgressLogger());
			cc.compute();
			assertEquals(cc.topK.length, 30);
			final int v = cc.topK[0], w = cc.topK[1];
			assertTrue(cc.topK[0] == 10 || cc.topK[0] == 21);
			assertTrue(w == 10 || w == 21);
			assertTrue(v != w);
		}
	}

	@Test
	public void testRandom() throws InterruptedException {
		for (final double p : new double[] { .1, .2, .5 }) {
			for (final int size : new int[] { 10, 50, 100, 500 }) {
				for (final int k : new int[] { 5, 10, 30, 100 }) {
					final ImmutableGraph graph = new ArrayListMutableGraph(new ErdosRenyiGraph(size, p, 0, false)).immutableView();
					final GeometricCentralities cc_exaustive = new GeometricCentralities(graph);
					cc_exaustive.compute();
					for (final Centrality cCur : c) {
						final TopKGeometricCentrality cc = new TopKGeometricCentrality(graph, k, cCur, 0, 0.5, new ProgressLogger());
						cc.compute();
						cc.checkReachLU();
						double[] centrExaustive;
						switch (cCur) {
						case LIN:
							centrExaustive = Arrays.copyOf(cc_exaustive.lin, cc_exaustive.lin.length);
							break;
						case HARMONIC:
							centrExaustive = Arrays.copyOf(cc_exaustive.harmonic, cc_exaustive.harmonic.length);
							break;
						case EXPONENTIAL:
							centrExaustive = Arrays.copyOf(cc_exaustive.exponential, cc_exaustive.exponential.length);
							break;
						default:
							centrExaustive = new double[size];
							break;
						}

						for (int v = 0; v < graph.numNodes(); v++) {
							if (cc.centrality[v] != -1) {
								assertEquals(cc.centrality[v], centrExaustive[v], 1E-12);
							}
						}
						Arrays.sort(centrExaustive);
						DoubleArrays.reverse(centrExaustive);

						for (int i = 0; i < cc.topK.length; i++) {
							assertEquals(cc.centrality[cc.topK[i]], centrExaustive[i], 1E-12);
						}
						assertEquals(cc.topK.length, Math.min(k, size));
					}
				}
			}
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidK() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		@SuppressWarnings("unused")
		final TopKGeometricCentrality cc = TopKGeometricCentrality.newLinCentrality(g, -1, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidNThreads() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		@SuppressWarnings("unused")
		final TopKGeometricCentrality cc = TopKGeometricCentrality.newHarmonicCentrality(g, 1, -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidAlpha() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final TopKGeometricCentrality cc = TopKGeometricCentrality.newExponentialCentrality(g, 1, 1, 0);
		cc.compute();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidAlpha1() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final TopKGeometricCentrality cc = TopKGeometricCentrality.newExponentialCentrality(g, 1, 0, 0);
		cc.compute();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidAlpha2() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final TopKGeometricCentrality cc = TopKGeometricCentrality.newExponentialCentrality(g, 1, -1.0, 0);
		cc.compute();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidAlpha3() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final TopKGeometricCentrality cc = TopKGeometricCentrality.newExponentialCentrality(g, 1, 5, 0);
		cc.compute();
	}

	public void testNoPL() {
		final ImmutableGraph g = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final TopKGeometricCentrality cc = new TopKGeometricCentrality(g, 10, Centrality.EXPONENTIAL, 1, 5, null);
		cc.compute();
	}
}
