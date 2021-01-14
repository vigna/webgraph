/*
 *  Copyright (C) 2011-2020 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;



//RELEASE-STATUS: DIST

public class BetweennessCentralityTest {

	@Test
	public void testPath() throws InterruptedException {
		final ImmutableGraph graph = new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView();

		final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(graph);
		betweennessCentrality.compute();

		assertEquals(0, betweennessCentrality.betweenness[0], 1E-5);
		assertEquals(1, betweennessCentrality.betweenness[1], 1E-5);
		assertEquals(0, betweennessCentrality.betweenness[2], 1E-5);
	}

	@Test
	public void testLozenge() throws InterruptedException {
		final ImmutableGraph graph = new ArrayListMutableGraph(4, new int[][] { { 0, 1 }, { 0, 2 }, { 1, 3 }, { 2, 3 } }).immutableView();

		final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(graph);
		betweennessCentrality.compute();

		assertEquals(0, betweennessCentrality.betweenness[0], 1E-5);
		assertEquals(0.5, betweennessCentrality.betweenness[1], 1E-5);
		assertEquals(0.5, betweennessCentrality.betweenness[2], 1E-5);
		assertEquals(0, betweennessCentrality.betweenness[3], 1E-5);
	}

	@Test
	public void testCycle() throws InterruptedException {
		for(final int size: new int[] { 10, 50, 100 }) {
			final ImmutableGraph graph = ArrayListMutableGraph.newDirectedCycle(size).immutableView();
			final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(graph);
			betweennessCentrality.compute();

			final double[] expected = new double[size];
			Arrays.fill(expected, (size - 1) * (size - 2) / 2.0);
			for(int i = size; i-- != 0;) assertEquals(expected[i], betweennessCentrality.betweenness[i], 1E-12);
		}
	}

	@Test
	public void testClique() throws InterruptedException {
		for(final int size: new int[] { 10, 50, 100 }) {
			final ImmutableGraph graph = ArrayListMutableGraph.newCompleteGraph(size, false).immutableView();
			final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(graph);
			betweennessCentrality.compute();

			final double[] expected = new double[size];
			Arrays.fill(expected, 0);
			for(int i = size; i-- != 0;) assertEquals(expected[i], betweennessCentrality.betweenness[i], 1E-12);
		}
	}

	@Test
	public void testCliqueNobridgeCycle() throws InterruptedException {
		for(final int p: new int[] { 10, 50, 100 }) {
			for(final int k: new int[] { 10, 50, 100 }) {
				final ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++)
						if (i != j) mg.addArc(i, j);
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				final ImmutableGraph g = mg.immutableView();

				final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(g);
				betweennessCentrality.compute();

				final double[] expected = new double[k + p];

				for (int i = 0; i < k; i++) expected[i] = 0;
				for (int i = k; i < k + p; i++) expected[i] = (p - 1) * (p - 2) / 2.0;

				for (int i = 0; i < k + p; i++) assertEquals(expected[i], betweennessCentrality.betweenness[i], 1E-12);
			}
		}
	}

	@Test
	public void testCliqueForwardbridgeCycle() throws InterruptedException {
		for(final int p: new int[] { 10, 50, 100 }) {
			for(final int k: new int[] { 10, 50, 100 }) {
				final ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++)
						if (i != j) mg.addArc(i, j);
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k - 1, k);
				final ImmutableGraph g = mg.immutableView();

				final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(g);
				betweennessCentrality.compute();

				final double[] expected = new double[k + p];

				for (int i = 0; i < k - 1; i++) expected[i] = 0;
				expected[k - 1] = p * (k - 1);
				for (int d = 0; d < p; d++) expected[k + d] = k * (p - d - 1) + (p - 1) * (p - 2) / 2.0;

				for (int i = 0; i < k + p; i++) assertEquals(expected[i], betweennessCentrality.betweenness[i], 1E-12);
			}
		}
	}

	@Test
	public void testCliqueBackbridgeCycle() throws InterruptedException {
		for(final int p: new int[] { 10, 50, 100 }) {
			for(final int k: new int[] { 10, 50, 100 }) {
				final ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++)
						if (i != j) mg.addArc(i, j);
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k, k - 1);
				final ImmutableGraph g = mg.immutableView();

				final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(g);
				betweennessCentrality.compute();

				final double[] expected = new double[k + p];

				for (int i = 0; i < k - 1; i++) expected[i] = 0;
				expected[k - 1] = p * (k - 1);
				for (int d = 0; d < p; d++) expected[k + d] = k * (d - 1 + (d == 0? p : 0)) + (p - 1) * (p - 2) / 2.0;

				for (int i = 0; i < k + p; i++) assertEquals(expected[i], betweennessCentrality.betweenness[i], 1E-12);
			}
		}
	}

	@Test
	public void testCliqueBibridgeCycle() throws InterruptedException {
		for(final int p: new int[] { 10, 50, 100 }) {
			for(final int k: new int[] { 10, 50, 100 }) {
				final ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++)
						if (i != j) mg.addArc(i, j);
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k, k - 1);
				mg.addArc(k - 1, k);
				final ImmutableGraph g = mg.immutableView();

				final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(g);
				betweennessCentrality.compute();

				final double[] expected = new double[k + p];

				for (int i = 0; i < k - 1; i++) expected[i] = 0;
				expected[k - 1] = 2 * p * (k - 1);
				expected[k] = 2 * k * (p - 1) + (p - 1) * (p - 2) / 2.0;
				for (int d = 1; d < p; d++) expected[k + d] = k * (p - 2) + (p - 1) * (p - 2) / 2.0;

				for (int i = 0; i < k + p; i++) assertEquals(expected[i], betweennessCentrality.betweenness[i], 1E-12);
			}
		}
	}

	@Test
	public void testRandom() throws InterruptedException {
		for (final double p: new double[] { .1, .2, .5, .7 })
			for(final int size: new int[] { 10, 50, 100 }) {
				final ImmutableGraph graph = new ArrayListMutableGraph(new ErdosRenyiGraph(size, p, 0, false)).immutableView();
				final BetweennessCentrality betweennessCentralityMultipleVisits = new BetweennessCentrality(graph);
				betweennessCentralityMultipleVisits.compute();

				final BetweennessCentrality betweennessCentrality = new BetweennessCentrality(graph);
				betweennessCentrality.compute();

				assertArrayEquals(betweennessCentrality.betweenness, betweennessCentralityMultipleVisits.betweenness, 1E-15);
			}
	}

	@Test
	public void testOverflowOK() throws InterruptedException {
		final int blocks = 20;
		final int blockSize = 10;
		final int n = blocks * blockSize;

		final ArrayListMutableGraph arrayListMutableGraph = new ArrayListMutableGraph(n);

		for(int i = blocks; i-- != 0;)
			for(int j = blockSize - 1; j-- != 0;) {
				arrayListMutableGraph.addArc(i * blockSize, i * blockSize + j + 1);
				arrayListMutableGraph.addArc(i * blockSize + j + 1, (i + 1) * blockSize % n);
			}

		new BetweennessCentrality(arrayListMutableGraph.immutableView()).compute();
	}

	@Test(expected=BetweennessCentrality.PathCountOverflowException.class)
	public void testOverflowNotOK() throws InterruptedException {
		final int blocks = 40;
		final int blockSize = 10;
		final int n = blocks * blockSize;

		final ArrayListMutableGraph arrayListMutableGraph = new ArrayListMutableGraph(n);

		for(int i = blocks; i-- != 0;)
			for(int j = blockSize - 1; j-- != 0;) {
				arrayListMutableGraph.addArc(i * blockSize, i * blockSize + j + 1);
				arrayListMutableGraph.addArc(i * blockSize + j + 1, (i + 1) * blockSize % n);
			}

		new BetweennessCentrality(arrayListMutableGraph.immutableView()).compute();
	}
}
