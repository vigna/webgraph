/*
 * Copyright (C) 2011-2023 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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
import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;



//RELEASE-STATUS: DIST

public class GeometricCentralitiesTest {

	@Test
	public void testPath() throws InterruptedException {
		final ImmutableGraph graph = Transform.transpose(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView());

		final GeometricCentralities centralities = new GeometricCentralities(graph);
		centralities.compute();

		assertEquals(0, centralities.closeness[0], 0);
		assertEquals(1, centralities.closeness[1], 0);
		assertEquals(1./3, centralities.closeness[2], 0);

		assertEquals(1, centralities.lin[0], 0);
		assertEquals(4, centralities.lin[1], 0);
		assertEquals(3, centralities.lin[2], 0);

		assertEquals(0, centralities.harmonic[0], 0);
		assertEquals(1, centralities.harmonic[1], 0);
		assertEquals(3./2, centralities.harmonic[2], 0);
	}

	@Test
	public void testCycle() throws InterruptedException {
		for(final int size: new int[] { 10, 50, 100 }) {
			final ImmutableGraph graph = ArrayListMutableGraph.newDirectedCycle(size).immutableView();
			final GeometricCentralities centralities = new GeometricCentralities(graph);
			centralities.compute();

			final double[] expected = new double[size];
			Arrays.fill(expected, 2. / (size * (size - 1.)));
			for(int i = size; i-- != 0;) assertEquals(expected[i], centralities.closeness[i], 1E-15);
			Arrays.fill(expected, size * 2. / (size - 1.));
			for(int i = size; i-- != 0;) assertEquals(expected[i], centralities.lin[i], 1E-15);
			double s = 0;
			for(int i = size; i-- != 1;) s += 1. / i;
			Arrays.fill(expected, s);
			for(int i = size; i-- != 0;) assertEquals(expected[i], centralities.harmonic[i], 1E-14);
		}
	}

	@Test
	public void testErdosRenyi() throws IOException, InterruptedException {
		for(final int size: new int[] { 10, 100 }) {
			for(final double density: new double[] { 0.0001, 0.001, 0.01 }) {
				final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(size, density, 0, false)).immutableView();
				final HyperBall hanf = new HyperBall(g, Transform.transpose(g), 20, null, 0, 0, 0, false, true, true, null, 0);
				hanf.init();
				do hanf.iterate(); while(hanf.modified() != 0);
				final GeometricCentralities centralities = new GeometricCentralities(g);
				centralities.compute();

				for(int i = 0; i < size; i++)
					assertEquals(hanf.sumOfInverseDistances[i], centralities.harmonic[i], 1E-3);
				for(int i = 0; i < size; i++)
					assertEquals(hanf.sumOfDistances[i] == 0 ? 0 : 1 / hanf.sumOfDistances[i], centralities.closeness[i], 1E-5);
				for(int i = 0; i < size; i++)
					assertEquals(hanf.sumOfDistances[i] == 0 ? 1 : hanf.count(i) * hanf.count(i) / hanf.sumOfDistances[i], centralities.lin[i], 1E-3);
				hanf.close();
			}
		}
	}
}
