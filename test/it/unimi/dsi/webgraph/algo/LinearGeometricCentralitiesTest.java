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

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;



//RELEASE-STATUS: DIST

public class LinearGeometricCentralitiesTest {

	@Test
	public void testPath() throws InterruptedException {
		final ImmutableGraph graph = Transform.transpose(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 } }).immutableView());

		final GeometricCentralities centralities = new GeometricCentralities(graph);
		centralities.compute();
		final LinearGeometricCentrality linear = new LinearGeometricCentrality(graph, new LinearGeometricCentrality.HarmonicCoefficients());
		linear.compute();

		assertEquals(0, centralities.harmonic[0], 0);
		assertEquals(1, centralities.harmonic[1], 0);
		assertEquals(3./2, centralities.harmonic[2], 0);
		assertEquals(0, linear.centrality[0], 0);
		assertEquals(1, linear.centrality[1], 0);
		assertEquals(3./2, linear.centrality[2], 0);
	}

	@Test
	public void testErdosRenyi() throws IOException, InterruptedException {
		for(final int size: new int[] { 10, 100 }) {
			for(final double density: new double[] { 0.0001, 0.001, 0.01 }) {
				final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(size, density, 0, false)).immutableView();
				final HyperBall hanf = new HyperBall(g, Transform.transpose(g), 20, null, 0, 0, 0, false, true, true, null, 0);
				hanf.init();
				do hanf.iterate(); while(hanf.modified() != 0);
				final LinearGeometricCentrality linear = new LinearGeometricCentrality(g, new LinearGeometricCentrality.HarmonicCoefficients());
				linear.compute();

				for(int i = 0; i < size; i++)
					assertEquals(hanf.sumOfInverseDistances[i], linear.centrality[i], 1E-3);
				hanf.close();
			}
		}
	}

	@Test
	public void testErdosRenyiBis() throws IOException, InterruptedException {
		for(final int size: new int[] { 10, 100 }) {
			for(final double density: new double[] { 0.0001, 0.001, 0.01 }) {
				final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(size, density, 0, false)).immutableView();
				LinearGeometricCentrality linear = new LinearGeometricCentrality(g, new LinearGeometricCentrality.PowerLawCoefficients(0));
				linear.compute();

				for(int i = 0; i < size; i++) 
					assertEquals(linear.reachable[i], linear.centrality[i], 1E-3);

				final GeometricCentralities centralities = new GeometricCentralities(g);
				centralities.compute(); 
				linear = new LinearGeometricCentrality(g, x->-x);  // Negative peripherality
				linear.compute();
				for(int i = 0; i < size; i++) {
					if (centralities.closeness[i] != 0)
						assertEquals(-1.0 / centralities.closeness[i], linear.centrality[i], 1E-3);
					else
						assertEquals(centralities.closeness[i], 0, 1E-3);
				}
				
				linear = new LinearGeometricCentrality(g, new LinearGeometricCentrality.ExponentialCoefficients(1));
				linear.compute();

				for(int i = 0; i < size; i++) 
					assertEquals(linear.reachable[i], linear.centrality[i], 1E-3);



			}
		}
	}

}
