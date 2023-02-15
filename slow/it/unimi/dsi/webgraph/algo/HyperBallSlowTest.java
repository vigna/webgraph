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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.WebGraphTestCase;


public class HyperBallSlowTest extends WebGraphTestCase {

	/** The true (i.e., exactly computed by {@link NeighbourhoodFunction}) neighbourhood function of <code>cnr-2000</code>. */
	public final static double[] cnr2000NF = { 325557.0, 3454267.0, 3.4531824E7, 1.5878699E8, 6.83926525E8, 1.190460703E9, 1.604430414E9, 2.35307782E9, 2.997067429E9, 3.968809803E9, 5.058079643E9,
			6.421976049E9, 8.284517654E9, 1.0243847731E10, 1.2607757915E10, 1.5228803201E10, 1.7747396141E10, 1.9909476778E10, 2.221766255E10, 2.4379845882E10, 2.6311779701E10, 2.8107451664E10,
			2.9665243165E10, 3.0951071763E10, 3.218581841E10, 3.3215135972E10, 3.4149034335E10, 3.4932882223E10, 3.5364851538E10, 3.5931189753E10, 3.6281498738E10, 3.6560429256E10, 3.6817190941E10,
			3.6998241145E10, 3.7125032189E10, 3.7214125718E10, 3.7278637339E10, 3.7317211025E10, 3.7344441435E10, 3.7363743739E10, 3.7376116159E10, 3.7386091516E10, 3.7393988067E10, 3.7401055259E10,
			3.740755634E10, 3.7413358276E10, 3.7418706947E10, 3.7423579858E10, 3.7427946736E10, 3.7431862349E10, 3.7435354797E10, 3.7438438086E10, 3.7441057447E10, 3.7443233065E10, 3.7445170896E10,
			3.7446818612E10, 3.7448244469E10, 3.7449425939E10, 3.745045924E10, 3.7451366966E10, 3.7452151719E10, 3.7452841271E10, 3.7453422635E10, 3.7453918161E10, 3.7454357668E10, 3.7454740726E10,
			3.7455030057E10, 3.745523956E10, 3.7455417775E10, 3.7455555869E10, 3.7455655899E10, 3.7455728404E10, 3.7455776324E10, 3.7455807203E10, 3.7455827683E10, 3.7455839892E10, 3.7455845502E10,
			3.7455848208E10, 3.7455850151E10, 3.745585096E10, 3.7455851388E10, 3.7455851633E10, 3.7455851773E10, 3.7455851833E10, 3.7455851843E10 };

	@Test
	public void testLarge() throws IOException {
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = ImmutableGraph.load(path);
		final int correct[] = new int[cnr2000NF.length];
		final int limit = cnr2000NF.length;
		for(final int log2m: new int[] { 4, 7 }) {
			final double rsd = HyperBall.relativeStandardDeviation(log2m);
			for(int attempt = 0; attempt < 10; attempt++) {
				final HyperBall hyperBall = new HyperBall(g, attempt % 3 == 0 ? Transform.transpose(g) : null, log2m, null, 0, 0, 0, attempt % 2 != 0, false, false, null, attempt);
				final SequentialHyperBall sequentialHyperBall = new SequentialHyperBall(g, log2m, null, attempt);
				hyperBall.init();
				sequentialHyperBall.init();
				for(int i = 1; i < limit; i++) {
					System.err.println("log2m: " + log2m + " attempt: " + attempt + " round: " + i);
					hyperBall.iterate();
					final double current = hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1);
					final double sequentialCurrent = sequentialHyperBall.iterate();
					HyperBallTest.assertState(g.numNodes(), log2m, sequentialHyperBall.registers(), hyperBall.registers());
					HyperBallTest.assertRelativeError(sequentialCurrent, current, HyperBallTest.THRESHOLD);
					if (Math.abs(cnr2000NF[i] - current)  <= cnr2000NF[i] * 2 * rsd) correct[i]++;
				}
				hyperBall.close();
				sequentialHyperBall.close();
			}
			for(int i = 1; i < limit; i++) assertTrue(correct[i] + " < " + 9, correct[i] >= 9);
		}
		deleteGraph(path);
	}

}
