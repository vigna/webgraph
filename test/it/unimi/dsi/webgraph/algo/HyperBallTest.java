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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.Int2DoubleFunction;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.util.HyperLogLogCounterArray;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.WebGraphTestCase;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;


public class HyperBallTest extends WebGraphTestCase {
	// Below this threshold errors due to block-by-block summing start to appear.
	protected static final double THRESHOLD = 1E-9;

	/** Checks that the state of two HyperBall implementation (as
	 * returned by {@link HyperLogLogCounterArray#registers()}) are exactly the same. */
	public final static void assertState(final int size, final int log2m, final LongBigList[] a, final LongBigList[] b) {
		final int m = 1 << log2m;
		for(int i = 0; i < size; i++) {
			for(int j = 0; j < m; j++) {
				final long index = ((long)i << log2m) + j;
				final int chunk = (int)(index >>> HyperLogLogCounterArray.CHUNK_SHIFT);
				final long offset = index & HyperLogLogCounterArray.CHUNK_MASK;
				assertEquals("Counter " + i + ", register " + j + ": ", a[chunk].getLong(offset), b[chunk].getLong(offset));
			}
		}
	}

	@Test
	public void testTrivial() throws IOException {
		final ImmutableGraph g = ArrayListMutableGraph.newCompleteBinaryIntree(10).immutableView();
		HyperBall hyperBall = new HyperBall(g, g, 7, null, 0, 0, 0, false, false, false, null, 0);
		hyperBall.run(Long.MAX_VALUE, -1);
		hyperBall.run(Long.MAX_VALUE, -1);
		hyperBall.close();

		hyperBall = new HyperBall(g, g, 7, null, 0, 0, 0, true, false, false, null, 0);
		hyperBall.run(Long.MAX_VALUE, -1);
		hyperBall.run(Long.MAX_VALUE, -1);
		hyperBall.close();

	}

	protected static void assertRelativeError(final double sequentialCurrent, final double current, final double threshold) {
		assertTrue(sequentialCurrent + " != " + current + ", " + Math.abs(current - sequentialCurrent) / current + " > " + threshold, Math.abs(current - sequentialCurrent) / current <= THRESHOLD);
	}

	/* All tests in this class check that 2 times the theoretical relative standard deviation
	 * is attained in 9 trials out of 10. The theory (in particular, the Vysochanskii-Petunin inequality)
	 * indeed says it should happen 90% of the times. */

	@Test
	public void testClique() throws IOException {
		for(final int log2m: new int[] { 4, 5, 6, 8 }) {
			final double rsd = HyperBall.relativeStandardDeviation(log2m);
			for(final int size: new int[] { 10, 100, 500 }) {
				int correct = 0;
				for(int attempt = 0; attempt < 10; attempt++) {
					System.err.println("log2m: " + log2m + " size: " + size + " attempt: " + attempt);
					final ImmutableGraph g = ArrayListMutableGraph.newCompleteGraph(size, false).immutableView();
					final HyperBall hyperBall = new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10, 10, attempt % 2 == 0, false, false, null, attempt);
					final SequentialHyperBall sequentialHyperBall = new SequentialHyperBall(g, log2m, null, attempt);
					hyperBall.init();
					sequentialHyperBall.init();
					hyperBall.iterate();
					final double current = hyperBall.neighbourhoodFunction.getDouble(1);
					final double sequentialCurrent = sequentialHyperBall.iterate();

					assertState(size, log2m, sequentialHyperBall.registers(), hyperBall.registers());

					if (Math.abs(size * size - current) <= 2 * rsd * size * size) correct++;

					assertRelativeError(sequentialCurrent, current, THRESHOLD);

					hyperBall.close();
					sequentialHyperBall.close();
				}
				assertTrue(size + ":" + rsd + " " + correct + " < " + 9, correct >= 9);
			}
		}
	}

	@Test
	public void testErdosRenyi() throws IOException {
		for(final int log2m: new int[] { 4, 5, 6, 8 }) {
			for(final int size: new int[] { 10, 100, 500 }) {
				for(int attempt = 0; attempt < 10; attempt++) {
					System.err.println("log2m: " + log2m + " size: " + size + " attempt: " + attempt);
					final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(size, .1, attempt, false)).immutableView();
					final HyperBall hyperBall = new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10 * (attempt % 3), 10, attempt % 2 == 0, false, false, null, attempt);
					final SequentialHyperBall sequentialHyperBall = new SequentialHyperBall(g, log2m, null, attempt);
					hyperBall.init();
					sequentialHyperBall.init();
					do {
						hyperBall.iterate();
						final double current = hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1);
						final double sequentialCurrent = sequentialHyperBall.iterate();
						assertState(size, log2m, sequentialHyperBall.registers(), hyperBall.registers());
						assertRelativeError(sequentialCurrent, current, THRESHOLD);
					} while(hyperBall.modified() != 0);

					hyperBall.init();
					sequentialHyperBall.init();
					do {
						hyperBall.iterate();
						final double current = hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1);
						final double sequentialCurrent = sequentialHyperBall.iterate();
						assertState(size, log2m, sequentialHyperBall.registers(), hyperBall.registers());
						assertRelativeError(sequentialCurrent, current, THRESHOLD);
					} while(hyperBall.modified() != 0);

					hyperBall.close();
					sequentialHyperBall.close();
				}
			}
		}
	}

	@Test
	public void testCycle() throws IOException {
		for(final int log2m: new int[] { 4, 5, 6 }) {
			final double rsd = HyperBall.relativeStandardDeviation(log2m);
			for(final int size: new int[] { 100, 500, 1000 }) {
				final int[] correct = new int[size + 1];
				for(int attempt = 0; attempt < 10; attempt++) {
					System.err.println("log2m: " + log2m + " size: " + size + " attempt: " + attempt);
					final ImmutableGraph g = ArrayListMutableGraph.newDirectedCycle(size).immutableView();
					final HyperBall hyperBall = new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10 * (attempt % 3), 10, attempt % 2 == 0, false, false, null, attempt);
					final SequentialHyperBall sequentialHyperBall = new SequentialHyperBall(g, log2m, null, attempt);
					hyperBall.init();
					sequentialHyperBall.init();
					for(int i = 2; i <= size; i++) {
						hyperBall.iterate();
						final double current = hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1);
						final double sequentialCurrent = sequentialHyperBall.iterate();
						assertState(size, log2m, sequentialHyperBall.registers(), hyperBall.registers());
						assertRelativeError(sequentialCurrent, current, THRESHOLD);
						if (Math.abs(size * i - current) <= 2 * rsd * size * i) correct[i]++;
					}
					hyperBall.close();
					sequentialHyperBall.close();
				}
				for(int i = 2; i <= size; i++) assertTrue(size + ":" + rsd + " " + correct[i] + " < " + 9, correct[i] >= 9);
			}
		}

	}

	@Test
	public void testLine() throws IOException {
		for(final int log2m: new int[] { 4, 5, 6 }) {
			final double rsd = HyperBall.relativeStandardDeviation(log2m);
			for(final int size: new int[] { 100, 500, 1000 }) {
				final int[] correct = new int[size + 1];
				for(int attempt = 0; attempt < 10; attempt++) {
					System.err.println("log2m: " + log2m + " size: " + size + " attempt: " + attempt);
					final ArrayListMutableGraph directedCycle = ArrayListMutableGraph.newDirectedCycle(size);
					directedCycle.removeArc(0, 1);
					final ImmutableGraph g = directedCycle.immutableView();
					final HyperBall hyperBall = new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10 * (attempt % 3), 10, attempt % 2 == 0, false, false, null, attempt);
					final SequentialHyperBall sequentialHyperBall = new SequentialHyperBall(g, log2m, null, attempt);
					hyperBall.init();
					sequentialHyperBall.init();
					for(int i = 2; i <= size; i++) {
						hyperBall.iterate();
						final double current = hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1);
						final double sequentialCurrent = sequentialHyperBall.iterate();
						assertState(size, log2m, sequentialHyperBall.registers(), hyperBall.registers());
						assertRelativeError(sequentialCurrent, current, THRESHOLD);
						long result = 0;
						for(int j = 0; j < i; j++) result += (size - j);
						if (Math.abs(result - current) <= 2 * rsd * size * i) correct[i]++;
					}
					hyperBall.close();
					sequentialHyperBall.close();
				}
				for(int i = 2; i <= size; i++) assertTrue(size + ":" + rsd + " " + correct[i] + " < " + 9, correct[i] >= 9);
			}
		}

	}

	@Test
	public void testOutdirectedStar() throws IOException {
		for(final int log2m: new int[] { 4, 5, 6 }) {
			final double rsd = HyperBall.relativeStandardDeviation(log2m);
			for(final int size: new int[] { 100, 500, 1000 }) {
				int correct = 0;
				for(int attempt = 0; attempt < 10; attempt++) {
					System.err.println("log2m: " + log2m + " size: " + size + " attempt: " + attempt);
					final ArrayListMutableGraph mg = new ArrayListMutableGraph(size);
					for(int i = 1; i < size; i++) mg.addArc(0, i);
					final ImmutableGraph g = mg.immutableView();
					final HyperBall hyperBall = new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10 * (attempt % 3), 10, attempt % 2 == 0, false, false, null, attempt);
					final SequentialHyperBall sequentialHyperBall = new SequentialHyperBall(g, log2m, null, attempt);
					hyperBall.init();
					sequentialHyperBall.init();
					hyperBall.iterate();
					final double current = hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1);
					final double sequentialCurrent = sequentialHyperBall.iterate();
					assertState(size, log2m, sequentialHyperBall.registers(), hyperBall.registers());
					assertRelativeError(sequentialCurrent, current, THRESHOLD);
					if (Math.abs(size * 2 - 1 - current) <= 2 * rsd * (size * 2 - 1)) correct++;
					hyperBall.close();
					sequentialHyperBall.close();
				}
				assertTrue(size + ":" + rsd + " " + correct + " < " + 9, correct >= 9);
			}
		}
	}

	@Test
	public void testTree() throws IOException {
		for(final int log2m: new int[] { 4, 5, 6, 7, 8, 10, 12 }) {
			final double rsd = HyperBall.relativeStandardDeviation(log2m);
			final ImmutableGraph g = ArrayListMutableGraph.newCompleteBinaryIntree(3).immutableView();
			final int[] correct = new int[3];
			for(int attempt = 0; attempt < 10; attempt++) {
				System.err.println("log2m: " + log2m + " attempt: " + attempt);
				final HyperBall hyperBall = new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10 * (attempt % 3), 10, attempt % 2 == 0, false, false, null, attempt);
				final SequentialHyperBall sequentialHyperBall = new SequentialHyperBall(g, log2m, null, attempt);
				hyperBall.init();
				sequentialHyperBall.init();

				hyperBall.iterate();
				if (Math.abs(hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1) - 29) <= 2 * rsd * 29) correct[0]++;
				sequentialHyperBall.iterate();
				assertState(g.numNodes(), log2m, sequentialHyperBall.registers(), hyperBall.registers());

				hyperBall.iterate();
				if (Math.abs(hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1) - 41) <= 2 * rsd * 41) correct[1]++;
				sequentialHyperBall.iterate();
				assertState(g.numNodes(), log2m, sequentialHyperBall.registers(), hyperBall.registers());

				hyperBall.iterate();
				if (Math.abs(hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1) - 49) <= 2 * rsd * 49) correct[2]++;
				sequentialHyperBall.iterate();
				assertState(g.numNodes(), log2m, sequentialHyperBall.registers(), hyperBall.registers());

				// Test that you can reuse the object

				hyperBall.init();
				sequentialHyperBall.init();

				hyperBall.iterate();
				if (Math.abs(hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1) - 29) <= 2 * rsd * 29) correct[0]++;
				sequentialHyperBall.iterate();
				assertState(g.numNodes(), log2m, sequentialHyperBall.registers(), hyperBall.registers());

				hyperBall.iterate();
				if (Math.abs(hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1) - 41) <= 2 * rsd * 41) correct[1]++;
				sequentialHyperBall.iterate();
				assertState(g.numNodes(), log2m, sequentialHyperBall.registers(), hyperBall.registers());

				hyperBall.iterate();
				if (Math.abs(hyperBall.neighbourhoodFunction.getDouble(hyperBall.neighbourhoodFunction.size() - 1) - 49) <= 2 * rsd * 49) correct[2]++;
				sequentialHyperBall.iterate();
				assertState(g.numNodes(), log2m, sequentialHyperBall.registers(), hyperBall.registers());

				hyperBall.close();
				sequentialHyperBall.close();
			}
			//System.err.println(Arrays.toString(correct));
			for(int i = 0; i < 3; i++) assertTrue(rsd + " " + correct[i] + " < " + 9, correct[i] >= 9);
		}
	}

	@Test(expected=IllegalStateException.class)
	public void testInitClosed() throws IOException {
		final ImmutableGraph g = ArrayListMutableGraph.newCompleteBinaryIntree(3).immutableView();
		final HyperBall hyperBall = new HyperBall(g, 8);
		hyperBall.close();
		hyperBall.init();
	}

	@Test(expected=IllegalStateException.class)
	public void testInitIterate() throws IOException {
		final ImmutableGraph g = ArrayListMutableGraph.newCompleteBinaryIntree(3).immutableView();
		final HyperBall hyperBall = new HyperBall(g, 8);
		hyperBall.close();
		hyperBall.iterate();
	}

	private int[] distancesFrom(final ImmutableGraph graph, final int from) {
		final IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
		final int n = graph.numNodes();
		final int[] dist = new int[n];
		Arrays.fill(dist, Integer.MAX_VALUE); // Initially, all distances are infinity.

		queue.enqueue(from);
		dist[from] = 0;

		LazyIntIterator successors;

		while(! queue.isEmpty()) {
			final int curr = queue.dequeueInt();
			successors = graph.successors(curr);
			int d = graph.outdegree(curr);
			while(d-- != 0) {
				final int succ = successors.nextInt();
				if (dist[succ] == Integer.MAX_VALUE) {
					dist[succ] = dist[curr] + 1;
					queue.enqueue(succ);
				}
			}
		}

		return dist;
	}

	@Test
	public void testErdosRenyiEccentricity() throws IOException {
		final XoRoShiRo128PlusRandom rand = new XoRoShiRo128PlusRandom(1);
		for(final int log2m: new int[] { 15 }) {
			for(final int size: new int[] { 10, 100, 500 }) {
				for(int attempt = 0; attempt < 5; attempt++) {
					System.err.println("log2m: " + log2m + " size: " + size + " attempt: " + attempt);
					final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(size, .1, attempt + 1, false)).immutableView();
					final HyperBall hyperBall =
						new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10 * (attempt % 3), 10, attempt % 2 == 0, true, false, null, attempt);
					hyperBall.init();
					do {
						hyperBall.iterate();
					} while(hyperBall.modified() != 0);

					final int n = g.numNodes();
					for (int i = 0; i < 10; i++) {
						final int from = rand.nextInt(n);
						final int dist[] = distancesFrom(g, from);
						long totDist = 0;
						int reachable = 0;
						for (int k = 0; k < n; k++)
							if (dist[k] < Integer.MAX_VALUE) {
								reachable++;
								totDist += dist[k];
							}
						assertEquals(1.0, reachable / hyperBall.count(from), 0.20);

						final double expEcc = (double)totDist / reachable;
						final double computedEcc = hyperBall.sumOfDistances[from] / hyperBall.count(from);
						if (expEcc == 0) assertEquals(0.0, computedEcc, 1E-3);
						else assertEquals(1.0, expEcc / computedEcc, 0.15);
					}

					hyperBall.close();
				}
			}
		}
	}

	@Test
	public void testErdosRenyiHarmonic() throws IOException {
		final XoRoShiRo128PlusRandom rand = new XoRoShiRo128PlusRandom(1);
		for(final int log2m: new int[] { 15 }) {
			for(final int size: new int[] { 10, 100, 500 }) {
				for(final boolean weights: new boolean[] { false, true }) {
					for(int attempt = 0; attempt < 5; attempt++) {
						System.err.println("log2m: " + log2m + " size: " + size + " attempt: " + attempt);
						final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(size, .1, attempt, false)).immutableView();
						final int[] weight;
						if (weights) {
							weight = new int[size];
							for(int i = size; i-- != 0;) weight[i] = i % 4;
						}
						else weight = null;

						final HyperBall hyperBall =
								new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10 * (attempt % 3), 10, attempt % 2 == 0, true, true, null, weight, attempt);
						hyperBall.init();
						do {
							hyperBall.iterate();
						} while(hyperBall.modified() != 0);

						final int n = g.numNodes();
						for (int i = 0; i < 10; i++) {
							final int from = rand.nextInt(n);
							final int dist[] = distancesFrom(g, from);
							double totDist = 0;
							for (int k = 0; k < n; k++)
								if (dist[k] < Integer.MAX_VALUE && dist[k] > 0)
									totDist += (double)(weight == null ? 1 : weight[k]) / dist[k];
							final double expHarm = n / totDist;
							final double computedHarm = n / hyperBall.sumOfInverseDistances[from];
							if (totDist != 0) assertEquals(1.0, expHarm / computedHarm, 0.13);
						}

						hyperBall.close();
					}
				}
			}
		}
	}


	@Test
	public void testErdosRenyiGain() throws IOException {
		for(final int log2m: new int[] { 15 }) {
			for(final int size: new int[] { 10, 100, 500 }) {
				for(int attempt = 0; attempt < 5; attempt++) {
					System.err.println("log2m: " + log2m + " size: " + size + " attempt: " + attempt);
					final ImmutableGraph g = new ArrayListMutableGraph(new ErdosRenyiGraph(size, .1, attempt, false)).immutableView();
					final HyperBall hyperBall =
						new HyperBall(g, attempt % 3 == 0 ? null : Transform.transpose(g), log2m, null, 0, 10 * (attempt % 3), 10, attempt % 2 == 0, true, true, new Int2DoubleFunction[] {
							new HyperBall.AbstractDiscountFunction() {
								private static final long serialVersionUID = 1L;
								@Override
								public double get(final int distance) {
									return distance;
								}
							},
							new HyperBall.AbstractDiscountFunction() {
								private static final long serialVersionUID = 1L;
								@Override
								public double get(final int distance) {
									return 1. / distance;
								}
							}
						},
							attempt);
					hyperBall.init();
					do {
						hyperBall.iterate();
					} while(hyperBall.modified() != 0);

					final int n = g.numNodes();
					for (int i = 0; i < n; i++) {
						assertEquals(hyperBall.sumOfDistances[i], hyperBall.discountedCentrality[0][i], 1E-5);
						assertEquals(hyperBall.sumOfInverseDistances[i], hyperBall.discountedCentrality[1][i], 1E-5);
					}
					hyperBall.close();
				}
			}
		}
	}
	}
