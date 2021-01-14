/*
 * Copyright (C) 2013-2020 Sebastiano Vigna
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

import static it.unimi.dsi.bits.LongArrayBitVector.word;

import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.sux4j.bits.SimpleSelectZero;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import it.unimi.dsi.util.HyperLogLogCounterArray;
import it.unimi.dsi.webgraph.ImmutableGraph;

/**<p>A content-addressable representation of the cumulative function of outdegrees that uses a stripped-down
 * implementation of Elias&ndash;Fano's representation of monotone sequences partially taken from {@link EliasFanoMonotoneLongBigList}.
 *
 * <p>The purpose of this class is that of storing quasi-succinctly the outdegrees of a graph so that it
 * is easy to find quickly a batch of nodes whose overall outdegree is a given quantity. It is most effective
 * in multicore computations depending on the outdegree, as usually the transposed graph has some very high-degree nodes, and often
 * in web graphs, due to crawling artifacts, these nodes are very close. As a result, a node-based job assignment
 * ends up in creating batches of nodes that are incredibly expensive, which in turns produced an unbalanced
 * iteration (e.g., in the last part few processors are actually working).
 *
 * <p>The main access method is {@link #skipTo(long)}, which will return a value of the cumulative function larger than or equal to
 * its argument. At that point, {@link #currentIndex()} returns the index of the node that realize that value.
 */

public final class EliasFanoCumulativeOutdegreeList {
	/** The number of lower bits. */
	private final int l;
	/** The mask used to round up returned {@link #currentIndex} values when {@link HyperLogLogCounterArray#m} &lt; 64, 0 otherwise. */
	private final long roundingMask;
	/** The upper-bits array. */
	private final long[] upperBits;
	/** The lower-bits list. */
	private final LongBigList lowerBits;
	/** The number of nodes, cached. */
	private final long numNodes;
	/** The 64-bit window. */
	private long window;
	/** The current word position in the list of upper bits. */
	private int curr;
	/** The index of the current prefix sum. */
	private int currentIndex;
	/** A zero-selection structure on {@link #upperBits}. */
	private final SimpleSelectZero simpleSelectZero;

	/** Creates a cumulative outdegree list with no rounding mask.
	 *
	 * @param graph a graph.
	 */
	public EliasFanoCumulativeOutdegreeList(final ImmutableGraph graph) {
		this(graph, graph.numArcs());
	}

	/** Creates a cumulative outdegree list with no rounding mask.
	 *
	 * @param graph a graph.
	 * @param numArcs the number of arcs in the graph (this parameter can be useful as some {@link ImmutableGraph} implementations
	 * do not support {@link ImmutableGraph#numArcs()}).
	 */
	public EliasFanoCumulativeOutdegreeList(final ImmutableGraph graph, final long numArcs) {
		this(graph, numArcs, 0);
	}

	/** Creates a cumulative outdegree list with specified rounding mask.
	 *
	 * @param graph a graph.
	 * @param numArcs the number of arcs in the graph (this parameter can be useful as some {@link ImmutableGraph} implementations
	 * do not support {@link ImmutableGraph#numArcs()}).
	 * @param roundingMask a number of the form 2<sup><var>k</var></sup> &minus; 1. After each call to {@link #skipTo(long)},
	 * {@link #currentIndex()} is guaranteed to return a multiple of 2<sup><var>k</var></sup>, unless {@link #currentIndex()} is
	 * equal to the number of nodes in {@code graph}.
	 */
	public EliasFanoCumulativeOutdegreeList(final ImmutableGraph graph, final long numArcs, final int roundingMask) {
		if (roundingMask + 1 != Integer.highestOneBit(roundingMask + 1)) throw new IllegalArgumentException("Illegal rounding mask: " + roundingMask);
		this.roundingMask = roundingMask;
		final long length = numNodes = graph.numNodes();
		final long upperBound = numArcs;
		l = length == 0 ? 0 : Math.max(0, Fast.mostSignificantBit(upperBound / length));
		final long lowerBitsMask = (1L << l) - 1;
		final LongBigList lowerBitsList = LongArrayBitVector.getInstance().asLongBigList(l);
		lowerBitsList.size(length);
		final BitVector upperBitsVector = LongArrayBitVector.getInstance().length(length + (upperBound >>> l) + 1);
		long v = 0;
		for(int i = 0; i < length; i++) {
			v += graph.outdegree(i);
			if (v > upperBound) throw new IllegalArgumentException("Too large value: " + v + " > " + upperBound);
			if (l != 0) lowerBitsList.set(i, v & lowerBitsMask);
			upperBitsVector.set((v >>> l) + i);
		}

		lowerBits = lowerBitsList;
		upperBits = upperBitsVector.bits();
		simpleSelectZero = new SimpleSelectZero(upperBitsVector);
		currentIndex = -1;
	}

	private long getNextUpperBits() {
		assert currentIndex < numNodes;
		while(window == 0) window = upperBits[++curr];
		final long upperBits = curr * (long)Long.SIZE + Long.numberOfTrailingZeros(window) - currentIndex++;
		window &= window - 1;
		return upperBits;
	}

	/** Returns the index realizing the last value returned by {@link #skipTo(long)}, that is,
	 * an index <var>x</var> such that the sum of the outdegrees of the nodes of index (strictly) smaller
	 * than <var>x</var> is equal to the last value returned by {@link #skipTo(long)}.
	 *
	 * @return the index of the node realizing the last value returned by {@link #skipTo(long)}, or -1 if {@link #skipTo(long)} has never been called.
	 */
	public int currentIndex() {
		return currentIndex;
	}

	/** Returns the first value of the cumulative function of outdegrees that is larger than or equal to the provided bound and
	 * that respect the rounding mask provided at construction time.
	 *
	 * @param lowerBound a lower bound on the returned value.
	 * @return the first value of the cumulative function of outdegrees that is larger than or equal to {@code lowerBound} and
	 * that respect the rounding mask provided at construction time.
	 */

	public long skipTo(final long lowerBound) {
		final long zeroesToSkip = (lowerBound >>> l) - 1;
		final long position = zeroesToSkip == -1 ? 0 : simpleSelectZero.selectZero(zeroesToSkip);
		window = upperBits[curr = word(position)];
		window &= -1L << position;
		assert zeroesToSkip == -1 || position - zeroesToSkip <= Integer.MAX_VALUE : position - zeroesToSkip;
		currentIndex = (int)(zeroesToSkip == -1 ? 0 : position - zeroesToSkip);

		for(;;) {
			final long lower = lowerBits.getLong(currentIndex);
			final long last = getNextUpperBits() << l | lower;
			if (last >= lowerBound && (currentIndex & roundingMask) == 0 || currentIndex == numNodes) return last;
		}
	}
}
