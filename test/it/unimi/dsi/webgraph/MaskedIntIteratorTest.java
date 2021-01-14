/*
 * Copyright (C) 2007-2020 Sebastiano Vigna
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

package it.unimi.dsi.webgraph;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

public class MaskedIntIteratorTest {

	public void test(final int length, final int numberOfZeroes) {
		final long seed = System.currentTimeMillis();
		final Random random = new Random(seed);
		System.err.println("Seed: " + seed);
		// Reads the length and number of 0s
		final int x[] = new int[length];
		final boolean keep[] = new boolean[length];
		final IntArrayList res = new IntArrayList();
		final IntArrayList blocks = new IntArrayList();
		int i, j, p = 0;
		boolean dep;

		// Generate
		for (i = 0; i < length; i++) p = x[i] = p + random.nextInt(1000);
		for (i = 0; i < length-numberOfZeroes; i++) keep[i] = true;
		for (i = 0; i < length; i++) {
			j = i + (int)(Math.random() * (length - i));
			dep = keep[i]; keep[i] = keep[j]; keep[j] = dep;
		}

		// Compute result
		for (i = 0; i < length; i++) if (keep[i]) res.add(x[i]);
		res.trim();
		final int result[] = res.elements();

		// Prepare blocks
		boolean lookAt = true;
		int curr = 0;
		for (i = 0; i < length; i++) {
			if (keep[i] == lookAt) curr++;
			else {
				blocks.add(curr);
				lookAt = !lookAt;
				curr = 1;
			}
		}
		blocks.trim();
		final int bs[] = blocks.elements();

		// Output
		System.out.println("GENERATED:");
		for (i = 0; i < length; i++) {
			if (keep[i]) System.out.print('*');
			System.out.print(x[i] + "  ");
		}
		System.out.println("\nBLOCKS:");
		for (i = 0; i < bs.length; i++)
			System.out.print(bs[i] + "  ");
		System.out.println("\nEXPECTED RESULT:");
		for (i = 0; i < result.length; i++)
			System.out.print(result[i] + "  ");
		System.out.println();

		LazyIntIterator maskedIterator = new MaskedIntIterator(bs, LazyIntIterators.lazy(new IntArrayList(x).iterator()));

		for (i = 0; i < result.length; i++) assertEquals(i + ": ", result[i], maskedIterator.nextInt());
		assertEquals(-1, maskedIterator.nextInt());

		// Test skips
		maskedIterator = new MaskedIntIterator(bs, LazyIntIterators.lazy(new IntArrayList(x).iterator()));
		final IntIterator results = IntIterators.wrap(result);

		for (i = 0; i < result.length; i++) {
			final int toSkip = random.nextInt(5);
			assertEquals(results.skip(toSkip), maskedIterator.skip(toSkip));
			if (results.hasNext()) assertEquals(i + ": ", results.nextInt(), maskedIterator.nextInt());
		}
		assertEquals(-1, maskedIterator.nextInt());

	}

	@Test
	public void test() {
		for(int i = 0; i < 20; i++)
			for(int j = 0; j < 20; j++)
				test(i, j);
	}

}
