/*
 * Copyright (C) 2003-2023 Paolo Boldi
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

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntIterator;

public class MergedIntIteratorTest {

	public void testMerge(final int n0, final int n1) {
		final Random r = new Random();
		final int x0[] = new int[n0];
		final int x1[] = new int[n1];
		int i, p = 0;

		// Generate
		for (i = 0; i < n0; i++) p = x0[i] = p + r.nextInt(10);
		p = 0;
		for (i = 0; i < n1; i++) p = x1[i] = p + (int)(Math.random() * 10);

		final IntAVLTreeSet s0 = new IntAVLTreeSet(x0);
		final IntAVLTreeSet s1 = new IntAVLTreeSet(x1);
		final IntAVLTreeSet res = new IntAVLTreeSet(s0);
		res.addAll(s1);

		final MergedIntIterator m = new MergedIntIterator(LazyIntIterators.lazy(s0.iterator()), LazyIntIterators.lazy(s1.iterator()));
		final IntIterator it = res.iterator();

		int x;
		while ((x = m.nextInt()) != -1) assertEquals(it.nextInt(), x);
		assertEquals(Boolean.valueOf(it.hasNext()), Boolean.valueOf(m.nextInt() != -1));
	}

	@Test
	public void testMerge() {
		for(int i = 0; i < 10; i++) {
			testMerge(i, i);
			testMerge(i, i + 1);
			testMerge(i, i * 2);
		}
	}
}
