/*
 * Copyright (C) 2003-2021 Paolo Boldi and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.ints.IntIterator;

/** An iterator returning the union of the integers returned by two {@link IntIterator}s.
 *  The two iterators must return integers in an increasing fashion; the resulting
 *  {@link MergedIntIterator} will do the same. Duplicates will be eliminated.
 */

public class MergedIntIterator implements LazyIntIterator {
	/** The first component iterator. */
	private final LazyIntIterator it0;
	/** The second component iterator. */
	private final LazyIntIterator it1;
	/** The last integer returned by {@link #it0}. */
	private int curr0;
	/** The last integer returned by {@link #it1}. */
	private int curr1;

	/** Creates a new merged iterator by merging two given iterators; the resulting iterator will not emit more than <code>n</code> integers.
	 *
	 * @param it0 the first (monotonically nondecreasing) component iterator.
	 * @param it1 the second (monotonically nondecreasing) component iterator.
	 */
	public MergedIntIterator(final LazyIntIterator it0, final LazyIntIterator it1) {
		this.it0 = it0;
		this.it1 = it1;
		curr0 = it0.nextInt();
		curr1 = it1.nextInt();
	}

	@Override
	public int nextInt() {
		if (curr0 < curr1) {
			if (curr0 == -1) {
				final int result = curr1;
				curr1 = it1.nextInt();
				return result;
			}

			final int result = curr0;
			curr0 = it0.nextInt();
			return result;
		}
		else {
			if (curr1 == -1) {
				final int result = curr0;
				curr0 = it0.nextInt();
				return result;
			}

			final int result = curr1;
			if (curr0 == curr1) curr0 = it0.nextInt();
			curr1 = it1.nextInt();
			return result;
		}
	}

	@Override
	public int skip(final int s) {
		int i;
		for(i = 0; i < s; i++) {
			if (curr0 == -1 && curr1 == -1) break;

			if (curr0 < curr1) {
				if (curr0 == -1) curr1 = it1.nextInt();
				else curr0 = it0.nextInt();
			}
			else {
				if (curr1 == -1) curr0 = it0.nextInt();
				else  {
					if (curr0 == curr1) curr0 = it0.nextInt();
					curr1 = it1.nextInt();
				}
			}
		}
		return i;
	}
}
