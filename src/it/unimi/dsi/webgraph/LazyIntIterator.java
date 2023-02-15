/*
 * Copyright (C) 2007-2023 Sebastiano Vigna
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

/** A lazy iterator over the integers.
 *
 * <p>An instance of this class represent a (skippable) iterator over the integers.
 * The iterator is exhausted when an implementation-dependent special marker is
 * returned. This fully lazy architecture halves the number of method
 * calls w.r.t. Java's eager iterators.
 */

public interface LazyIntIterator {
	/** The next integer returned by this iterator, or the special
	 * marker if this iterator is exhausted.
	 *
	 * @return next integer returned by this iterator, or the special
	 * marker if this iterator is exhausted.
	 */
	public int nextInt();

	/** Skips a given number of elements.
	 *
	 * @param n the number of elements to skip.
	 * @return the number of elements actually skipped (which might
	 * be less than <code>n</code> if this iterator is exhausted).
	 */
	public int skip(int n);
}
