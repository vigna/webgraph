/*
 * Copyright (C) 2013-2023 Sebastiano Vigna
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

/** A skippable {@linkplain LazyIntIterator lazy iterator over the integers}.
 *
 * <p>An instance of this class represent an iterator over integers
 * that returns elements in increasing order. The iterator makes it possible to {@linkplain #skipTo(int) skip elements
 * by <em>value</em>}.
 */

public interface LazyIntSkippableIterator extends LazyIntIterator {
	public static final int END_OF_LIST = Integer.MAX_VALUE;

	/** Skips to a given element.
	 *
	 * <p>Note that this interface is <em>fragile</em>: after {@link #END_OF_LIST}
	 * has been returned, the behavour of further calls to this method will be
	 * unpredictable.
	 *
	 * @param lowerBound a lower bound to the returned element.
	 * @return if the last returned element is greater than or equal to
	 * {@code lowerBound}, the last returned element; otherwise,
	 * the smallest element greater
	 * than or equal to <code>lowerBound</code> that would be
	 * returned by this iterator, or {@link #END_OF_LIST}
	 * if no such element exists.
	 */
	public int skipTo(int lowerBound);
}
