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

/** An abstract implementation of a lazy integer iterator, implementing {@link #skip(int)}
 * by repeated calls to {@link LazyIntIterator#nextInt() nextInt()}. */

public abstract class AbstractLazyIntIterator implements LazyIntIterator {

	@Override
	public int skip(final int n) {
		int i;
		for(i = 0; i < n && nextInt() != -1; i++);
		return i;
	}

}
