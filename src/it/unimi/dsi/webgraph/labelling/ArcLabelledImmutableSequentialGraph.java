/*
 * Copyright (C) 2007-2021 Paolo Boldi
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

package it.unimi.dsi.webgraph.labelling;

import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

/** An abstract arc-labelled immutable graph that throws an {@link java.lang.UnsupportedOperationException}
 * on all random-access methods.
 *
 * <p>The main purpose of this class is to be used as a base for the numerous anonymous
 * classes that do not support random access.
 */

public abstract class ArcLabelledImmutableSequentialGraph extends ArcLabelledImmutableGraph {
	/** Throws an {@link java.lang.UnsupportedOperationException}. */
	@Override
	public int[] successorArray(final int x) { throw new UnsupportedOperationException(); }
	/** Throws an {@link java.lang.UnsupportedOperationException}. */
	@Override
	public Label[] labelArray(final int x) { throw new UnsupportedOperationException(); }
	/** Throws an {@link java.lang.UnsupportedOperationException}. */
	@Override
	public int outdegree(final int x) { throw new UnsupportedOperationException(); }
	/** Throws an {@link java.lang.UnsupportedOperationException}. */
	@Override
	public ArcLabelledNodeIterator nodeIterator(final int x) {
		if (x == 0) return nodeIterator();
		throw new UnsupportedOperationException();
	}
	/** Throws an {@link java.lang.UnsupportedOperationException}. */
	@Override
	public LabelledArcIterator successors(final int x) { throw new UnsupportedOperationException(); }
	/** Returns false.
	 * @return false.
	 */
	@Override
	public boolean randomAccess() { return false; }

	/** Throws an {@link UnsupportedOperationException}. */
	@Override
	public ArcLabelledImmutableGraph copy() { throw new UnsupportedOperationException(); }
}
