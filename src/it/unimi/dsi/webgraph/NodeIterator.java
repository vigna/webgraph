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

import java.util.NoSuchElementException;

import it.unimi.dsi.fastutil.ints.IntIterator;

/** This interface extends {@link IntIterator} and is used to scan a graph, that is, to read its nodes and their successor lists
 *  sequentially. The {@link #nextInt()} method returns the node that will be scanned. After a call to this method, calling
 *  {@link #successors()} or {@link #successorArray()} will return the list of successors.
 *
 *  <p>Implementing subclasses can override either {@link #successors()} or
 *  {@link #successorArray()}, but at least one of them <strong>must</strong> be implemented.
 *
 *  <p>The {@link #copy(int)} methods is in fact optional, but should be implemented whenever the graph can be
 *  scanned more than once.
 */
public abstract class NodeIterator implements IntIterator {

	/** An empty node iterator.
	 */
	public static final NodeIterator EMPTY = new NodeIterator() {
		@Override
		public NodeIterator copy(final int upperBound) {
			return this;
		}
		@Override
		public boolean hasNext() {
			return false;
		}
		@Override
		public int outdegree() {
			throw new IllegalStateException();
		}
		@Override
		public int nextInt() {
			throw new NoSuchElementException();
		}
	};

	/** Returns the outdegree of the current node.
	 *
	 *  @return the outdegree of the current node.
	 */
	public abstract int outdegree();

	/** Returns a lazy iterator over the successors of the current node.  The iteration terminates
	 * when -1 is returned.
	 *
	 * <P>This implementation just wraps the array returned by {@link #successorArray()}.
	 *
	 *  @return a lazy iterator over the successors of the current node.
	 */
	public LazyIntIterator successors() {
		return LazyIntIterators.wrap(successorArray(), outdegree());
	}

	/**
	 * Returns a reference to an array containing the successors of the current node.
	 *
	 * <P>
	 * The returned array may contain more entries than the outdegree of the current node. However, only
	 * those with indices from 0 (inclusive) to the outdegree of the current node (exclusive) contain
	 * valid data.
	 *
	 * @implSpec This implementation just unwrap the iterator returned by {@link #successors()}.
	 *
	 * @return an array whose first elements are the successors of the current node; the array must not
	 *         be modified by the caller.
	 */
	public int[] successorArray() {
		final int[] successor = new int[outdegree()];
		LazyIntIterators.unwrap(successors(), successor);
		return successor;
	}

	/**
	 * Creates a copy of this iterator that will never return nodes &ge; the specified bound; the copy
	 * must be accessible by a different thread. Optional operation (it should be implemented by all
	 * classes that allow to scan the graph more than once).
	 *
	 * @implSpec This implementation just throws an {@link UnsupportedOperationException}. It should be
	 *           kept in sync with the result of {@link ImmutableGraph#hasCopiableIterators()}.
	 *
	 * @param upperBound the upper bound.
	 * @return a copy of this iterator, with the given upper bound.
	 */
	public NodeIterator copy(final int upperBound) {
		throw new UnsupportedOperationException();
	}
}
