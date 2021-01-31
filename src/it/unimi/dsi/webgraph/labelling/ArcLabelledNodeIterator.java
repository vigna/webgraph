/*
 * Copyright (C) 2007-2021 Paolo Boldi and Sebastiano Vigna
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

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;

/** An iterator returning nodes, their successors and labels on the arcs.
 *
 * <p>The purpose of this abstract implementation is to override covariantly
 * the return type of of {@link NodeIterator#successors()}, so that
 * it has to be a {@link ArcLabelledNodeIterator.LabelledArcIterator}, and provide a general
 * implementation of a new {@link #labelArray()} method that returns
 * the labels of the arcs going out of the current node as an array.
 */
public abstract class ArcLabelledNodeIterator extends NodeIterator {

	/** An iterator returning successor and the labels of the arcs toward them.
	 *  The label can be accessed through {@link #label()}, which must be called just after
	 *  advancing the iterator.
	 *
	 *  <p><strong>Warning</strong>: the returned label can be the same object
	 *  upon several calls to {@link #label()}; if you need to store it,
	 *  you should {@linkplain Label#copy() copy it}.
	 */
	public interface LabelledArcIterator extends LazyIntIterator {
		/** The label of arc leading to the last returned successor.
		 *
		 * @return the label of arc leading to the last returned successor.
		 */
		public Label label();
	}

	@Override
	public abstract ArcLabelledNodeIterator.LabelledArcIterator successors();

	/**
	 * Returns a reference to an array containing the labels of the arcs going out of the current node
	 * in the same order as the order in which the corresponding successors are returned by
	 * {@link #successors()}.
	 *
	 * <P>
	 * The returned array may contain more entries than the outdegree of the current node. However, only
	 * those with indices from 0 (inclusive) to the outdegree of the current node (exclusive) contain
	 * valid data.
	 *
	 * @implSpec This implementation just unwrap the iterator returned by {@link #successors()} and
	 *           writes in a newly allocated array copies of the labels returned by
	 *           {@link LabelledArcIterator#label()}.
	 *
	 * @return an array whose first elements are the labels of the arcs going out of the current node;
	 *         the array must not be modified by the caller.
	 */

	public Label[] labelArray() {
		return unwrap(successors(), outdegree());
	}

	/** Returns a new array of labels filled with exactly <code>howMany</code> labels from the given iterator.
	 *  Note that the iterator is required to have at least as many labels as needed.
	 *
	 * @param iterator the iterator.
	 * @param howMany the number of labels.
	 * @return the new array where labels are copied.
	 */
	protected static Label[] unwrap(final ArcLabelledNodeIterator.LabelledArcIterator iterator, final int howMany) {
		final Label[] result = new Label[howMany];
		for (int i = 0; i < howMany; i++) {
			iterator.nextInt();
			result[i] = iterator.label().copy();
		}
		return result;
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
	@Override
	public ArcLabelledNodeIterator copy(final int upperBound) {
		throw new UnsupportedOperationException();
	}

}
