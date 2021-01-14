/*
 * Copyright (C) 2007-2020 Paolo Boldi and Sebastiano Vigna
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

/** A way to merge two labels into one; the actual merge is performed by the {@link #merge(Label, Label)}
 *  method. Usually, strategies require that the two labels provided are of
 *  the same kind (i.e., instances of the same {@link it.unimi.dsi.webgraph.labelling.Label}
 *  class). Moreover, some strategies only accept label of a certain type,
 *  and throw an {@link java.lang.IllegalArgumentException} if the type
 *  is wrong.
 *
 */
public interface LabelMergeStrategy {

	/** Merges two given labels; either label may be <code>null</code>, but not
	 *  both. Implementing classes may decide to throw an {@link IllegalArgumentException}
	 *  if the labels provided are not of the same type, or not of a
	 *  specific type.
	 *
	 * @param first the first label to be merged.
	 * @param second the second label to be merged.
	 * @return the resulting label (note that the returned label may be reused by the
	 *  implementing class, so users are invited to make a {@link Label#copy()}
	 *  of it if they need to keep the label in between calls).
	 */
	public Label merge(Label first, Label second);

}
