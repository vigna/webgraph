/*
 * Copyright (C) 2008-2023 Paolo Boldi and Sebastiano Vigna
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

import it.unimi.dsi.webgraph.Transform;

/** A semiring used to compose labels.
 * <p>When {@linkplain Transform#compose(it.unimi.dsi.webgraph.ImmutableGraph, it.unimi.dsi.webgraph.ImmutableGraph) composing}
 * two labelled graphs, we need a way to combine labels along a path, and a way to combine labels from different
 * paths connecting two nodes. These two operations are implemented by
 * {@link #multiply(Label, Label)} and {@link #add(Label, Label)}. The name of the two
 * methods are due to the fact that their operations must define a <em>semiring</em>
 * for which you must also provide a {@link #zero()} and a {@link #one()}. For instance,
 * if a graph is labelled with weights, a semiring implementing {@link #multiply(Label, Label)} by
 * a standard sum and {@link #add(Label, Label)} using the minimum operator will give a composition
 * strategy that computes the shortest path connecting two nodes.
 *
 *  <p>Usually, strategies require that the two labels provided are of
 *  the same kind (i.e., instances of the same {@link it.unimi.dsi.webgraph.labelling.Label}
 *  class). Moreover, some strategies only accept label of a certain type,
 *  and throw an {@link java.lang.IllegalArgumentException} if the type
 *  is wrong.
 */
public interface LabelSemiring {

	/** Multiply two given labels; either label may be <code>null</code>, but not
	 *  both. Implementing classes may decide to throw an {@link IllegalArgumentException}
	 *  if the labels provided are not of the same type, or not of a
	 *  specific type.
	 *
	 * @param first the first label to be multiplied.
	 * @param second the second label to be multiplied.
	 * @return the resulting label (note that the returned label may be reused by the
	 *  implementing class, so users are invited to make a {@link Label#copy()}
	 *  of it if they need to keep the label in between calls).
	 */
	public Label multiply(Label first, Label second);

	/** Adds two given labels; either label may be <code>null</code>, but not
	 *  both. Implementing classes may decide to throw an {@link IllegalArgumentException}
	 *  if the labels provided are not of the same type, or not of a
	 *  specific type.
	 *
	 * @param first the first label to be added.
	 * @param second the second label to be added.
	 * @return the resulting label (note that the returned label may be reused by the
	 *  implementing class, so users are invited to make a {@link Label#copy()}
	 *  of it if they need to keep the label in between calls).
	 */
	public Label add(Label first, Label second);

	/** Returns the zero of {@link #add(Label, Label)}.
	 *
	 * @return the zero of {@link #add(Label, Label)}.
	 */
	public Label zero();

	/** Returns the one of {@link #multiply(Label, Label)}.
	 *
	 * @return the one of {@link #multiply(Label, Label)}.
	 */
	public Label one();

}
