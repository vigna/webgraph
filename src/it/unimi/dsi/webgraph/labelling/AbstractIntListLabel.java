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

import java.util.Arrays;

/** An abstract (single-attribute) list-of-integers label.
*
* <p>This class provides basic methods for a label holding a list of integers.
* Concrete implementations may impose further requirements on the integer.
*
* <p>Implementing subclasses must provide constructors, {@link Label#copy()},
* {@link Label#fromBitStream(it.unimi.dsi.io.InputBitStream, int)}, {@link Label#toBitStream(it.unimi.dsi.io.OutputBitStream, int)}
* and possibly override {@link #toString()}.
*/

public abstract class AbstractIntListLabel extends AbstractLabel implements Label {
	/** The key of the attribute represented by this label. */
	protected final String key;
	/** The values of the attribute represented by this label. */
	public int[] value;

	/** Creates an int label with given key and value.
	 *
	 * @param key the (only) key of this label.
	 * @param value the value of this label.
	 */
	public AbstractIntListLabel(final String key, final int[] value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public String wellKnownAttributeKey() {
		return key;
	}

	@Override
	public String[] attributeKeys() {
		return new String[] { key };
	}

	@Override
	public Class<?>[] attributeTypes() {
		return new Class<?>[] { int[].class };
	}

	@Override
	public Object get(final String key) {
		if (this.key.equals(key)) return value;
		throw new IllegalArgumentException();
	}

	@Override
	public Object get() {
		return value;
	}

	@Override
	public String toString() {
		return key + ":" + Arrays.toString(value);
	}

	@Override
	public boolean equals(final Object x) {
		if (x instanceof AbstractIntListLabel) return Arrays.equals(value, ((AbstractIntListLabel)x).value);
		else return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(value);
	}
}
