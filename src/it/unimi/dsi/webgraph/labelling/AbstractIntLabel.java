/*
 * Copyright (C) 2007-2023 Paolo Boldi and Sebastiano Vigna
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

/** An abstract (single-attribute) integer label.
 *
 * <p>This class provides basic methods for a label holding an integer.
 * Concrete implementations may impose further requirements on the integer.
 *
 * <p>Implementing subclasses must provide constructors, {@link Label#copy()},
 * {@link Label#fromBitStream(it.unimi.dsi.io.InputBitStream, int)}, {@link Label#toBitStream(it.unimi.dsi.io.OutputBitStream, int)}
 * and possibly override {@link #toString()}.
 */

public abstract class AbstractIntLabel extends AbstractLabel implements Label {
	/** The key of the attribute represented by this label. */
	protected final String key;
	/** The value of the attribute represented by this label. */
	public int value;

	/** Creates an int label with given key and value.
	 *
	 * @param key the (only) key of this label.
	 * @param value the value of this label.
	 */
	public AbstractIntLabel(final String key, final int value) {
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
		return new Class<?>[] { int.class };
	}

	@Override
	public Object get(final String key) {
		return Integer.valueOf(getInt(key));
	}

	@Override
	public int getInt(final String key) {
		if (this.key.equals(key)) return value;
		throw new IllegalArgumentException("Unknown key " + key);
	}

	@Override
	public long getLong(final String key) {
		return getInt(key);
	}

	@Override
	public float getFloat(final String key) {
		return getInt(key);
	}

	@Override
	public double getDouble(final String key) {
		return getInt(key);
	}

	@Override
	public Object get() {
		return Integer.valueOf(getInt());
	}

	@Override
	public int getInt() {
		return value;
	}

	@Override
	public long getLong() {
		return value;
	}

	@Override
	public float getFloat() {
		return value;
	}

	@Override
	public double getDouble() {
		return value;
	}

	@Override
	public String toString() {
		return key + ":" + value;
	}

	@Override
	public boolean equals(final Object x) {
		if (x instanceof AbstractIntLabel) return (value == ((AbstractIntLabel)x).value);
		else return false;
	}

	@Override
	public int hashCode() {
		return value;
	}
}
