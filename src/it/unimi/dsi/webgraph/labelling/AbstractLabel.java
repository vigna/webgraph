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

/** An abstract implementation throwing an {@link IllegalArgumentException} on all primitive-type methods. */

public abstract class AbstractLabel implements Label {

	@Override
	public byte getByte() throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public short getShort(final String key) throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public int getInt(final String key) throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public long getLong(final String key) throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public float getFloat(final String key) throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public double getDouble(final String key) throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public char getChar(final String key) throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public boolean getBoolean(final String key) throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public byte getByte(final String key) throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public short getShort() throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public int getInt() throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public long getLong() throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public float getFloat() throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public double getDouble() throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public char getChar() throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}

	@Override
	public boolean getBoolean() throws IllegalArgumentException {
		throw new IllegalArgumentException();
	}
}
