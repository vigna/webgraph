/*
 * Copyright (C) 2007-2023 Paolo Boldi
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

import java.io.IOException;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

/** An integer represented in fixed width, that fakely provides context sensitivity:
 * when storing label <var>v</var> onto the arc (<var>x</var>,<var>y</var>),
 * the value <var>v</var>*(<var>x</var>+1) is stored instead. The provided width must
 * be smaller than 32.
 */

public class FakeCSFixedWidthIntLabel extends AbstractIntLabel {
	/** The bit width used to represent the value of this label. */
	private final int width;

	/** Creates a new fixed-width int label.
	 *
	 * @param key the (only) key of this label.
	 * @param width the label width (in bits).
	 * @param value the value of this label.
	 */
	public FakeCSFixedWidthIntLabel(final String key, final int width, final int value) {
		super(key, value);
		if (width < 0 || width > 31) throw new IllegalArgumentException("Width out of range: " + width);
		if (value < 0 || value >= 1L << width) throw new IllegalArgumentException("Value out of range: " + Integer.toString(value));
		this.width = width;
	}

	/** Creates a new fixed-width int label of value 0.
	 *
	 * @param key the (only) key of this label.
	 * @param width the label width (in bits).
	 */
	public FakeCSFixedWidthIntLabel(final String key, final int width) {
		this(key, width, 0);
	}

	/** Creates a new fixed-width integer label using the given key and width
	 *  with value 0.
	 *
	 * @param arg two strings containing the key and the width of this label.
	 */
	public FakeCSFixedWidthIntLabel(final String... arg) {
		this(arg[0], Integer.parseInt(arg[1]));
	}

	@Override
	public Label copy() {
		return new FakeCSFixedWidthIntLabel(key, width, value);
	}

	/** Returns the width of this label (as provided at construction time).
	 * @return the width of this label.
	 */
	@Override
	public int fixedWidth() {
		return width;
	}

	@Override
	public String toString() {
		return key + ":" + value + " (width:" + width + ")";
	}

	@Override
	public int fromBitStream(final InputBitStream inputBitStream, final int source) throws IOException, UnsupportedOperationException {
		final int v = inputBitStream.readInt(width);
		value = v / (source + 1);
		return width;
	}

	@Override
	public int toBitStream(final OutputBitStream outputBitStream, final int source) throws IOException, UnsupportedOperationException {
		return outputBitStream.writeInt((source + 1) * value, width);
	}

	@Override
	public String toSpec() {
		return this.getClass().getName() + "(" + key + ","  + width + ")";
	}
}
