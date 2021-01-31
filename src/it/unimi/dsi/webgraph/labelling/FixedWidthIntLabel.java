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

import java.io.IOException;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

/** An integer represented in fixed width. The provided width must
 * be smaller than 32.
 */

public class FixedWidthIntLabel extends AbstractIntLabel {
	/** The bit width used to represent the value of this label. */
	protected final int width;

	/** Creates a new fixed-width int label.
	 *
	 * @param key the (only) key of this label.
	 * @param width the label width (in bits).
	 * @param value the value of this label.
	 */
	public FixedWidthIntLabel(final String key, final int width, final int value) {
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
	public FixedWidthIntLabel(final String key, final int width) {
		this(key, width, 0);
	}

	/** Creates a new fixed-width integer label using the given key and width
	 *  with value 0.
	 *
	 * @param arg two strings containing the key and the width of this label.
	 */
	public FixedWidthIntLabel(final String... arg) {
		this(arg[0], Integer.parseInt(arg[1]));
	}

	@Override
	public Label copy() {
		return new FixedWidthIntLabel(key, width, value);
	}

	@Override
	public int fromBitStream(final InputBitStream inputBitStream, final int sourceUnused) throws IOException {
		value = inputBitStream.readInt(width);
		return width;
	}

	@Override
	public int toBitStream(final OutputBitStream outputBitStream, final int sourceUnused) throws IOException {
		return outputBitStream.writeInt(value, width);
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
	public String toSpec() {
		return this.getClass().getName() + "(" + key + ","  + width + ")";
	}
}
