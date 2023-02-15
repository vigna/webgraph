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

import java.io.IOException;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

/** A natural number represented in {@linkplain OutputBitStream#writeGamma(int) &gamma; coding}. */

public class GammaCodedIntLabel extends AbstractIntLabel {

	/** Creates a new label with given key and value.
	 *
	 * @param key the (only) key.
	 * @param value the value of this label.
	 */
	public GammaCodedIntLabel(final String key, final int value) {
		super(key, value);
		if (value < 0) throw new IllegalArgumentException("Value cannot be negative: " + value);
	}

	/** Creates a new &gamma;-coded label using the given key and value 0.
	 *
	 * @param key one string containing the key of this label.
	 */
	public GammaCodedIntLabel(final String... key) {
		super(key[0], 0);
	}

	@Override
	public GammaCodedIntLabel copy() {
		return new GammaCodedIntLabel(key, value);
	}

	/** Fills this label {@linkplain InputBitStream#readGamma() reading a &gamma;-coded natural number}
	 *  from the given input bit stream.
	 *
	 * @param inputBitStream an input bit stream.
	 * @return the number of bits read to fill this lbael.
	 */

	@Override
	public int fromBitStream(final InputBitStream inputBitStream, final int sourceUnused) throws IOException {
		final long prevRead = inputBitStream.readBits();
		value = inputBitStream.readGamma();
		return (int)(inputBitStream.readBits() - prevRead);
	}

	/** Writes this label {@linkplain OutputBitStream#writeGamma(int) as a &gamma;-coded natural number}
	 *  to the given output bit stream.
	 *
	 * @param outputBitStream an output bit stream.
	 * @return the number of bits written.
	 */

	@Override
	public int toBitStream(final OutputBitStream outputBitStream, final int sourceUnused) throws IOException {
		return outputBitStream.writeGamma(value);
	}

	/** Returns -1 (as this label has not a fixed width).
	 * @return -1.
	 */

	@Override
	public int fixedWidth() {
		return -1;
	}

	@Override
	public String toString() {
		return key + ":" + value + " (gamma)";
	}

	@Override
	public String toSpec() {
		return this.getClass().getName() + "(" + key + ")";
	}

}
