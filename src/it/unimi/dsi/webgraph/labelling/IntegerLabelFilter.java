/*
 * Copyright (C) 2008-2023 Sebastiano Vigna
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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.webgraph.Transform.LabelledArcFilter;


/** A filter for labelled graphs preserving those arcs whose integer labels are in a specified set.
 *
 * @author Sebastiano Vigna
 *
 */
public class IntegerLabelFilter implements LabelledArcFilter {
	/** The values of the label that will be preserved. */
	private final IntOpenHashSet values;
	/** The key to retrieve labels. If <code>null</code>, the well-known attribute will be retrieved. */
	private final String key;

	/** Creates a new integer-label filter.
 	 *
	 * @param key the key to be queried to filter an arc, or the empty string to query the well-known attribute.
	 * @param value a list of values that will be preserved.
	 */

	public IntegerLabelFilter(final String key, final int... value) {
		this.key = key;
		values = new IntOpenHashSet(value);
	}

	/** Creates a new integer-label filter.
 	 *
	 * @param keyAndvalues the key to be queried to filter an arc,
	 * or the empty string to query the well-known attribute, followed by a list of values that will be preserved.
	 */
	public IntegerLabelFilter(final String... keyAndvalues) {
		if (keyAndvalues.length == 0) throw new IllegalArgumentException("You must specificy a key name");
		this.key = keyAndvalues[0].length() == 0 ? null : keyAndvalues[0];
		values = new IntOpenHashSet(keyAndvalues.length);
		for(int i = 1; i < keyAndvalues.length; i++) values.add(Integer.parseInt(keyAndvalues[i]));
	}

	@Override
	public boolean accept(final int i, final int j, final Label label) {
		return values.contains(key == null ? label.getInt() : label.getInt(key));
	}
}
