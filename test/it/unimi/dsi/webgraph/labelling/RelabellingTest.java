/*
 * Copyright (C) 2007-2020 Paolo Boldi
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import it.unimi.dsi.webgraph.WebGraphTestCase;
import it.unimi.dsi.webgraph.examples.IntegerTriplesArcLabelledImmutableGraph;

public class RelabellingTest extends WebGraphTestCase {

	@Test
	public void testIntRelabelling() {
		// Take a graph and convert from gamma to fixed-width
		final ArcLabelledImmutableGraph gorig = new IntegerTriplesArcLabelledImmutableGraph(new int[][]
		     {
				{ 0, 1, 203 }, { 0, 2, 104 }, { 1, 3, 102 }
		     });
		final ArcLabelledImmutableGraph gfixed = new ArcRelabelledImmutableGraph(gorig, new FixedWidthIntLabel("FOO", 15), ArcRelabelledImmutableGraph.INT_LABEL_CONVERSION_STRATEGY);
		assertGraph(gorig);
		assertGraph(gfixed);
		assertEquals(gorig, gfixed);

		// Convert its labels to lists, digitwise; e.g. 203-> [2,0,3]...
		final ArcLabelledImmutableGraph glist = new ArcRelabelledImmutableGraph(gorig, new FixedWidthIntListLabel("FOO", 15), (from, to, source, target) -> {
			final String sValue = Integer.toString(((AbstractIntLabel)from).value);
			final int[] s = new int[sValue.length()];
			for (int i = 0; i < sValue.length(); i++) s[i] = sValue.charAt(i) - '0';
			((AbstractIntListLabel)to).value = s;
		});
		// ...and then back to integer, but backwards; e.g. [2,0,3] -> 302...
		final ArcLabelledImmutableGraph grevert = new ArcRelabelledImmutableGraph(glist, new FixedWidthIntLabel("FOO", 15), (from, to, source, target) -> {
			final int[] v = ((AbstractIntListLabel)from).value;
			int tot = 0;
			for (int i = v.length - 1; i >= 0; i--)
				tot = tot * 10 + v[i];
			((AbstractIntLabel)to).value = tot;
		});
		assertGraph(glist);
		assertGraph(grevert);
		assertGraph(new ArcRelabelledImmutableGraph(gorig, new FixedWidthIntLabel("FOO", 15), (from, to, source, target) -> {
		}));


		// Check the result is correct
		assertEquals(grevert, new IntegerTriplesArcLabelledImmutableGraph(new int[][]
             {
				{ 0, 1, 302 }, { 0, 2, 401 }, { 1, 3, 201 }
		     }));
	}


}
