/*
 * Copyright (C) 2007-2023 Sebastiano Vigna
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

package it.unimi.dsi.webgraph;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;

public class ArcListASCIIGraphTest extends WebGraphTestCase {

	@Test
	public void testLoadOnce() throws UnsupportedEncodingException, IOException {

		ArcListASCIIGraph g = ArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("0 2\n0 1\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")));
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("0 1\n0 2\n1  0\n1 \t 2\n2 0\n2 1".getBytes("ASCII")));
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("2 0\n2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,0},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("1 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{1,2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("0 1\n2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("\n\n0 1\n2 1\n\n".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}


}
