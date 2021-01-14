/*
 * Copyright (C) 2007-2020 Sebastiano Vigna
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;

public class ScatteredArcsASCIIGraphTest extends WebGraphTestCase {

	@Test
	public void testConstructor() throws UnsupportedEncodingException, IOException {

		ScatteredArcsASCIIGraph g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")));
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("-1 15\n15 2\n2 -1\nOOPS!\n-1 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2},{1,2},{2,0}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { -1, 15, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("2 0\n2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 0, 1 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("1 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 1 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("\n0 1\n\n2 1".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("\n0 1\n# comment\n2\n2 1\n2 X".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1  0\n1 \t 2\n2 0\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("2 0\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 0, 1 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("1 2".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 1 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("\n0 1\n\n2 1".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("\n0 1\n# comment\n2\n2 1\n2 X".getBytes("ASCII")), true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 0\n0 1\n0 2\n2 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")), true, true, 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

	}


	@Test
	public void testConstructorWithStrings() throws UnsupportedEncodingException, IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);

		map.clear();
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2\n1 0\n1 2\n2 0\n2 1".getBytes("ASCII")), map, null, 3));

		map.clear();
		map.put("-1", 1);
		map.put("15", 0);
		map.put("2", 2);
		final ImmutableGraph g = new ArrayListMutableGraph(3, new int[][] {{0,2},{1,0},{1,2},{2,1}}).immutableView();
		assertEquals(g, new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("-1 15\n15 2\n2 -1\nOOPS!\n-1 2".getBytes("ASCII")), map, null, 3));
		assertEquals(g, new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("-1 15\n15 2\n2 -1\nOOPS!\n-1 2\n32 2\n2 32".getBytes("ASCII")), map, null, 3));

		map.clear();
		map.put("topo", 0);
		map.put("cane", 1);
		map.put("topocane", 2);
		assertEquals(g, new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("topocane cane\ncane topo\ncane topocane\ntopo topocane\n".getBytes("ASCII")), map, null, 3));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testTargetOutOfRange() throws UnsupportedEncodingException, IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n0 2".getBytes("ASCII")), map, null, 2));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSourceOutOfRange() throws UnsupportedEncodingException, IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredArcsASCIIGraph(new FastByteArrayInputStream("0 1\n2 0".getBytes("ASCII")), map, null, 2));
	}

	private static Iterator<long[]> toIterator(final String s) {
		final String[] arcs = s.split("\n");
		final List<long[]> arcSet = new ArrayList<>();
		for (final String arc: arcs) {
			final String[] parts = arc.split(" ");
			arcSet.add(new long[] { Long.parseLong(parts[0]), Long.parseLong(parts[1]) });
		}
		return arcSet.iterator();
	}

	@Test
	public void testConstructorWithArray() throws IOException {
		ScatteredArcsASCIIGraph g = new ScatteredArcsASCIIGraph(toIterator("0 1\n0 2\n1 0\n1 2\n2 0\n2 1"), false, false, 100, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		System.out.println(Arrays.toString(g.ids));
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(toIterator("-1 15\n15 2\n2 -1\n-1 2"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2},{1,2},{2,0}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { -1, 15, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(toIterator("2 0\n2 1"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 0, 1 }, g.ids);

		g = new ScatteredArcsASCIIGraph(toIterator("1 2"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(toIterator("2 1"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 1 }, g.ids);

		g = new ScatteredArcsASCIIGraph(toIterator("0 1\n2 1"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(toIterator("0 1\n0 2\n1 0\n1 2\n2 0\n2 1"), true, false, 1, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredArcsASCIIGraph(toIterator("2 0\n2 1"), true, false, 1, null, null);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0,1},{0,2}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 0, 1 }, g.ids);

		g = new ScatteredArcsASCIIGraph(toIterator("0 0\n0 1\n0 2\n2 2\n1 0\n1 2\n2 0\n2 1"), true, true, 2, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);


	}

}
