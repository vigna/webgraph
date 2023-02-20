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

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.webgraph.labelling.GammaCodedIntLabel;
import it.unimi.dsi.webgraph.labelling.Label;
import it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ScatteredLabelledArcsASCIIGraphTest extends WebGraphTestCase {
	private static final Label prototype = new GammaCodedIntLabel("FOO");
	private static final LabelMapping labelMapping = (label, st) -> ((GammaCodedIntLabel)label).value = st.hashCode();

	// TODO: label tests

	@Test
	public void testConstructor() throws IOException {

		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b\n1 0 c\n1 2 d\n2 0 e\n2 1 f".getBytes("ASCII")), prototype, labelMapping);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("-1 15 a\n15 2 b\n2 -1 c\nOOPS!\n-1 2 d".getBytes("ASCII")), prototype, labelMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 0, 2 }, { 1, 2 },
				{ 2, 0 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { -1, 15, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("2 0 a\n2 1 b".getBytes("ASCII")), prototype, labelMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 0, 2 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 0, 1 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("1 2 a".getBytes("ASCII")), prototype, labelMapping);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {
				{ 0, 1 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("2 1 a".getBytes("ASCII")), prototype, labelMapping);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {
				{ 0, 1 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 1 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n2 1 b".getBytes("ASCII")), prototype, labelMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 2, 1 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("\n0 1 a\n\n2 1 b".getBytes("ASCII")), prototype, labelMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 2, 1 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("\n0 1 a\n# comment\n2 b\n2 1 c\n2 X d".getBytes("ASCII")), prototype, labelMapping);
		System.out.println(g);
		assertEquals(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 2, 1 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b\n1 0 c\n1 2 d\n2 0 e\n2 1 f".getBytes("ASCII")), prototype, labelMapping, true, false, 1);
		System.out.println(g);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b\n1  0 c\n1 d   \t 2 e \n2 0    f\n2 1	g".getBytes("ASCII")), prototype, labelMapping, true, false, 1);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("2 0 a\n2 1 b".getBytes("ASCII")), prototype, labelMapping, true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 0, 2 } }).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 0, 1 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("1 2 a".getBytes("ASCII")), prototype, labelMapping, true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(2, new int[][] {
				{ 0, 1 } }).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("2 1 a".getBytes("ASCII")), prototype, labelMapping, true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(2, new int[][] {
				{ 0, 1 } }).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 1 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n2 1 b".getBytes("ASCII")), prototype, labelMapping, true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 2, 1 } }).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("\n0 1 a\n\n2 1 b".getBytes("ASCII")), prototype, labelMapping, true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 2, 1 } }).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("\n0 1 a\n# comment\n2\n2 1 b\n2 X".getBytes("ASCII")), prototype, labelMapping, true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 2, 1 } }).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 0 a\n0 1 b\n0 2 c\n2 2 d\n1 0 e\n1 2 f\n2 0 g\n2 1 h".getBytes("ASCII")), prototype, labelMapping, true, true, 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

	}

	@Test
	public void testConstructorWithStrings() throws IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);

		map.clear();
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b\n1 0 c\n1 2 d\n2 0 e\n2 1 f".getBytes("ASCII")), map, prototype, labelMapping, null, 3));

		map.clear();
		map.put("-1", 1);
		map.put("15", 0);
		map.put("2", 2);
		final ImmutableGraph g = new ArrayListMutableGraph(3, new int[][] { { 0, 2 }, { 1, 0 }, { 1, 2 },
				{ 2, 1 } }).immutableView();
		assertEquals(g, new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("-1 15 a\n15 2 b\n2 -1 c\nOOPS!\n-1 2 d".getBytes("ASCII")), map, prototype, labelMapping, null, 3));
		assertEquals(g, new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("-1 15 a\n15 2 b\n2 -1 c\nOOPS!\n-1 2 d\n32 2 e\n2 32 f".getBytes("ASCII")), map, prototype, labelMapping, null, 3));

		map.clear();
		map.put("topo", 0);
		map.put("cane", 1);
		map.put("topocane", 2);
		assertEquals(g, new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("topocane cane a\ncane topo b\ncane topocane c\ntopo topocane d\n".getBytes("ASCII")), map, prototype, labelMapping, null, 3));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTargetOutOfRange() throws IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b".getBytes("ASCII")), map, prototype, labelMapping, null, 2));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSourceOutOfRange() throws IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n2 0 b".getBytes("ASCII")), map, prototype, labelMapping, null, 2));
	}

	private static Iterator<long[]> toArcsIterator(final String s) {
		return Arrays.stream(s.split("\n")).map(l -> Arrays.stream(l.split(" ")).mapToLong(Long::parseLong).toArray()).iterator();
	}

	private static Iterator<Label> toLabelIterator(final String s) {
		Label copy = prototype.copy();
		return Arrays.stream(s.split(" ")).map(l -> {
			labelMapping.apply(copy, l);
			return copy.copy();
		}).iterator();
	}

	@Test
	public void testConstructorWithArray() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n0 2\n1 0\n1 2\n2 0\n2 1"), toLabelIterator("a b c d e f"), false, false, 100, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		System.out.println(Arrays.toString(g.ids));
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("-1 15\n15 2\n2 -1\n-1 2"), toLabelIterator("a b c d"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 0, 2 }, { 1, 2 },
				{ 2, 0 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { -1, 15, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 0\n2 1"), toLabelIterator("a b"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 0, 2 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 0, 1 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("1 2"), toLabelIterator("a b"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {
				{ 0, 1 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 1"), toLabelIterator("a b"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {
				{ 0, 1 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 1 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n2 1"), toLabelIterator("a b"), false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 2, 1 } }).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n0 2\n1 0\n1 2\n2 0\n2 1"), toLabelIterator("a b c d e f"), true, false, 1, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 0\n2 1"), toLabelIterator("a b"), true, false, 1, null, null);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] { { 0, 1 },
				{ 0, 2 } }).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 2, 0, 1 }, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 0\n0 1\n0 2\n2 2\n1 0\n1 2\n2 0\n2 1"), toLabelIterator("a b c d e f h g"), true, true, 2, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] { 0, 1, 2 }, g.ids);
	}

}
