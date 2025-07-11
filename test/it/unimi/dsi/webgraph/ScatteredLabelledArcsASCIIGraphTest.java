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
import it.unimi.dsi.fastutil.longs.Long2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.webgraph.labelling.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph.LabelMapping;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ScatteredLabelledArcsASCIIGraphTest extends WebGraphTestCase {
	private static final Label gammaPrototype = new GammaCodedIntLabel("FOO");
	private static final Long2IntFunction identity = Math::toIntExact;
	private static final LabelMapping hashcodeMapping = (label, st) -> ((GammaCodedIntLabel)label).value = st.hashCode();
	private static final LabelMapping integerMapping = (label, st) -> ((GammaCodedIntLabel)label).value = Integer.parseInt((String) st);

	private static Iterator<long[]> toArcsIterator(final String s) {
		final String[] arcs = s.split("\n");
		final List<long[]> arcSet = new ArrayList<>();
		for (final String arc : arcs) {
			final String[] parts = arc.split(" ");
			arcSet.add(new long[] {Long.parseLong(parts[0]), Long.parseLong(parts[1])});
		}
		return arcSet.iterator();
	}

	private static Iterator<Label> toLabelIterator(final String s, Label prototype, LabelMapping mapping) {
		Label copy = prototype.copy();
		final String[] labels = s.split(" ");
		final List<Label> labelSet = new ArrayList<>();
		for (final String label : labels) {
			mapping.apply(copy, label);
			labelSet.add(copy.copy());
		}
		return labelSet.iterator();
	}

	private static int[][] getLabelValues(final ScatteredLabelledArcsASCIIGraph g) {
		int[][] labelValues = new int[g.numNodes()][];
		ArcLabelledNodeIterator it = g.nodeIterator();
		for (int i = 0; i < g.numNodes(); i++) {
			it.nextInt();
			Label[] labels = it.labelArray();
			labelValues[i] = Arrays.stream(labels).mapToInt(Label::getInt).toArray();
		}
		return labelValues;
	}

	private static class MergeIntegers implements LabelMergeStrategy {
		private final Label prototype;

		public MergeIntegers(Label prototype) {
			this.prototype = prototype;
		}

		@Override
		public Label merge(final Label first, final Label second) {
			((GammaCodedIntLabel) prototype).value = first.getInt() + second.getInt();
			return prototype;
		}
	}

	@Test
	public void testConstructor() throws IOException {

		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b\n1 0 c\n1 2 d\n2 0 e\n2 1 f".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("-1 15 a\n15 2 b\n2 -1 c\nOOPS!\n-1 2 d".getBytes("ASCII")), gammaPrototype, hashcodeMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {0, 2}, {1, 2}, {2, 0}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {-1, 15, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("2 0 a\n2 1 b".getBytes("ASCII")), gammaPrototype, hashcodeMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {0, 2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 0, 1}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("1 2 a".getBytes("ASCII")), gammaPrototype, hashcodeMapping);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("2 1 a".getBytes("ASCII")), gammaPrototype, hashcodeMapping);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 1}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n2 1 b".getBytes("ASCII")), gammaPrototype, hashcodeMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {2, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("\n0 1 a\n\n2 1 b".getBytes("ASCII")), gammaPrototype, hashcodeMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {2, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("\n0 1 a\n# comment\n2 b\n2 1 c\n2 X d".getBytes("ASCII")), gammaPrototype, hashcodeMapping);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {2, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b\n1 0 c\n1 2 d\n2 0 e\n2 1 f".getBytes("ASCII")), gammaPrototype, hashcodeMapping);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b\n1  0 c\n1 d   \t 2 e \n2 0    f\n2 1	g".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null, true, false);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("2 0 a\n2 1 b".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null, true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {0, 2}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 0, 1}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("1 2 a".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null,true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("2 1 a".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null,true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 1}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n2 1 b".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null,true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {2, 1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("\n0 1 a\n\n2 1 b".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null,true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {2, 1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("\n0 1 a\n# comment\n2\n2 1 b\n2 X".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null, true, false, 1);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {2, 1}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 0 a\n0 1 b\n0 2 c\n2 2 d\n1 0 e\n1 2 f\n2 0 g\n2 1 h".getBytes("ASCII")), gammaPrototype, hashcodeMapping, null, true, true, 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

	}

	@Test
	public void testConstructorWithStrings() throws IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);

		map.clear();
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b\n1 0 c\n1 2 d\n2 0 e\n2 1 f".getBytes("ASCII")), map, null, 3, gammaPrototype, hashcodeMapping, null));

		map.clear();
		map.put("-1", 1);
		map.put("15", 0);
		map.put("2", 2);
		final ImmutableGraph g = new ArrayListMutableGraph(3, new int[][] {{0, 2}, {1, 0}, {1, 2}, {2, 1}}).immutableView();
		assertEquals(g, new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("-1 15 a\n15 2 b\n2 -1 c\nOOPS!\n-1 2 d".getBytes("ASCII")), map, null, 3, gammaPrototype, hashcodeMapping, null));
		assertEquals(g, new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("-1 15 a\n15 2 b\n2 -1 c\nOOPS!\n-1 2 d\n32 2 e\n2 32 f".getBytes("ASCII")), map, null, 3, gammaPrototype, hashcodeMapping, null));

		map.clear();
		map.put("topo", 0);
		map.put("cane", 1);
		map.put("topocane", 2);
		assertEquals(g, new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("topocane cane a\ncane topo b\ncane topocane c\ntopo topocane d\n".getBytes("ASCII")), map, null, 3, gammaPrototype, hashcodeMapping, null));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTargetOutOfRange() throws IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n0 2 b".getBytes("ASCII")), map, null, 2, gammaPrototype, hashcodeMapping, null));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSourceOutOfRange() throws IOException {
		final Object2LongFunction<String> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0", 0);
		map.put("1", 1);
		map.put("2", 2);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ScatteredLabelledArcsASCIIGraph(new FastByteArrayInputStream("0 1 a\n2 0 b".getBytes("ASCII")), map, null, 2, gammaPrototype, hashcodeMapping, null));
	}

	@Test
	public void testConstructorWithArray() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n0 2\n1 0\n1 2\n2 0\n2 1"), null, -1, toLabelIterator("a b c d e f", gammaPrototype, hashcodeMapping), null, false, false, 100, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("-1 15\n15 2\n2 -1\n-1 2"), null, -1, toLabelIterator("a b c d", gammaPrototype, hashcodeMapping), null, false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {0, 2}, {1, 2}, {2, 0}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {-1, 15, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 0\n2 1"), null, -1, toLabelIterator("a b", gammaPrototype, hashcodeMapping), null, false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {0, 2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 0, 1}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("1 2"), null, -1, toLabelIterator("a b", gammaPrototype, hashcodeMapping), null, false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 1"), null, -1, toLabelIterator("a b", gammaPrototype, hashcodeMapping), null, false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 1}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n2 1"), null, -1, toLabelIterator("a b", gammaPrototype, hashcodeMapping), null, false, false, 100, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {2, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n0 2\n1 0\n1 2\n2 0\n2 1"), null, -1, toLabelIterator("a b c d e f", gammaPrototype, hashcodeMapping), null, true, false, 1, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 0\n2 1"), null, -1, toLabelIterator("a b", gammaPrototype, hashcodeMapping), null, true, false, 1, null, null);
		assertEquals(Transform.symmetrize(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {0, 2}}).immutableView()), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 0, 1}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 0\n0 1\n0 2\n2 2\n1 0\n1 2\n2 0\n2 1"), null, -1, toLabelIterator("a b c d e f h g", gammaPrototype, hashcodeMapping), null, true, true, 2, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2}, g.ids);

		g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 0\n0 1\n0 2\n2 2\n1 0\n1 2\n2 0\n2 1"), identity, 3, toLabelIterator("a b c d e f h g", gammaPrototype, hashcodeMapping), null, true, true, 2, null, null);
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMissingLabel() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 1\n1 3"), null, -1, toLabelIterator("a", gammaPrototype, hashcodeMapping), null, false, false, 1, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 1}, g.ids);
	}

	@Test
	public void testTooManyLabels() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 1\n1 3"), null, -1, toLabelIterator("a b c", gammaPrototype, hashcodeMapping), null, false, false, 1, null, null);
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0, 1}, {1, 2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 1, 3}, g.ids);
	}

	@Test
	public void testSameArcSameLabel() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 1\n2 1"), null, -1, toLabelIterator("a a", gammaPrototype, hashcodeMapping), null, false, false, 1, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 1}, g.ids);
	}

	@Test
	public void testSameArcDifferentLabel() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("2 1\n2 1"), null, -1, toLabelIterator("a b", gammaPrototype, hashcodeMapping), null, false, false, 1, null, null);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {2, 1}, g.ids);
	}

	@Test
	public void testDifferentArcSameLabel() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n2 3"), null, -1, toLabelIterator("a a", gammaPrototype, hashcodeMapping), null, false, false, 1, null, null);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}, {2, 3}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new long[] {0, 1, 2, 3}, g.ids);
	}

	@Test
	public void testLabelMergeStrategy() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n2 3\n0 1"), null, -1, toLabelIterator("5 6 9", gammaPrototype, integerMapping), new MergeIntegers(gammaPrototype), false, false, 1, null, null);
		int[][] labelValues = getLabelValues(g);

		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}, {2, 3}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new int[][] {{14}, {}, {6}, {}}, labelValues);
	}

	@Test
	public void testNoLabelMergeStrategyOnlyKeepTheLastLabel() throws IOException {
		ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(toArcsIterator("0 1\n2 3\n0 1"), null, -1, toLabelIterator("5 6 9", gammaPrototype, integerMapping), null, false, false, 10, null, null);
		int[][] labelValues = getLabelValues(g);

		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}, {2, 3}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
		assertArrayEquals(new int[][] {{9}, {}, {6}, {}}, labelValues);
	}
}
