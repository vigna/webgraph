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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import com.google.common.io.Files;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;

public class ShiftedByOneArcListASCIIGraphTest extends WebGraphTestCase {

	@Test
	public void testLoadOnce() throws UnsupportedEncodingException, IOException {

		ArcListASCIIGraph g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("1 3\n1 2\n2 1\n2 3\n3 1\n3 2".getBytes("ASCII")));
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("3 1\n3 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,0},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("2 3".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{1,2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("3 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("1 2\n3 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}

	@Test
	public void testLoad() throws UnsupportedEncodingException, IOException {
		final File file = File.createTempFile(ShiftedByOneArcListASCIIGraphTest.class.getSimpleName(), ".txt");
		file.deleteOnExit();
		Files.asCharSink(file, StandardCharsets.US_ASCII).write("1 3\n1 2\n2 1\n2 3\n3 1\n3 2");
		ImmutableGraph g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());

		Files.asCharSink(file, StandardCharsets.US_ASCII).write("3 1\n3 2");
		g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,0},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		Files.asCharSink(file, StandardCharsets.US_ASCII).write("2 3");
		g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{1,2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		Files.asCharSink(file, StandardCharsets.US_ASCII).write("3 2");
		g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		Files.asCharSink(file, StandardCharsets.US_ASCII).write("1 2\n3 2");
		g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}

	@Test
	public void testLoadMapped() throws IOException {
		final File file = File.createTempFile(ShiftedByOneArcListASCIIGraphTest.class.getSimpleName(), ".txt");
		file.deleteOnExit();
		Files.asCharSink(file, StandardCharsets.US_ASCII).write("1 3\n1 2\n2 1\n2 3\n3 1\n3 2");
		ImmutableGraph g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(g).immutableView());

		Files.asCharSink(file, StandardCharsets.US_ASCII).write("3 1\n3 2");
		g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,0},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		Files.asCharSink(file, StandardCharsets.US_ASCII).write("2 3");
		g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{1,2}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		Files.asCharSink(file, StandardCharsets.US_ASCII).write("3 2");
		g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());

		Files.asCharSink(file, StandardCharsets.US_ASCII).write("1 2\n3 2");
		g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}
}
