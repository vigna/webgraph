/*
 * Copyright (C) 2010-2020 Sebastiano Vigna
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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.URISyntaxException;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.logging.ProgressLogger;

public class BuildHostMapTest extends WebGraphTestCase {

	@Test
	public void testSimpleNoLogger() throws IOException, URISyntaxException {
		final BufferedReader br = new BufferedReader(new StringReader("http://a/b\nhttp://c\nhttp://a.b:81/\nhttp://c/c\nhttp://a:80/\nhttps://a/\nhttps://a.b\nhttp://159.149.130.49/"));
		final FastByteArrayOutputStream mapFbaos = new FastByteArrayOutputStream();
		final FastByteArrayOutputStream countFbaos = new FastByteArrayOutputStream();
		final FastByteArrayOutputStream hostsStream = new FastByteArrayOutputStream();
		final PrintStream hosts = new PrintStream(hostsStream);
		final DataOutputStream mapDos = new DataOutputStream(mapFbaos);
		final DataOutputStream countDos = new DataOutputStream(countFbaos);
		BuildHostMap.run(br, hosts, mapDos, countDos, false, null);
		mapDos.close();
		hosts.close();
		final DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(mapFbaos.array, 0, mapFbaos.length));
		assertEquals(0, dis.readInt());
		assertEquals(1, dis.readInt());
		assertEquals(2, dis.readInt());
		assertEquals(1, dis.readInt());
		assertEquals(0, dis.readInt());
		assertEquals(0, dis.readInt());
		assertEquals(2, dis.readInt());
		assertEquals(3, dis.readInt());
		assertEquals(0, dis.available());
		dis.close();
		final BufferedReader hostsIn = new BufferedReader(new InputStreamReader(new FastByteArrayInputStream(hostsStream.array, 0, hostsStream.length)));
		assertEquals("a", hostsIn.readLine());
		assertEquals("c", hostsIn.readLine());
		assertEquals("a.b", hostsIn.readLine());
		assertEquals("159.149.130.49", hostsIn.readLine());
		assertEquals(null, hostsIn.readLine());
		hostsIn.close();
		assertArrayEquals(new int[] { 3, 2, 2, 1 }, IntIterators.unwrap(BinIO.asIntIterator(new DataInputStream(new FastByteArrayInputStream(countFbaos.array, 0, countFbaos.length)))));
	}

	@Test
	public void testSimpleLogger() throws IOException, URISyntaxException {
		final BufferedReader br = new BufferedReader(new StringReader("http://a/b\nhttp://c\nhttp://a.b/\nhttp://c/c\nhttp://a/\nhttps://a/\nhttps://a.b"));
		final FastByteArrayOutputStream mapFbaos = new FastByteArrayOutputStream();
		final FastByteArrayOutputStream countFbaos = new FastByteArrayOutputStream();
		final FastByteArrayOutputStream hostsStream = new FastByteArrayOutputStream();
		final PrintStream hosts = new PrintStream(hostsStream);
		final DataOutputStream mapDos = new DataOutputStream(mapFbaos);
		final DataOutputStream countDos = new DataOutputStream(countFbaos);
		BuildHostMap.run(br, hosts, mapDos, countDos, false, new ProgressLogger());
		mapDos.close();
		hosts.close();
		final DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(mapFbaos.array, 0, mapFbaos.length));
		assertEquals(0, dis.readInt());
		assertEquals(1, dis.readInt());
		assertEquals(2, dis.readInt());
		assertEquals(1, dis.readInt());
		assertEquals(0, dis.readInt());
		assertEquals(0, dis.readInt());
		assertEquals(2, dis.readInt());
		assertEquals(0, dis.available());
		dis.close();
		final BufferedReader hostsIn = new BufferedReader(new InputStreamReader(new FastByteArrayInputStream(hostsStream.array, 0, hostsStream.length)));
		assertEquals("a", hostsIn.readLine());
		assertEquals("c", hostsIn.readLine());
		assertEquals("a.b", hostsIn.readLine());
		assertEquals(null, hostsIn.readLine());
		hostsIn.close();
		assertArrayEquals(new int[] { 3, 2, 2 }, IntIterators.unwrap(BinIO.asIntIterator(new DataInputStream(new FastByteArrayInputStream(countFbaos.array, 0, countFbaos.length)))));
	}

	@Test
	public void testTopPrivateDomainNoLogger() throws IOException, URISyntaxException {
		final BufferedReader br = new BufferedReader(new StringReader("http://b.a.co.uk/b\nhttp://c.a.co.uk\nhttp://a.b.co.uk\nhttp://159.149.130.49/"));
		final FastByteArrayOutputStream mapFbaos = new FastByteArrayOutputStream();
		final FastByteArrayOutputStream countFbaos = new FastByteArrayOutputStream();
		final FastByteArrayOutputStream hostsStream = new FastByteArrayOutputStream();
		final PrintStream hosts = new PrintStream(hostsStream);
		final DataOutputStream mapDos = new DataOutputStream(mapFbaos);
		final DataOutputStream countDos = new DataOutputStream(countFbaos);
		BuildHostMap.run(br, hosts, mapDos, countDos, true, null);
		mapDos.close();
		hosts.close();
		final DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(mapFbaos.array, 0, mapFbaos.length));
		assertEquals(0, dis.readInt());
		assertEquals(0, dis.readInt());
		assertEquals(1, dis.readInt());
		assertEquals(2, dis.readInt());
		assertEquals(0, dis.available());
		dis.close();
		final BufferedReader hostsIn = new BufferedReader(new InputStreamReader(new FastByteArrayInputStream(hostsStream.array, 0, hostsStream.length)));
		assertEquals("a.co.uk", hostsIn.readLine());
		assertEquals("b.co.uk", hostsIn.readLine());
		assertEquals("159.149.130.49", hostsIn.readLine());
		assertEquals(null, hostsIn.readLine());
		hostsIn.close();
		assertArrayEquals(new int[] { 2, 1, 1 }, IntIterators.unwrap(BinIO.asIntIterator(new DataInputStream(new FastByteArrayInputStream(countFbaos.array, 0, countFbaos.length)))));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testMalformed() throws IOException, URISyntaxException {
		final BufferedReader br = new BufferedReader(new StringReader("http://a/b\nhttp://c\nhttp//a.b/\nhttp://c/c\nhttp://a/\nhttps://a/\nhttps://a.b"));
		final FastByteArrayOutputStream mapFbaos = new FastByteArrayOutputStream();
		final FastByteArrayOutputStream countFbaos = new FastByteArrayOutputStream();
		final FastByteArrayOutputStream hostsStream = new FastByteArrayOutputStream();
		final PrintStream hosts = new PrintStream(hostsStream);
		final DataOutputStream mapDos = new DataOutputStream(mapFbaos);
		final DataOutputStream countDos = new DataOutputStream(countFbaos);
		BuildHostMap.run(br, hosts, mapDos, countDos, false, null);
	}
}
