/*
 * Copyright (C) 2007-2021 Sebastiano Vigna
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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class BVGraphTest extends WebGraphTestCase {

	public static File storeTempGraph(final ImmutableGraph g) throws IOException, IllegalArgumentException, SecurityException {
		final File basename = File.createTempFile(BVGraphTest.class.getSimpleName(), "test");
		BVGraph.store(g, basename.toString());
		return basename;
	}

	public static File storeTempGraph(final ImmutableGraph g, final int windowSize, final int maxRefCount, final int minIntervalLength, final int flags) throws IOException, IllegalArgumentException, SecurityException {
		final File basename = File.createTempFile(BVGraphTest.class.getSimpleName(), "test");
		BVGraph.store(g, basename.toString(), windowSize, maxRefCount, minIntervalLength, 3, flags);
		return basename;
	}

	@Test
	public void testCompression() throws IOException, IllegalArgumentException, SecurityException {
		for(int n = 1; n < 8; n++)
			for(int type = 1; type < 3; type++) {
				final ImmutableGraph g = type == 0 ? ArrayListMutableGraph.newCompleteGraph(n, false).immutableView() :
					type == 1 ? ArrayListMutableGraph.newCompleteBinaryIntree(n).immutableView() :
						ArrayListMutableGraph.newCompleteBinaryOuttree(n).immutableView();
					for(int w = 0; w < 3; w++)
						for(int r = 0; r < (w == 0 ? 1 : 3); r++)
							for(int i = 0; i < 4; i++) { // Minimum interval length; 0 is NO_INTERVALS
								System.err.println("Testing type " + type + ", n=" + n + ", w=" + w + ", r=" + r + ", i=" + i + "...");
								final File basename = BVGraphTest.storeTempGraph(g, w, r, i, 0);
								final Properties properties = new Properties();
								final FileInputStream propertyFile = new FileInputStream(basename + BVGraph.PROPERTIES_EXTENSION);
								properties.load(propertyFile);
								propertyFile.close();
								assertEquals(new File(basename + BVGraph.GRAPH_EXTENSION).length(),
										(Long.parseLong(properties.getProperty("bitsforoutdegrees"))+
												Long.parseLong(properties.getProperty("bitsforreferences"))+
												Long.parseLong(properties.getProperty("bitsforblocks"))+
												Long.parseLong(properties.getProperty("bitsforintervals"))+
												Long.parseLong(properties.getProperty("bitsforresiduals")) + 7) / 8
										);

								assertEquals(g.numArcs(), Long.parseLong(properties.getProperty("copiedarcs")) + Long.parseLong(properties.getProperty("intervalisedarcs")) + Long.parseLong(properties.getProperty("residualarcs")));
								ImmutableGraph h;

								System.err.println("Testing offline...");
								h = BVGraph.loadOffline(basename.toString());
								assertGraph(h);
								assertEquals(g, h);

								// We try to force deallocation of memory-mapped graphs
								System.gc();

								System.err.println("Testing mapped...");
								h = BVGraph.loadMapped(basename.toString());
								assertGraph(h);
								assertEquals(g, h);

								System.err.println("Testing standard...");
								h = BVGraph.load(basename.toString());
								assertGraph(h);
								assertEquals(g, h);

								basename.delete();
								deleteGraph(basename);
							}
			}
	}

	@Test
	public void testLarge() throws IOException {
		ASCIIGraph asciiGraph = ASCIIGraph.loadOnce(new GZIPInputStream(getClass().getResourceAsStream("cnr-2000.graph-txt.gz")));
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = ImmutableGraph.load(path);
		assertEquals(asciiGraph, g);

		asciiGraph = ASCIIGraph.loadOnce(new GZIPInputStream(getClass().getResourceAsStream("cnr-2000.graph-txt.gz")));
		final NodeIterator nodeIterator = asciiGraph.nodeIterator();
		for(int i = 0; i < g.numNodes(); i++) {
			nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			assertEquals(d, g.outdegree(i));
			final LazyIntIterator asciiSuccessors = nodeIterator.successors(), successors = g.successors(i);
			for(int j = 0; j <= d; j++) assertEquals(asciiSuccessors.nextInt(), successors.nextInt());
		}

		deleteGraph(path);
	}

	@Test
	public void testStats() throws IOException {
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = ImmutableGraph.load(path);
		System.err.println("*******"+ g.getClass());
		// We overwrite the previously created temporary graph
		BVGraph.store(g, path + "2");
		System.err.println("*******");

		// Test statistics
		final int[] bin = new int[32];
		NodeIterator nodeIterator = g.nodeIterator();
		for(int i = 0; i < g.numNodes(); i++) {
			nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			final int[] a = nodeIterator.successorArray();
			if (d > 0) {
				for(int j = d - 1; j-- != 0;) bin[Fast.mostSignificantBit(a[j + 1] - a[j])]++;
				final int msb = Fast.mostSignificantBit(Fast.int2nat(a[0] - i));
				if (msb >= 0) bin[msb]++;
			}
		}


		final Properties properties = new Properties();
		final FileInputStream inStream = new FileInputStream(path + "2" + BVGraph.PROPERTIES_EXTENSION);
		properties.load(inStream);
		inStream.close();
		final String stats = properties.getProperty("successorexpstats");
		final String[] s = stats.split(",");
		for(int i = s.length; i-- != 0;) assertEquals(bin[i], Integer.parseInt(s[i]));

		long gap = 1, totGap = 0, tot = 0;
		double totLogGap = 0;
		for (final String element : s) {
			totGap += (gap * 2 + gap - 1) * Integer.parseInt(element);
			totLogGap += (Fast.log2(gap * 2 + gap + 1) - 1) * Integer.parseInt(element);
			tot += Integer.parseInt(element);
			gap *= 2;
		}

		assertEquals((double)totGap / (tot * 2), Double.parseDouble(properties.getProperty("successoravggap")), 1E-3);
		assertEquals(totLogGap / tot, Double.parseDouble(properties.getProperty("successoravgloggap")), 1E-3);

		assertEquals(new File(path + "2" + BVGraph.GRAPH_EXTENSION).length(),
				(Long.parseLong(properties.getProperty("bitsforoutdegrees"))+
						Long.parseLong(properties.getProperty("bitsforreferences"))+
						Long.parseLong(properties.getProperty("bitsforblocks"))+
						Long.parseLong(properties.getProperty("bitsforintervals"))+
						Long.parseLong(properties.getProperty("bitsforresiduals")) + 7) / 8
				);

		assertEquals(g.numArcs(), Long.parseLong(properties.getProperty("copiedarcs")) + Long.parseLong(properties.getProperty("intervalisedarcs")) + Long.parseLong(properties.getProperty("residualarcs")));

		// To test residual stats, we compress with no intervalisation etc.
		BVGraph.store(g, path + "2", 0, 0, 0, 3, 0);

		// Test statistics
		Arrays.fill(bin, 0);
		nodeIterator = g.nodeIterator();
		for(int i = 0; i < g.numNodes(); i++) {
			nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			final int[] a = nodeIterator.successorArray();
			if (d > 0) {
				for(int j = d - 1; j-- != 0;) bin[Fast.mostSignificantBit(a[j + 1] - a[j])]++;
				final int msb = Fast.mostSignificantBit(Fast.int2nat(a[0] - i));
				if (msb >= 0) bin[msb]++;
			}
		}

		/* TODO: write test for residuals
		stats = properties.getProperty("residualexpstats");
		s = stats.split(",");
		for(int i = s.length; i-- != 0;) assertEquals(bin[i], Integer.parseInt(s[i]));


		gap = 1;
		totGap = 0;
		tot = 0;
		totLogGap = 0;
		for(int i = 0; i < s.length; i++) {
			totGap += (gap * 2 + gap - 1) * Integer.parseInt(s[i]);
			totLogGap += (Fast.log2(gap * 2 + gap + 1) - 1) * Integer.parseInt(s[i]);
			tot += Integer.parseInt(s[i]);
			gap *= 2;
		}
		assertEquals((double)totGap / (tot * 2), Double.parseDouble(properties.getProperty("residualavggap")), 1E-3);
		assertEquals(totLogGap / tot, Double.parseDouble(properties.getProperty("residualavgloggap")), 1E-3);
		 */

		deleteGraph(path);
		deleteGraph(path + "2");
	}

	@Test
	public void testSerialization() throws IOException, ClassNotFoundException {
		final String path = getGraphPath("cnr-2000");
		final ImmutableGraph g = ImmutableGraph.load(path);
		final int d = g.outdegree(0);
		g.outdegree(10);
		final FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream();
		BinIO.storeObject(g, fbaos);
		fbaos.flush();
		final ImmutableGraph h = (ImmutableGraph) BinIO.loadObject(new FastByteArrayInputStream(fbaos.array));
		assertEquals(d, g.outdegree(0));
		assertEquals(g, h);
	}
}
