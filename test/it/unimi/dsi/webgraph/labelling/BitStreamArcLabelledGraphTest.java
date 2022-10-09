/*
 * Copyright (C) 2003-2021 Paolo Boldi and Sebastiano Vigna
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.BVGraphTest;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.LazyIntIterators;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.WebGraphTestCase;
import it.unimi.dsi.webgraph.examples.IntegerTriplesArcLabelledImmutableGraph;

public class BitStreamArcLabelledGraphTest {

	private static final int[] SIZES = { 0, 1, 2, 3, 4, 7 };
	private static final int MAX_WIDTH_FOR_FIXED = 32;
	private static final int[] WIDTHS = { -1, 0, 1, 2, 3, 8, 32, 40, 41, 63 };
	private static final int[] BATCH_SIZES = { 1, 2, 4, 5, 16 };

	public static File storeTempGraph(final ArcLabelledImmutableGraph g) throws IOException, IllegalArgumentException, SecurityException {
		final File basename = File.createTempFile(BitStreamArcLabelledGraphTest.class.getSimpleName(), "test");
		BitStreamArcLabelledImmutableGraph.store(g, basename.toString(), basename.toString() + "-underlying");
		BVGraph.store(g, basename.toString() + "-underlying");
		return basename;
	}

	private static OutputBitStream createTempBitStream(final String name) throws FileNotFoundException {
		final File f = new File(name);
		f.deleteOnExit();
		return new OutputBitStream(f.getAbsolutePath());
	}

	public String createGraphWithFixedWidthLabels(final File basename, final ImmutableGraph g, final int width) throws IllegalArgumentException, SecurityException, IOException {
		final int n = g.numNodes();
		System.err.println("Testing " + n + " nodes, width " + width+ ", basename " + basename);

		final OutputBitStream labels = createTempBitStream(basename + "-fixedlabel" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION);
		final OutputBitStream offsets = createTempBitStream(basename + "-fixedlabel" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION);
		offsets.writeGamma(0);
		for(int i = 0; i < n; i++) {
			int bits = 0;
			for(final IntIterator j = LazyIntIterators.eager(g.successors(i)); j.hasNext();) bits += labels.writeInt(i * j.nextInt() + i, width);
			offsets.writeGamma(bits);
		}
		labels.close();
		offsets.close();

		final PrintWriter pw = new PrintWriter(new FileWriter(basename + "-fixedlabel.properties"));
		pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
		pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + FixedWidthIntLabel.class.getName() + "(TEST," + width + ")");
		pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = " + basename.getName());
		pw.close();

		return basename + "-fixedlabel";
	}

	public String createGraphWithFixedWidthListLabels(final File basename, final ImmutableGraph g, final int width) throws IllegalArgumentException, SecurityException, IOException {
		final int n = g.numNodes();
		System.err.println("Testing " + n + " nodes, element width " + width+ ", basename " + basename);

		final OutputBitStream labels = createTempBitStream(basename + "-fixedlistlabel" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION);
		final OutputBitStream offsets = createTempBitStream(basename + "-fixedlistlabel" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION);
		offsets.writeGamma(0);
		for(int i = 0; i < n; i++) {
			int bits = 0;
			for(final IntIterator j = LazyIntIterators.eager(g.successors(i)); j.hasNext();) {
				final int succ = j.nextInt();
				bits += labels.writeGamma((succ + 1) * 2); // list length
				for(int k = 0; k < (succ + 1) * 2 ; k++) bits += labels.writeInt(i * k + i, width);
			}
			offsets.writeGamma(bits);
		}
		labels.close();
		offsets.close();

		final PrintWriter pw = new PrintWriter(new FileWriter(basename + "-fixedlistlabel" + ImmutableGraph.PROPERTIES_EXTENSION));
		pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
		pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + FixedWidthIntListLabel.class.getName() + "(TEST," + width + ")");
		pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = " + basename.getName());
		pw.close();

		return basename + "-fixedlistlabel";
	}

	public String createGraphWithGammaLabels(final File basename, final ImmutableGraph g) throws IllegalArgumentException, SecurityException, IOException {
		// We create a complete graph with labels
		final int n = g.numNodes();
		System.err.println("Testing " + n + " nodes, gamma coding, basename " + basename);

		final OutputBitStream labels = createTempBitStream(basename + "-gammalabel" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION);
		final OutputBitStream offsets = createTempBitStream(basename + "-gammalabel" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION);
		offsets.writeGamma(0);
		for(int i = 0; i < n; i++) {
			int bits = 0;
			for(final IntIterator j = LazyIntIterators.eager(g.successors(i)); j.hasNext();) bits += labels.writeGamma(i * j.nextInt() + i);
			offsets.writeGamma(bits);
		}
		labels.close();
		offsets.close();

		final PrintWriter pw = new PrintWriter(new FileWriter(basename + "-gammalabel" + ImmutableGraph.PROPERTIES_EXTENSION));
		pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
		pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + GammaCodedIntLabel.class.getName() + "(TEST)");
		pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = " + basename.getName());
		pw.close();

		return basename + "-gammalabel";
	}

	public void testLabels(final ArcLabelledImmutableGraph alg, final int width) {

		final int mask = (int)(width == MAX_WIDTH_FOR_FIXED ? -1 : (1L << width) - 1);

		// Sequential access, iterators
		for(final ArcLabelledNodeIterator nodeIterator = alg.nodeIterator(); nodeIterator.hasNext();) {
			final int curr = nodeIterator.nextInt();
			final ArcLabelledNodeIterator.LabelledArcIterator l = nodeIterator.successors();
			int d = nodeIterator.outdegree();
			while(d-- != 0) {
				final int succ = l.nextInt();
				if (l.label() instanceof AbstractIntLabel)
					assertEquals(curr + " -> " + succ,(curr * succ + curr) & mask, l.label().getInt());
				else {
					final int[] value = (int[]) l.label().get();
					assertEquals((succ + 1) * 2, value.length);
					for(int i = 0; i < value.length; i++) assertEquals("Successor of index " + i + " of " + curr + "(" + succ + ")", (curr * i + curr) & mask, value[i]);
				}
			}
		}

		// Sequential access, arrays
		for(final ArcLabelledNodeIterator nodeIterator = alg.nodeIterator(); nodeIterator.hasNext();) {
			final int curr = nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			final int succ[] = nodeIterator.successorArray();
			final Label[] label = nodeIterator.labelArray();
			for(int i = 0; i < d; i++) {
				if (label[i] instanceof AbstractIntLabel)
					assertEquals(curr + " -> " + succ[i], (curr * succ[i] + curr) & mask, label[i].getInt());
				else {
					final int[] value = (int[]) label[i].get();
					assertEquals((succ[i] + 1) * 2, value.length);
					for(int j = 0; j < value.length; j++) assertEquals((curr * j + curr) & mask, value[j]);
				}
			}
		}

		if (! alg.randomAccess()) return;

		// Random access, iterators
		for(int curr = 0; curr < alg.numNodes(); curr++) {
			final ArcLabelledNodeIterator.LabelledArcIterator l = alg.successors(curr);
			int d = alg.outdegree(curr);
			while(d-- != 0) {
				final int succ = l.nextInt();
				if (l.label() instanceof AbstractIntLabel)
					assertEquals(curr + " -> " + succ ,(curr * succ + curr) & mask, l.label().getInt());
				else {
					final int[] value = (int[]) l.label().get();
					assertEquals((succ + 1) * 2, value.length);
					for(int i = 0; i < value.length; i++) assertEquals((curr * i + curr) & mask, value[i]);
				}
			}
		}

		// Random access, arrays
		for(int curr = 0; curr < alg.numNodes(); curr++) {
			final int d = alg.outdegree(curr);
			final int succ[] = alg.successorArray(curr);
			final Label[] label = alg.labelArray(curr);
			for(int i = 0; i < d; i++) {
				if (label[i] instanceof AbstractIntLabel)
					assertEquals(curr + " -> " + succ[i], (curr * succ[i] + curr) & mask, label[i].getInt());
				else {
					final int[] value = (int[]) label[i].get();
					assertEquals((succ[i] + 1) * 2, value.length);
					for(int j = 0; j < value.length; j++) assertEquals((curr * j + curr) & mask, value[j]);
				}
			}
		}
	}

	@Test
	public void testLabels() throws IOException, IllegalArgumentException, SecurityException {
		for(final int n: SIZES) {
			for(int type = 0; type < 3; type++) {
				System.err.println("Testing type " + type + "...");
				final ImmutableGraph g = type == 0 ? ArrayListMutableGraph.newCompleteGraph(n, false).immutableView() :
					type == 1 ? ArrayListMutableGraph.newCompleteBinaryIntree(n).immutableView() :
						ArrayListMutableGraph.newCompleteBinaryOuttree(n).immutableView();
				final File basename = BVGraphTest.storeTempGraph(g);
				// -1 means gamma coding
				for(final int width: WIDTHS) {
					final String basenameLabel = width == -1 ?
							createGraphWithGammaLabels(basename, g) :
								width < MAX_WIDTH_FOR_FIXED ?  createGraphWithFixedWidthLabels(basename, g, width) :
									createGraphWithFixedWidthListLabels(basename, g, width - MAX_WIDTH_FOR_FIXED);

					System.err.println("Testing offline...");
					testLabels(BitStreamArcLabelledImmutableGraph.loadOffline(basenameLabel), width % MAX_WIDTH_FOR_FIXED);
					System.err.println("Testing standard...");
					testLabels(BitStreamArcLabelledImmutableGraph.load(basenameLabel), width % MAX_WIDTH_FOR_FIXED);
					System.err.println("Testing mapped...");
					testLabels(BitStreamArcLabelledImmutableGraph.loadMapped(basenameLabel), width % MAX_WIDTH_FOR_FIXED);
					WebGraphTestCase.assertGraph(BitStreamArcLabelledImmutableGraph.loadOffline(basenameLabel));

					new File(basenameLabel + ImmutableGraph.PROPERTIES_EXTENSION).delete();
					new File(basenameLabel + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION).delete();
					new File(basenameLabel + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION).delete();
				}
				basename.delete();
				BVGraphTest.deleteGraph(basename);
			}
		}
	}

	// Proceeds with the same test as before, but with a graph obtained as a union
	@Test
	public void testUnion() throws IllegalArgumentException, SecurityException, IOException {
		for(final int n: SIZES) {
			for(int type = 0; type < 3; type++) {
				System.err.println("Testing arc-labelled union type " + type + "...");
				final ImmutableGraph g = type == 0 ? ArrayListMutableGraph.newCompleteGraph(n, false).immutableView() :
					type == 1 ? ArrayListMutableGraph.newCompleteBinaryIntree(n).immutableView() :
						ArrayListMutableGraph.newCompleteBinaryOuttree(n).immutableView();

				// Now split the graph g into two (possibly non-disjoint) graphs
				final ArrayListMutableGraph g0mut = new ArrayListMutableGraph();
				final ArrayListMutableGraph g1mut = new ArrayListMutableGraph();
				g0mut.addNodes(g.numNodes()); g1mut.addNodes(g.numNodes());
				final NodeIterator nit = g.nodeIterator();
				while (nit.hasNext()) {
					final int from = nit.nextInt();
					final LazyIntIterator succ = nit.successors();
					int d = nit.outdegree();
					while (d-- != 0) {
						final int to = succ.nextInt();
						if (Math.random() < .5) g0mut.addArc(from, to);
						else if (Math.random() < .5) g1mut.addArc(from, to);
						else { g0mut.addArc(from, to); g1mut.addArc(from, to); }
					}
				}
				final ImmutableGraph g0 = g0mut.immutableView();
				final ImmutableGraph g1 = g1mut.immutableView();

				final File basename0 = BVGraphTest.storeTempGraph(g0);
				final File basename1 = BVGraphTest.storeTempGraph(g1);
				// -1 means gamma coding
				for(final int width: WIDTHS) {
					final String basenameLabel0 = width == -1 ?
							createGraphWithGammaLabels(basename0, g0) :
								width < MAX_WIDTH_FOR_FIXED ?  createGraphWithFixedWidthLabels(basename0, g0, width) :
									createGraphWithFixedWidthListLabels(basename0, g0, width - MAX_WIDTH_FOR_FIXED);
					final String basenameLabel1 = width == -1 ?
							createGraphWithGammaLabels(basename1, g1) :
								width < MAX_WIDTH_FOR_FIXED ?  createGraphWithFixedWidthLabels(basename1, g1, width) :
									createGraphWithFixedWidthListLabels(basename1, g1, width - MAX_WIDTH_FOR_FIXED);


					System.err.println("Testing arc-labelled union offline...");
					WebGraphTestCase.assertGraph(Transform.union(BitStreamArcLabelledImmutableGraph.loadOffline(basenameLabel0), BitStreamArcLabelledImmutableGraph.loadOffline(basenameLabel1)));
					testLabels((ArcLabelledImmutableGraph) Transform.union(BitStreamArcLabelledImmutableGraph.loadOffline(basenameLabel0), BitStreamArcLabelledImmutableGraph.loadOffline(basenameLabel1)), width % MAX_WIDTH_FOR_FIXED);
					System.err.println("Testing arc-labelled union standard...");
					WebGraphTestCase.assertGraph(Transform.union(BitStreamArcLabelledImmutableGraph.load(basenameLabel0), BitStreamArcLabelledImmutableGraph.load(basenameLabel1)));
					testLabels((ArcLabelledImmutableGraph) Transform.union(BitStreamArcLabelledImmutableGraph.load(basenameLabel0), BitStreamArcLabelledImmutableGraph.load(basenameLabel1)), width % MAX_WIDTH_FOR_FIXED);
					System.err.println("Testing arc-labelled union mapped...");
					WebGraphTestCase.assertGraph(Transform.union(BitStreamArcLabelledImmutableGraph.loadMapped(basenameLabel0), BitStreamArcLabelledImmutableGraph.loadMapped(basenameLabel1)));
					testLabels((ArcLabelledImmutableGraph)Transform.union(BitStreamArcLabelledImmutableGraph.loadMapped(basenameLabel0), BitStreamArcLabelledImmutableGraph.loadMapped(basenameLabel1)), width % MAX_WIDTH_FOR_FIXED);

					new File(basenameLabel0 + ImmutableGraph.PROPERTIES_EXTENSION).delete();
					new File(basenameLabel0 + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION).delete();
					new File(basenameLabel0 + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION).delete();
					new File(basenameLabel1 + ImmutableGraph.PROPERTIES_EXTENSION).delete();
					new File(basenameLabel1 + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION).delete();
					new File(basenameLabel1 + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION).delete();
				}
				basename0.delete();
				BVGraphTest.deleteGraph(basename0);
				basename1.delete();
				BVGraphTest.deleteGraph(basename1);
			}
		}
	}

	@Test
	public void moreTestUnion() {
		final IntegerTriplesArcLabelledImmutableGraph integerTriplesArcLabelledImmutableGraph0 = new IntegerTriplesArcLabelledImmutableGraph(new int[][]
	             {
					{ 0, 1, 302 }, { 0, 2, 401 }, { 1, 3, 201 }
			     });

		final IntegerTriplesArcLabelledImmutableGraph integerTriplesArcLabelledImmutableGraph1 = new IntegerTriplesArcLabelledImmutableGraph(new int[][]
	             {
					{ 0, 5, 302 }, { 0, 2, 401 }, { 1, 2, 201 }
			     });

		final ImmutableGraph union = Transform.union(integerTriplesArcLabelledImmutableGraph0, integerTriplesArcLabelledImmutableGraph1);
		WebGraphTestCase.assertGraph(union);
	}

	@Test
	public void testTransposition() throws IOException, IllegalArgumentException, SecurityException {
		for(final int n: new int[] {7}) {
				for(int type = 0; type < 3; type++) {
				System.err.println("Testing arc-labelled transposition type " + type + "...");
				final ImmutableGraph g = type == 0 ? ArrayListMutableGraph.newCompleteGraph(n, false).immutableView() :
					type == 1 ? ArrayListMutableGraph.newCompleteBinaryIntree(n).immutableView() :
						ArrayListMutableGraph.newCompleteBinaryOuttree(n).immutableView();
				final File basename = BVGraphTest.storeTempGraph(g);
				// -1 means gamma coding
				for(final int width: WIDTHS) {
					final String basenameLabel;

					if (width == -1) basenameLabel = createGraphWithGammaLabels(basename, g);
					else if (width < MAX_WIDTH_FOR_FIXED) basenameLabel = createGraphWithFixedWidthLabels(basename, g, width);
					else basenameLabel = createGraphWithFixedWidthListLabels(basename, g, width - MAX_WIDTH_FOR_FIXED);

					for (final int batchSize: BATCH_SIZES) {
						final ArcLabelledImmutableGraph gt = Transform.transposeOffline(BitStreamArcLabelledImmutableGraph.loadOffline(basenameLabel),
								batchSize, new File(System.getProperty("java.io.tmpdir")), null);

						final ArcLabelledImmutableGraph gtt = Transform.transposeOffline(gt,
								batchSize, new File(System.getProperty("java.io.tmpdir")), null);
						System.err.println("Testing with batch size " + batchSize + "...");
						testLabels(gtt, width % MAX_WIDTH_FOR_FIXED);
					}

					new File(basenameLabel + ImmutableGraph.PROPERTIES_EXTENSION).delete();
					new File(basenameLabel + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION).delete();
					new File(basenameLabel + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION).delete();
				}
				basename.delete();
				BVGraphTest.deleteGraph(basename);
			}
		}
	}

}
