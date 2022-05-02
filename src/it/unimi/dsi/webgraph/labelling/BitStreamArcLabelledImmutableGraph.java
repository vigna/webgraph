/*
 * Copyright (C) 2007-2021 Paolo Boldi and Sebastiano Vigna
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastMultiByteArrayInputStream;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import it.unimi.dsi.webgraph.AbstractLazyIntIterator;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

/** A labelled graph storing its labels as a bit stream.
 *
 * <p>Instances of this class wrap a given {@linkplain ImmutableGraph immutable graph} and a bit stream.
 * Given a prototype {@link Label}, the bit stream is then considered as containing all labels of all arcs
 * as returned by a complete enumeration (made using {@link #nodeIterator()}). The overall graph is described
 * by a <em>label file</em> (with extension
 * <code>.labels</code>), an <em>offset file</em> (with extension
 * <code>.labeloffsets</code>) and a <em>property file</em> (with extension
 * <code>.properties</code>). The latter, not surprisingly, is a Java property file.
 *
 * <H2>The Label and Offset Files</H2>
 *
 * <P>Since the labels are stored as a bit stream, we must have some way to know where the labels
 * related to the successors of each node start.
 * This information is stored in the offset file, which contains the bit offset of the list of labels
 * of the arcs going out of each node (in particular,
 * the offset of the first list will be zero). As a commodity, the offset file contains an additional
 * offset pointing just after the last list (providing, as a side-effect, the actual bit length of the label file).
 * Each offset (except for the first one) is stored as a {@linkplain OutputBitStream#writeGamma(int) &gamma;-coded} difference from the previous offset.
 *
 * <H2>The Property File</H2>
 *
 * <p>The property file for an instance of this class must contain the following entries:
 *
 * <dl>
 * <dt>graphclass
 * <dd>the name of this class; it is necessary so that load methods in
 * {@link ImmutableGraph} can identify this class;
 * <dt>underlyinggraph
 * <dd>the basename (relative to the name of the property file, unless it is absolute) of the underlying {@link ImmutableGraph};
 * <dt>labelspec
 * <dd>a string describing a constructor call for a label class; an example is
 * <div style="margin:1em; text-align: center">
 * <code>it.unimi.dsi.webgraph.labelling.FixedWidthIntLabel(FOO,10)</code>
 * </div>
 * parameters
 * are separated by a comma, and no quoting or escaping is allowed (see {@link Label} for details
 * about string-based constructors).
 * </dl>
 *
 * <p>The {@link #load(it.unimi.dsi.webgraph.ImmutableGraph.LoadMethod, CharSequence, java.io.InputStream, ProgressLogger) load()}
 * method of this class takes care of looking at the property file, loading the underlying immutable graph,
 * and setting up either sequential or random access to the bit stream containing the labels. If
 * just sequential access is required, the offsets are not loaded into memory, and if just offline
 * access is required, bit stream is never loaded into memory.
 *
 * <h2>Saving labels</h2>
 *
 * <p>The {@link #store(ArcLabelledImmutableGraph, CharSequence, CharSequence)}
 * and {@link #store(ArcLabelledImmutableGraph, CharSequence, CharSequence, ProgressLogger)}
 * methods will save the labels of an instance of this graph as expected, that is,
 * the bitstream and its offsets will be saved with the extensions described above.
 */

public class BitStreamArcLabelledImmutableGraph extends ArcLabelledImmutableGraph {
	/** The standard extension for the labels bit stream. */
	public static final String LABELS_EXTENSION = ".labels";
	/** The standard extension for the label offsets bit stream. */
	public static final String LABEL_OFFSETS_EXTENSION = ".labeloffsets";
	/** The standard property key for a label specification. */
	public static final String LABELSPEC_PROPERTY_KEY = "labelspec";

	/** The buffer size we use for most operations. */
	private static final int STD_BUFFER_SIZE = 1024 * 1024;
	/** The underlying immutable graph. */
	public final ImmutableGraph g;
	/** A prototype label, used to deserialise labels and create copies. */
	protected final Label prototype;

	/** A byte array containing the label bit stream, or <code>null</code> for offline processing or for streams longer than {@link Integer#MAX_VALUE} bytes (see {@link #labelStream}). */
	private final byte[] byteArray;
	/** A multi-byte array input stream that replaces {@link #byteArray} for streams longer than {@link Integer#MAX_VALUE} bytes. */
	private final FastMultiByteArrayInputStream labelStream;
	/** The basename of this graph (required for offline access). */
	protected final CharSequence basename;
	/** The offset array, or <code>null</code> for sequential access. */
	protected final EliasFanoMonotoneLongBigList offset;

	/** Builds a new labelled graph using a bit stream of labels.
	 *
	 * @param basename the basename of the graph (mandatory for offline access).
	 * @param g the underlying immutable graph.
	 * @param prototype a label instance.
	 * @param byteArray a byte array containing the bit stream of labels, or <code>null</code> for offline access
	 * or large file access.
	 * @param labelStream if <code>byteArray</code> is <code>null</code>, this stream is used as the bit stream of labels.
	 * @param offset the offset array for random access, or <code>null</code>.
	 */
	protected BitStreamArcLabelledImmutableGraph(final CharSequence basename, final ImmutableGraph g, final Label prototype, final byte[] byteArray, final FastMultiByteArrayInputStream labelStream, final EliasFanoMonotoneLongBigList offset) {
		this.g = g;
		this.byteArray = byteArray;
		this.labelStream = labelStream;
		this.prototype = prototype;
		this.basename = basename;
		this.offset = offset;
	}

	@Override
	public BitStreamArcLabelledImmutableGraph copy() {
		return new BitStreamArcLabelledImmutableGraph(basename, g.copy(), prototype.copy(), byteArray, labelStream, offset);
	}

	/** Returns the label bit stream.
	 *
	 * <p>This method takes care of creating the bit stream from the right source&mdash;the byte array,
	 * the stream of multiple byte arrays or the label file itself.
	 *
	 * @return the label bit stream.
	 */
	protected InputBitStream newInputBitStream() throws FileNotFoundException {
		return byteArray != null ? new InputBitStream(byteArray) :
			labelStream != null ? new InputBitStream(new FastMultiByteArrayInputStream(labelStream)) :
				new InputBitStream(basename + LABELS_EXTENSION);
	}

	@Override
	public CharSequence basename() {
		return basename;
	}

	/** Return the actual offset of the labels of the arcs going out of a given node.
	 *
	 * @param x a node.
	 * @return the offset of the labels of the arcs going out of <code>x</code>.
	 */
	protected long offset(final int x) {
		// Without offsets, we just give up.
		return offset.getLong(x);
	}

	protected static class BitStreamLabelledArcIterator extends AbstractLazyIntIterator implements ArcLabelledNodeIterator.LabelledArcIterator {
		final protected LazyIntIterator underlyingIterator;
		final protected InputBitStream ibs;
		final protected Label label;
		final protected int from;

		public BitStreamLabelledArcIterator(final BitStreamArcLabelledImmutableGraph alg, final int x) {
			this.underlyingIterator = alg.g.successors(from = x);
			try {
				ibs = alg.newInputBitStream();
				ibs.position(alg.offset(x));
			}
			catch (final IOException e) {
				throw new RuntimeException(e);
			}
			label = alg.prototype.copy();
		}

		@Override
		public Label label() {
			return label;
		}

		@Override
		public int nextInt() {
			final int successor = underlyingIterator.nextInt();
			if (successor == -1) return -1;
			try {
				label.fromBitStream(ibs, from);
			}
			catch (final IOException e) {
				throw new RuntimeException(e);
			}
			return successor;
		}
	}

	@Override
	public ArcLabelledNodeIterator.LabelledArcIterator successors(final int x) {
		return new BitStreamLabelledArcIterator(this, x);
	}

	@Override
	public int[] successorArray(final int x) {
		return g.successorArray(x);
	}

	@Override
	public int numNodes() {
		return g.numNodes();
	}

	@Override
	public long numArcs() {
		return g.numArcs();
	}

	@Override
	public boolean randomAccess() {
		return g.randomAccess() && offset != null;
	}

	@Override
	public boolean hasCopiableIterators() {
		return false;
	}

	@Override
	public int outdegree(final int x) {
		return g.outdegree(x);
	}

	@Deprecated
	public static BitStreamArcLabelledImmutableGraph loadSequential(final CharSequence basename) throws IOException {
		return load(LoadMethod.SEQUENTIAL, basename, null);
	}

	@Deprecated
	public static BitStreamArcLabelledImmutableGraph loadSequential(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.SEQUENTIAL, basename, pl);
	}

	public static BitStreamArcLabelledImmutableGraph loadOffline(final CharSequence basename) throws IOException {
		return load(LoadMethod.OFFLINE, basename, null);
	}

	public static BitStreamArcLabelledImmutableGraph loadOffline(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.OFFLINE, basename, pl);
	}

	public static BitStreamArcLabelledImmutableGraph loadMapped(final CharSequence basename) throws IOException {
		return load(LoadMethod.MAPPED, basename, null);
	}

	public static BitStreamArcLabelledImmutableGraph loadMapped(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.MAPPED, basename, pl);
	}

	public static BitStreamArcLabelledImmutableGraph load(final CharSequence basename) throws IOException {
		return load(LoadMethod.STANDARD, basename, null);
	}

	public static BitStreamArcLabelledImmutableGraph load(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.STANDARD, basename, pl);
	}

	/** Loads a labelled graph using the given method.
	 *
	 * @param method a load method.
	 * @param basename the basename of the graph.
	 * @param pl a progress logger.
	 * @return a graph labelled using a bit stream.
	 */

	@SuppressWarnings("deprecation")
	protected static BitStreamArcLabelledImmutableGraph load(final LoadMethod method, final CharSequence basename, final ProgressLogger pl) throws IOException {
		final FileInputStream propertyFile = new FileInputStream(basename + PROPERTIES_EXTENSION);
		final Properties properties = new Properties();
		properties.load(propertyFile);
		propertyFile.close();

		if (properties.getProperty(UNDERLYINGGRAPH_PROPERTY_KEY) == null) throw new IOException("The property file for " + basename + " does not contain an underlying graph basename");
		// We resolve the underlying graph basename relatively to our basename
		String graphName = properties.getProperty(UNDERLYINGGRAPH_PROPERTY_KEY);
		// This is a workaround because absolute filenames are not correctly relativised
		if (! (new File(graphName).isAbsolute())) graphName = new File(new File(basename.toString()).getParentFile(), properties.getProperty(UNDERLYINGGRAPH_PROPERTY_KEY)).toString();

		final ImmutableGraph g;

		// A kluge to pass the offset step down to a BVGraph

		final FileInputStream graphPropertyFile = new FileInputStream(graphName + PROPERTIES_EXTENSION);
		final Properties graphProperties = new Properties();
		graphProperties.load(graphPropertyFile);
		graphPropertyFile.close();

		g = ImmutableGraph.load(method, graphName, null, pl);

		// We parse the label spec and build a prototype
		if (properties.getProperty(LABELSPEC_PROPERTY_KEY) == null) throw new IOException("The property file for " + basename + " does not contain a label specification");
		Label prototype;
		try {
			try {
				prototype = ObjectParser.fromSpec(new File(basename.toString()).getParentFile(), properties.getProperty(LABELSPEC_PROPERTY_KEY), Label.class);
			}
			catch(final NoSuchMethodException e) {
				prototype = ObjectParser.fromSpec(properties.getProperty(LABELSPEC_PROPERTY_KEY), Label.class);
			}
		}
		catch (final RuntimeException e) {
			throw new RuntimeException(e);
		}
		catch (final Exception e) {
			throw new RuntimeException(e);
		}

		byte[] byteArray = null;
		FastMultiByteArrayInputStream labelStream = null;
		EliasFanoMonotoneLongBigList offsets = null;

		if (method != LoadMethod.OFFLINE) {
			if (pl != null) {
				pl.itemsName = "bytes";
				pl.start("Loading labels...");
			}

			final FileInputStream fis = new FileInputStream(basename + LABELS_EXTENSION);
			final long size = fis.getChannel().size();
			if (size <= Integer.MAX_VALUE) byteArray = BinIO.loadBytes(basename + LABELS_EXTENSION);
			else labelStream = new FastMultiByteArrayInputStream(fis, size);

			if (pl != null) {
				pl.count = size;
				pl.done();
			}
			// We do not load offsets if only sequential access is required.
			if (method != LoadMethod.SEQUENTIAL) {
				if (pl != null) {
					pl.itemsName = "deltas";
					pl.expectedUpdates = g.numNodes() + 1;
					pl.start("Loading label offsets...");
				}
				final InputBitStream offsetStream = new InputBitStream(basename + LABEL_OFFSETS_EXTENSION);

				offsets = new EliasFanoMonotoneLongBigList(g.numNodes() + 1, size * Byte.SIZE + 1, new LongIterator() {
					private long off;
					private int i;

					@Override
					public boolean hasNext() {
						return i <= g.numNodes();
					}
					@Override
					public long nextLong() {
						i++;
						try {
							return off = offsetStream.readLongGamma() + off;
						}
						catch (final IOException e) {
							throw new RuntimeException(e);
						}
					}
				});

				offsetStream.close();
				if (pl != null) {
					pl.count = g.numNodes() + 1;
					pl.done();
					pl.logger().info("Label pointer bits per node: " + offsets.numBits() / (g.numNodes() + 1.0));
				}
			}

			fis.close();
		}

		return new BitStreamArcLabelledImmutableGraph(basename, g, prototype, byteArray, labelStream, offsets);

	}

	private final static class BitStreamArcLabelledNodeIterator extends ArcLabelledNodeIterator {
		final private NodeIterator underlyingNodeIterator;
		final private InputBitStream ibs;
		final private Label prototype;
		private Label[] label = Label.EMPTY_LABEL_ARRAY;

		public BitStreamArcLabelledNodeIterator(final int from, final ImmutableGraph g, final Label prototype, final InputBitStream ibs) {
			this.prototype = prototype;
			this.ibs = ibs;
			underlyingNodeIterator = g.nodeIterator();
			// Skip nodes up to from. This is necessary to skip labels, too.
			for(int i = from; i-- != 0;) nextInt();
		}

		private final static class BitStreamArcLabelledNodeIteratorArcIterator extends AbstractLazyIntIterator implements ArcLabelledNodeIterator.LabelledArcIterator {
			private final Label[] label;
			private final int[] successor;
			private final int outdegree;
			private int curr;

			public BitStreamArcLabelledNodeIteratorArcIterator(final int outdegree, final int[] successor, final Label[] label) {
				this.outdegree = outdegree;
				this.successor = successor;
				this.label = label;
				curr = -1;
			}

			@Override
			public Label label() {
				if (curr == -1) throw new IllegalStateException("This successor iterator is currently not valid");
				return label[curr];
			}

			@Override
			public int nextInt() {
				if (curr == outdegree - 1) return -1;
				return successor[++curr];
			}

			@Override
			public int skip(final int n) {
				final int toSkip = Math.min(n, outdegree - 1 - curr);
				curr += toSkip;
				return toSkip;
			}
		}


		@Override
		public ArcLabelledNodeIterator.LabelledArcIterator successors() {
			return new BitStreamArcLabelledNodeIteratorArcIterator(underlyingNodeIterator.outdegree(), underlyingNodeIterator.successorArray(), label);
		}

		@Override
		public int[] successorArray() {
			return underlyingNodeIterator.successorArray();
		}

		@Override
		public Label[] labelArray() {
			return label;
		}

		@Override
		public int outdegree() {
			return underlyingNodeIterator.outdegree();
		}

		@Override
		public int nextInt() {
			final int curr = underlyingNodeIterator.nextInt();
			final int d = underlyingNodeIterator.outdegree();
			// Store all labels of arcs going out of the current node
			if (label.length < d) {
				label = ObjectArrays.grow(label, d);
				for(int i = label.length; i-- != 0 && label[i] == null;) label[i] = prototype.copy();
			}
			try {
				for(int i = 0; i < d; i++) label[i].fromBitStream(ibs, curr);
			}
			catch (final IOException e) {
				throw new RuntimeException(e);
			}
			return curr;
		}

		@Override
		public boolean hasNext() {
			return underlyingNodeIterator.hasNext();
		}
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator(final int from) {
		try {
			return new BitStreamArcLabelledNodeIterator(from, g, prototype, newInputBitStream());
		}
		catch (final FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Label prototype() {
		return prototype;
	}

	public static void store(final ArcLabelledImmutableGraph graph, final CharSequence basename, final CharSequence underlyingBasename) throws IOException {
		store(graph, basename, underlyingBasename, null);
	}

	public static void store(final ArcLabelledImmutableGraph graph, final CharSequence basename, final CharSequence underlyingBasename, final ProgressLogger pl) throws IOException {
		final OutputBitStream labels = new OutputBitStream(basename + LABELS_EXTENSION, STD_BUFFER_SIZE);
		final OutputBitStream offsets = new OutputBitStream(basename + LABEL_OFFSETS_EXTENSION, STD_BUFFER_SIZE);

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = graph.numNodes();
			pl.start("Saving labels...");
		}

		final ArcLabelledNodeIterator nodeIterator = graph.nodeIterator();
		offsets.writeGamma(0);
		int curr;
		long count;
		LabelledArcIterator successors;

		while(nodeIterator.hasNext()) {
			curr = nodeIterator.nextInt();
			successors = nodeIterator.successors();
			count = 0;
			while(successors.nextInt() != -1) count += successors.label().toBitStream(labels, curr);
			offsets.writeLongGamma(count);
			if (pl != null) pl.lightUpdate();
		}

		if (pl != null) pl.done();
		labels.close();
		offsets.close();

		final PrintWriter properties = new PrintWriter(new FileOutputStream(basename + ImmutableGraph.PROPERTIES_EXTENSION));
		properties.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
		properties.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = " + underlyingBasename);
		properties.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + graph.prototype().toSpec());
		properties.close();
	}
}
