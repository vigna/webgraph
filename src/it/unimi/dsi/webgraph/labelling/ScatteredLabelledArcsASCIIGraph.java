/*
 * Copyright (C) 2011-2023 Sebastiano Vigna
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

import com.martiansoftware.jsap.*;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.longs.Long2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.webgraph.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static it.unimi.dsi.webgraph.Transform.processTransposeBatch;
import static it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX;

/**
 * An {@link ArcLabelledImmutableGraph} that corresponds to a labelled graph stored as a scattered list of arcs.
 *
 * <p>
 * A <em>scattered list of arcs</em> describes a graph in a fairly loose way. Each line contains a
 * labelled arc specified as two node identifiers and a label separated by whitespace (but we suggest exactly one TAB
 * character).
 *
 * <p>
 * In the <em>standard</em> description, node identifiers can be in the range
 * [-2<sup>63</sup>..2<sup>63</sup>): they will be remapped in a compact identifier space by
 * assigning to each newly seen identifier a new node number. The list of identifiers in order of
 * appearance is available in {@link #ids}. Lines can be empty, or comments starting with
 * <code>#</code>. Characters following the target will be discarded with a warning.
 * Similarly, the labels can be in the range [-2<sup>63</sup>..2<sup>63</sup>) and will be saved
 * as-is in gamma coding, in case of duplicates only the last new label will be considered,
 * this behaviour can be changed by providing more parameters.
 *
 * <p>
 * <strong>Warning:</strong> Lines not conforming the above specification will cause an error to be
 * logged, but will be otherwise ignored.
 *
 * <p>
 * Alternatively, you can
 * {@linkplain #ScatteredLabelledArcsASCIIGraph(InputStream, Object2LongFunction, Charset, int, boolean)
 * provide} an {@link Object2LongFunction Object2LongFunction&lt;String>} with default return value
 * -1 that will be used to map identifiers to node numbers, along with a {@link Charset} to parse
 * lines and the number of nodes of the graph (which must be a strict upper bound for the largest
 * value returned by the function). Note that in principle an {@link Object2IntFunction} would be
 * sufficient, but we want to make easier using functions from Sux4J such as {@link GOV3Function}.
 *
 * <p>
 * Additionally, the resulting graph can be symmetrized, and its loops be removed, using
 * {@linkplain #ScatteredLabelledArcsASCIIGraph(InputStream, boolean, boolean, int, File, ProgressLogger)
 * suitable constructor options}.
 *
 * <p>
 * You can provide {@linkplain #ScatteredLabelledArcsASCIIGraph(InputStream, labelPrototype, labelMapping, labelMergeStrategy)
 * suitable constructor options} a {@link Label} as prototype, a {@link LabelMapping} as a way to
 * convert the written labels to object of the prototype's type and a {@link LabelMergeStrategy}
 * to handle the case of identical arcs with different labels.
 *
 * <p>
 * This class has no load method, and its main method converts a scattered-arcs representation
 * directly into a {@link BVGraph}.
 *
 * <h2>Using {@link ScatteredLabelledArcsASCIIGraph} to convert your data</h2>
 *
 * <p>
 * A simple (albeit rather inefficient) way to import data into WebGraph is using ASCII graphs
 * specified by scattered arcs. Suppose you create the following file, named
 * <code>example.arcs</code>:
 *
 * <pre>
 *  # My graph
 *  -1 15 100
 *  15 2 200
 *  2 -1 300 This will cause a warning to be logged
 *  OOPS! (This will cause an error to be logged)
 *  -1 2 400
 * </pre>
 *
 * Then, the command
 *
 * <pre>
 *  java it.unimi.dsi.webgraph.ScatteredLabelledArcsASCIIGraph example &lt; example.arcs
 * </pre>
 *
 * will produce a compressed labelled graph in {@link it.unimi.dsi.webgraph.BVGraph} format.
 * The underlying graph will be saved with basename <code>example-underlying</code>.
 * The file <code>example.ids</code> will contain the list of longs -1, 15, 2.
 * The node with identifer -1 will be the node 0 in the output graph, the node with identifier
 * 15 will be node 1, and the node with identifier 2 will be node 2. The graph <code>example</code>
 * will thus have three nodes and four arcs (viz., &lt;0,1&gt;, &lt;0,2&gt;, &lt;1,2&gt; and
 * &lt;2,0&gt;). The labels will be saved as <code>example.labels</code> in the order of visit
 * of the arcs, the offset <code>example.labeloffsets</code> relay the offset of each specific label,
 * because in general labels are not written in a fixed number of bits.
 *
 * <h2>Memory requirements</h2>
 *
 * <p>
 * To convert node identifiers to node numbers, instances of this class use a custom map that in the
 * worst case will require
 * 19.5&times;2<sup>&lceil;log(4<var>n</var>/3)&rceil;</sup>&nbsp;&le;&nbsp;52<var>n</var> bytes,
 * where <var>n</var> is the number of distinct identifiers. Storing batches of arcs in memory
 * requires 8 bytes per arc.
 */

public class ScatteredLabelledArcsASCIIGraph extends ImmutableSequentialGraph {
	/**
	 * The default batch size.
	 */
	public static final int DEFAULT_BATCH_SIZE = 1000000;
	/**
	 * The default label prototype.
	 */
	public static final Label DEFAULT_LABEL_PROTOTYPE = new GammaCodedIntLabel("FOO");
	/**
	 * The default label mapping function.
	 */
	public static final LabelMapping DEFAULT_LABEL_MAPPING = (label, st) -> ((GammaCodedIntLabel) label).value = Integer.parseInt((String) st);

	private static final Logger LOGGER = LoggerFactory.getLogger(ScatteredLabelledArcsASCIIGraph.class);
	private final static boolean DEBUG = false;

	/**
	 * The extension of the identifier file (a binary list of longs).
	 */
	private static final String IDS_EXTENSION = ".ids";
	/**
	 * The labelled batch graph used to return node iterators.
	 */
	private final Transform.ArcLabelledBatchGraph arcLabelledBatchGraph;
	/**
	 * The list of identifiers in order of appearance.
	 */
	public long[] ids;

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is) throws IOException {
		this(is, DEFAULT_LABEL_PROTOTYPE, DEFAULT_LABEL_MAPPING, null, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param labelPrototype an example of the labels contained in the graph.
	 * @param labelMapping a function mapping string into the label defined by the prototype.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Label labelPrototype, final LabelMapping labelMapping) throws IOException {
		this(is, labelPrototype, labelMapping, null, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param labelPrototype an example of the labels contained in the graph.
	 * @param labelMapping a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy) throws IOException {
		this(is, labelPrototype, labelMapping, labelMergeStrategy, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param labelPrototype an example of the labels contained in the graph.
	 * @param labelMapping a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize) throws IOException {
		this(is, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param labelPrototype an example of the labels contained in the graph.
	 * @param labelMapping a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops) throws IOException {
		this(is, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, noLoops, DEFAULT_BATCH_SIZE);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param labelPrototype an example of the labels contained in the graph.
	 * @param labelMapping a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by
	 * 		this method.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops, final int batchSize) throws IOException {
		this(is, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, noLoops, batchSize, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param labelPrototype an example of the labels contained in the graph.
	 * @param labelMapping a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by
	 * 		this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *        {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir) throws IOException {
		this(is, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, noLoops, batchSize, tempDir, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 * @param batchSize  the number of integers in a batch; two arrays of integers of this size will be allocated by
	 *                   this method.
	 * @param tempDir    a temporary directory for the batches, or <code>null</code> for
	 *                   {@link File#createTempFile(String, String)}'s choice.
	 * @param pl         a progress logger, or <code>null</code>.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		this(is, null, null, -1, DEFAULT_LABEL_PROTOTYPE, DEFAULT_LABEL_MAPPING, null, symmetrize, noLoops, batchSize, tempDir, pl);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param labelPrototype an example of the labels contained in the graph.
	 * @param labelMapping a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by
	 * 		this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *        {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		this(is, null, null, -1, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, noLoops, batchSize, tempDir, pl);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is                 an input stream containing a scattered list of arcs.
	 * @param function           an explicitly provided function from string representing nodes to node numbers, or
	 *                           <code>null</code> for the standard behaviour.
	 * @param charset            a character set that will be used to read the identifiers passed to <code>function</code>, or
	 *                           <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n                  the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param labelPrototype     an example of the labels contained in the graph.
	 * @param labelMapping       a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy) throws IOException {
		this(is, function, charset, n, labelPrototype, labelMapping, labelMergeStrategy, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is                 an input stream containing a scattered list of arcs.
	 * @param function           an explicitly provided function from string representing nodes to node numbers, or
	 *                           <code>null</code> for the standard behaviour.
	 * @param charset            a character set that will be used to read the identifiers passed to <code>function</code>, or
	 *                           <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n                  the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize         the new graph will be forced to be symmetric.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final boolean symmetrize) throws IOException {
		this(is, function, charset, n, DEFAULT_LABEL_PROTOTYPE, DEFAULT_LABEL_MAPPING, null, symmetrize, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is                 an input stream containing a scattered list of arcs.
	 * @param function           an explicitly provided function from string representing nodes to node numbers, or
	 *                           <code>null</code> for the standard behaviour.
	 * @param charset            a character set that will be used to read the identifiers passed to <code>function</code>, or
	 *                           <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n                  the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param labelPrototype     an example of the labels contained in the graph.
	 * @param labelMapping       a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize         the new graph will be forced to be symmetric.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize) throws IOException {
		this(is, function, charset, n, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is                 an input stream containing a scattered list of arcs.
	 * @param function           an explicitly provided function from string representing nodes to node numbers, or
	 *                           <code>null</code> for the standard behaviour.
	 * @param charset            a character set that will be used to read the identifiers passed to <code>function</code>, or
	 *                           <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n                  the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param labelPrototype     an example of the labels contained in the graph.
	 * @param labelMapping       a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize         the new graph will be forced to be symmetric.
	 * @param noLoops            the new graph will have no loops.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops) throws IOException {
		this(is, function, charset, n, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, noLoops, DEFAULT_BATCH_SIZE);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is                 an input stream containing a scattered list of arcs.
	 * @param function           an explicitly provided function from string representing nodes to node numbers, or
	 *                           <code>null</code> for the standard behaviour.
	 * @param charset            a character set that will be used to read the identifiers passed to <code>function</code>, or
	 *                           <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n                  the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param labelPrototype     an example of the labels contained in the graph.
	 * @param labelMapping       a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize         the new graph will be forced to be symmetric.
	 * @param noLoops            the new graph will have no loops.
	 * @param batchSize          the number of integers in a batch; two arrays of integers of this size will be allocated by
	 *                           this method.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops, final int batchSize) throws IOException {
		this(is, function, charset, n, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, noLoops, batchSize, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is                 an input stream containing a scattered list of arcs.
	 * @param function           an explicitly provided function from string representing nodes to node numbers, or
	 *                           <code>null</code> for the standard behaviour.
	 * @param charset            a character set that will be used to read the identifiers passed to <code>function</code>, or
	 *                           <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n                  the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param labelPrototype     an example of the labels contained in the graph.
	 * @param labelMapping       a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize         the new graph will be forced to be symmetric.
	 * @param noLoops            the new graph will have no loops.
	 * @param batchSize          the number of integers in a batch; two arrays of integers of this size will be allocated by
	 *                           this method.
	 * @param tempDir            a temporary directory for the batches, or <code>null</code> for
	 *                           {@link File#createTempFile(String, String)}'s choice.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir) throws IOException {
		this(is, function, charset, n, labelPrototype, labelMapping, labelMergeStrategy, symmetrize, noLoops, batchSize, tempDir, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is                 an input stream containing a scattered list of arcs.
	 * @param function           an explicitly provided function from string representing nodes to node numbers, or
	 *                           <code>null</code> for the standard behaviour.
	 * @param charset            a character set that will be used to read the identifiers passed to <code>function</code>, or
	 *                           <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n                  the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param labelPrototype     an example of the labels contained in the graph.
	 * @param labelMapping       a function mapping string into the label defined by the prototype.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize         the new graph will be forced to be symmetric.
	 * @param noLoops            the new graph will have no loops.
	 * @param batchSize          the number of integers in a batch; two arrays of integers of this size will be allocated by
	 *                           this method.
	 * @param tempDir            a temporary directory for the batches, or <code>null</code> for
	 *                           {@link File#createTempFile(String, String)}'s choice.
	 * @param pl                 a progress logger, or <code>null</code>.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, Charset charset, final int n, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		@SuppressWarnings("resource")
		final FastBufferedInputStream fbis = new FastBufferedInputStream(is);
		ScatteredArcsASCIIGraph.Id2NodeMap map = new ScatteredArcsASCIIGraph.Id2NodeMap();

		int numNodes = -1;
		if (charset == null) charset = StandardCharsets.ISO_8859_1;

		int j;
		int[] source = new int[batchSize], target = new int[batchSize];
		long[] labelStart = new long[batchSize];
		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream(fbos);
		final ObjectArrayList<File> batches = new ObjectArrayList<>(), labelBatches = new ObjectArrayList<>();
		final Label prototype = labelPrototype.copy();

		if (pl != null) {
			pl.itemsName = "labelled arcs";
			pl.start("Creating sorted batches...");
		}

		j = 0;
		long pairs = 0; // Number of pairs
		byte[] array = new byte[1024];
		for (long line = 1; ; line++) {
			int start = 0, len;
			while ((len = fbis.readLine(array, start, array.length - start, FastBufferedInputStream.ALL_TERMINATORS)) == array.length - start) {
				start += len;
				array = ByteArrays.grow(array, array.length + 1);
			}

			if (len == -1) break; // EOF

			final int lineLength = start + len;

			if (DEBUG)
				System.err.println("Reading line " + line + "... (" + new String(array, 0, lineLength, charset) + ")");

			// Skip whitespace at the start of the line.
			int offset = 0;
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				if (DEBUG) System.err.println("Skipping line " + line + "...");
				continue; // Whitespace line
			}

			if (array[0] == '#') continue;

			// Scan source id.
			start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			int s;

			if (function == null) {
				final long sl;
				try {
					sl = getLong(array, start, offset - start);
				} catch (final RuntimeException e) {
					// Discard up to the end of line
					LOGGER.error("Error at line " + line + ": " + e.getMessage());
					continue;
				}

				s = map.getNode(sl);

				if (DEBUG) System.err.println("Parsed source at line " + line + ": " + sl + " => " + s);
			} else {
				final String ss = new String(array, start, offset - start, charset);
				final long sl = function.getLong(ss);
				if (sl == -1) {
					LOGGER.warn("Unknown source identifier " + ss + " at line " + line);
					continue;
				}
				if (sl < 0 || sl >= n)
					throw new IllegalArgumentException("Source node number out of range for node " + ss + ": " + sl);
				s = (int)sl;
				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + ss + " => " + s);
			}

			// Skip whitespace between identifiers.
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				LOGGER.error("Error at line " + line + ": no target");
				continue;
			}

			// Scan target id.
			start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			int t;

			if (function == null) {
				final long tl;
				try {
					tl = getLong(array, start, offset - start);
				} catch (final RuntimeException e) {
					// Discard up to the end of line
					LOGGER.error("Error at line " + line + ": " + e.getMessage());
					continue;
				}

				t = map.getNode(tl);

				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + tl + " => " + t);
			} else {
				final String ts = new String(array, start, offset - start, charset);
				final long tl = function.getLong(ts);
				if (tl == -1) {
					LOGGER.warn("Unknown target identifier " + ts + " at line " + line);
					continue;
				}

				if (tl < 0 || tl >= n)
					throw new IllegalArgumentException("Target node number out of range for node " + ts + ": " + tl);
				t = (int)tl;
				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + ts + " => " + t);
			}

			// Skip whitespace between identifiers.
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				LOGGER.error("Error at line " + line + ": no label");
				continue;
			}

			// Scan label.
			start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			final String ls = new String(array, start, offset - start, charset);

			// Insert current value into the prototype label.
			labelMapping.apply(prototype, ls);
			if (DEBUG) System.err.println("Parsed label at line " + line + ": " + ls + " => " + prototype.get());

			// Skip whitespace after label.
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset < lineLength) LOGGER.warn("Trailing characters ignored at line " + line);

			if (DEBUG)
				System.err.println("Parsed labelled arc at line " + line + ": " + s + " -> " + t + " (" + prototype.get() + ")");

			if (s != t || !noLoops) {
				source[j] = s;
				target[j] = t;
				labelStart[j] = obs.writtenBits();
				prototype.toBitStream(obs, s);
				j++;

				if (j == batchSize) {
					obs.flush();
					pairs += processTransposeBatch(batchSize, source, target, labelStart, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, labelMergeStrategy);
					fbos = new FastByteArrayOutputStream();
					obs = new OutputBitStream(fbos);
					j = 0;
				}

				if (symmetrize && s != t) {
					source[j] = t;
					target[j] = s;
					labelStart[j] = obs.writtenBits();
					prototype.toBitStream(obs, t);
					j++;

					if (j == batchSize) {
						obs.flush();
						pairs += processTransposeBatch(batchSize, source, target, labelStart, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, labelMergeStrategy);
						fbos = new FastByteArrayOutputStream();
						obs = new OutputBitStream(fbos);
						j = 0;
					}
				}

				if (pl != null) pl.lightUpdate();
			}
		}

		if (j != 0) {
			obs.flush();
			pairs += processTransposeBatch(j, source, target, labelStart, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, labelMergeStrategy);
		}

		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}

		numNodes = function == null ? (int)map.size() : function.size();
		source = null;
		target = null;
		labelStart = null;

		if (function == null) {
			ids = map.getIds(tempDir);
		}

		this.arcLabelledBatchGraph = new Transform.ArcLabelledBatchGraph(function == null ? numNodes : n, pairs, batches, labelBatches, prototype, labelMergeStrategy);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param arcs               an iterator returning the arcs as two-element arrays.
	 * @param function           a function to map the long ids passed in <code>arcs</code> to int nodes.
	 * @param n                  the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param arcLabels          a homogeneous iterator returning the labels in the same order as the arcs.
	 * @param labelMergeStrategy a merge strategy to apply when encountering duplicate arcs with different labels.
	 * @param symmetrize         the new graph will be forced to be symmetric.
	 * @param noLoops            the new graph will have no loops.
	 * @param batchSize          the number of integers in a batch; two arrays of integers of this size will be allocated by
	 *                           this method.
	 * @param tempDir            a temporary directory for the batches, or <code>null</code> for
	 *                           {@link File#createTempFile(String, String)}'s choice.
	 * @param pl                 a progress logger, or <code>null</code>.
	 */
	public ScatteredLabelledArcsASCIIGraph(final Iterator<long[]> arcs, final Long2IntFunction function, final int n, final Iterator<Label> arcLabels, LabelMergeStrategy labelMergeStrategy, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		ScatteredArcsASCIIGraph.Id2NodeMap map = new ScatteredArcsASCIIGraph.Id2NodeMap();

		int numNodes = -1;

		int j;
		int[] source = new int[batchSize], target = new int[batchSize];
		long[] labelStart = new long[batchSize];
		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream(fbos);
		Label prototype = null;
		final ObjectArrayList<File> batches = new ObjectArrayList<>(), labelBatches = new ObjectArrayList<>();

		if (pl != null) {
			pl.itemsName = "labelled arcs";
			pl.start("Creating sorted batches...");
		}

		j = 0;
		long pairs = 0; // Number of pairs
		while (arcs.hasNext()) {
			final long[] arc = arcs.next();

			final long sl = arc[0];
			int s = function == null ? map.getNode(sl) : function.get(sl);

			final long tl = arc[1];
			int t = function == null ? map.getNode(tl) : function.get(tl);

			if (!arcLabels.hasNext()) {
				throw new IllegalArgumentException("Not enough labels");
			}

			Label l = arcLabels.next();
			prototype = prototype == null ? l : prototype;

			if (s != t || !noLoops) {
				source[j] = s;
				target[j] = t;
				labelStart[j] = obs.writtenBits();
				l.toBitStream(obs, s);
				j++;

				if (j == batchSize) {
					obs.flush();
					pairs += processTransposeBatch(batchSize, source, target, labelStart, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, labelMergeStrategy);
					fbos = new FastByteArrayOutputStream();
					obs = new OutputBitStream(fbos);
					j = 0;
				}

				if (symmetrize && s != t) {
					source[j] = t;
					target[j] = s;
					labelStart[j] = obs.writtenBits();
					l.toBitStream(obs, t);
					j++;

					if (j == batchSize) {
						obs.flush();
						pairs += processTransposeBatch(batchSize, source, target, labelStart, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, labelMergeStrategy);
						fbos = new FastByteArrayOutputStream();
						obs = new OutputBitStream(fbos);
						j = 0;
					}
				}

				if (pl != null) pl.lightUpdate();
			}
		}

		if (arcLabels.hasNext()) {
			LOGGER.warn("Too many labels and not enough arcs, some label were left without arcs");
		}

		if (j != 0) {
			obs.flush();
			pairs += processTransposeBatch(j, source, target, labelStart, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, labelMergeStrategy);
		}

		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}

		numNodes = function == null ? (int)map.size() : function.size();
		source = null;
		target = null;
		labelStart = null;

		if (function == null) {
			ids = map.getIds(tempDir);
		}

		this.arcLabelledBatchGraph = new Transform.ArcLabelledBatchGraph(function == null ? numNodes : n, pairs, batches, labelBatches, prototype, labelMergeStrategy);
	}

	protected static void logBatches(final ObjectArrayList<File> batches, final long pairs, final ProgressLogger pl) {
		long length = 0;
		for (final File f : batches) length += f.length();
		pl.logger().info("Created " + batches.size() + " batches using " + Util.format((double)Byte.SIZE * length / pairs) + " bits/arc.");
	}

	private static long getLong(final byte[] array, int offset, int length) {
		if (length == 0) throw new NumberFormatException("Empty number");
		int sign = 1;
		if (array[offset] == '-') {
			sign = -1;
			offset++;
			length--;
		}

		long value = 0;
		for (int i = 0; i < length; i++) {
			final byte digit = array[offset + i];
			if (digit < '0' || digit > '9') throw new NumberFormatException("Not a digit: " + (char)digit);
			value *= 10;
			value += digit - '0';
		}

		return sign * value;
	}

	@Override
	public int numNodes() {
		if (this.arcLabelledBatchGraph == null)
			throw new UnsupportedOperationException("The number of nodes is unknown (you need to exhaust the input)");
		return this.arcLabelledBatchGraph.numNodes();
	}

	@Override
	public long numArcs() {
		if (this.arcLabelledBatchGraph == null)
			throw new UnsupportedOperationException("The number of arcs is unknown (you need to exhaust the input)");
		return this.arcLabelledBatchGraph.numArcs();
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator() {
		return this.arcLabelledBatchGraph.nodeIterator(0);
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator(final int from) {
		return this.arcLabelledBatchGraph.nodeIterator(from);
	}

	@Override
	public boolean hasCopiableIterators() {
		return this.arcLabelledBatchGraph.hasCopiableIterators();
	}

	@Override
	public ScatteredLabelledArcsASCIIGraph copy() {
		return this;
	}

	@Override
	public String toString() {
		final MutableString ms = new MutableString();
		ArcLabelledNodeIterator nodeIterator = nodeIterator();
		ms.append("Nodes: " + numNodes() + "\nArcs: " + numArcs() + "\n");
		while (nodeIterator.hasNext()) {
			int node = nodeIterator.nextInt();
			ArcLabelledNodeIterator.LabelledArcIterator successors = nodeIterator.successors();
			Label[] labels = nodeIterator.labelArray();
			ms.append("Successors of " + node + " (degree " + nodeIterator.outdegree() + "):");
			for (int k = 0; k < nodeIterator.outdegree(); k++) {
				ms.append(" " + successors.nextInt() + " (" + labels[k].get() + ")");
			}
			ms.append("\n");

		}
		return ms.toString();
	}

	public interface LabelMapping {
		void apply(Label prototype, CharSequence representation);
	}

	@SuppressWarnings("unchecked")
	public static void main(final String[] args) throws IllegalArgumentException, SecurityException, IOException, JSAPException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(ScatteredLabelledArcsASCIIGraph.class.getName(),
				"Converts a scattered list of labelled arcs from standard input into a BVGraph. The list of " +
						"identifiers in order of appearance will be saved with extension \"" + IDS_EXTENSION + "\", " +
						"unless a translation function has been specified. The labels must be written after each " +
						"arc, will be interpreted as integers and stored in gamma coding unless a mapping function " +
						"has been specified, alongside a label prototype. The underlying representation of the labels " +
						"will be saved as the given basename with the \"" + UNDERLYINGGRAPH_SUFFIX + "\" suffix.",
				new Parameter[]{
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new FlaggedOption("batchSize", JSAP.INTSIZE_PARSER, Integer.toString(DEFAULT_BATCH_SIZE), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in arcs."),
						new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary batch files."),
						new Switch("symmetrize", 'S', "symmetrize", "Force the output graph to be symmetric."),
						new Switch("noLoops", 'L', "no-loops", "Remove loops from the output graph."),
						new Switch("zipped", 'z', "zipped", "The string list is compressed in gzip format."),
						new FlaggedOption("function", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "function", "A serialised function from strings to longs that will be used to translate identifiers to node numbers."),
						new FlaggedOption("charset", JSAP.STRING_PARSER, "ISO-8859-1", JSAP.NOT_REQUIRED, 'C', "charset", "The charset used to read the list of arcs."),
						new FlaggedOption("n", JSAP.INTSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n', "n", "The number of nodes of the graph (only if you specified a function that does not return the size of the key set, or if you want to override that size)."),
						new FlaggedOption("comp", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag (may be specified several times).").setAllowMultipleDeclarations(true),
						new FlaggedOption("windowSize", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_WINDOW_SIZE), JSAP.NOT_REQUIRED, 'w', "window-size", "Reference window size (0 to disable)."),
						new FlaggedOption("maxRefCount", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_MAX_REF_COUNT), JSAP.NOT_REQUIRED, 'm', "max-ref-count", "Maximum number of backward references (-1 for ∞)."),
						new FlaggedOption("minIntervalLength", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_MIN_INTERVAL_LENGTH), JSAP.NOT_REQUIRED, 'i', "min-interval-length", "Minimum length of an interval (0 to disable)."),
						new FlaggedOption("zetaK", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_ZETA_K), JSAP.NOT_REQUIRED, 'k', "zeta-k", "The k parameter for zeta-k codes."),
						new FlaggedOption("labelPrototype", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "label-prototype", "The prototype of the labels."),
						new FlaggedOption("labelMapping", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'a', "label-mapping", "A serialised function from strings to the given label prototype that will be used to translate label strings to label objects."),
						new FlaggedOption("labelMergeStrategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'e', "label-merge-strategy", "A serialized LabelMergeStrategy object defining how to tread duplicated arcs with the same label."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the output graph."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) {
			System.exit(1);
		}

		String basename = jsapResult.getString("basename");

		int flags = 0;
		for(final String compressionFlag: jsapResult.getStringArray("comp"))
			try {
				flags |= BVGraph.class.getField(compressionFlag).getInt(BVGraph.class);
			}
			catch (final Exception notFound) {
				throw new JSAPException("Compression method " + compressionFlag + " unknown.");
			}

		final int windowSize = jsapResult.getInt("windowSize");
		final int zetaK = jsapResult.getInt("zetaK");
		int maxRefCount = jsapResult.getInt("maxRefCount");
		if (maxRefCount == -1) maxRefCount = Integer.MAX_VALUE;
		final int minIntervalLength = jsapResult.getInt("minIntervalLength");

		Object2LongFunction<String> function = null;
		Charset charset = null;
		int n = -1;

		if (jsapResult.userSpecified("function")) {
			function = (Object2LongFunction<String>) BinIO.loadObject(jsapResult.getString("function"));
			charset = Charset.forName(jsapResult.getString("charset"));
			if (function.size() == -1) {
				if (!jsapResult.userSpecified("n")) {
					throw new IllegalArgumentException("You must specify a graph size if you specify a translation function that does not return the size of the key set.");
				}
				n = jsapResult.getInt("n");
			} else {
				n = function.size();
			}
		}

		if (jsapResult.userSpecified("labelPrototype") != jsapResult.userSpecified("labelMapping")) {
			throw new IllegalArgumentException("You must specify either both a label prototype and a label mapping or none.");
		}

		Label labelPrototype = DEFAULT_LABEL_PROTOTYPE;
		if (jsapResult.userSpecified("labelPrototype")) {
			labelPrototype = (Label) BinIO.loadObject(jsapResult.getString("labelPrototype"));
		}

		LabelMapping labelMapping = DEFAULT_LABEL_MAPPING;
		if (jsapResult.userSpecified("labelMapping")) {
			labelMapping = (LabelMapping) BinIO.loadObject(jsapResult.getString("labelMapping"));
		}

		LabelMergeStrategy labelMergeStrategy = null;
		if (jsapResult.userSpecified("labelMergeStrategy")) {
			labelMergeStrategy = (LabelMergeStrategy) BinIO.loadObject(jsapResult.getString("labelMergeStrategy"));
		}

		File tempDir = null;
		if (jsapResult.userSpecified("tempDir")) {
			tempDir = new File(jsapResult.getString("tempDir"));
		}

		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		final boolean zipped = jsapResult.getBoolean("zipped");
		final InputStream inStream = (zipped ? new GZIPInputStream(System.in) : System.in);

		final ScatteredLabelledArcsASCIIGraph graph = new ScatteredLabelledArcsASCIIGraph(
				inStream, function, charset, n, labelPrototype, labelMapping, labelMergeStrategy,
				jsapResult.userSpecified("symmetrize"), jsapResult.userSpecified("noLoops"),
				jsapResult.getInt("batchSize"), tempDir, pl);
		BVGraph.storeLabelled(graph.arcLabelledBatchGraph, basename, basename + UNDERLYINGGRAPH_SUFFIX,
				windowSize, maxRefCount, minIntervalLength, zetaK, flags, pl);

		if (function == null) {
			BinIO.storeLongs(graph.ids, basename + IDS_EXTENSION);
		}
	}
}

