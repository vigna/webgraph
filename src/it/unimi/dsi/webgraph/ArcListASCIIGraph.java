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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StreamTokenizer;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.logging.ProgressLogger;


/** An {@link ImmutableGraph} that corresponds to graphs stored in a human-readable
 *  ASCII format were each line contains an arc.
 *
 *  <p>The file format is very simple: each line contains an arc specified as two nodes
 *  separated by whitespace (but we suggest exactly one TAB character). Sources must be in increasing
 *  order, but targets can be in any order. The {@linkplain #ArcListASCIIGraph(InputStream, int) constructor}
 *  provides an additional parameter, called <em>shift</em>, which will be added to
 *  all node indices. The default is 0, but for lists that number nodes starting from 1
 *  it can be set to -1. Actually, the class {@link ShiftedByOneArcListASCIIGraph} can be used in place
 *  of this class for setting the shift to -1 without specifying additional parameters.
 *
 *  <P>Contrarily to other classes, the load methods of this class <strong>do not always return instances of this class</strong>.
 *  In particular, {@link #loadOnce(InputStream)} <em>will</em> return an instance of this class for
 *  read-once access. The instance will not provide offline or random access, but read-once access will be backed by
 *  the original input stream and only the successors of a single node will be loaded in core memory at any time.
 *
 *  <p>The {@link #load(CharSequence)} method, on the other hand, will return an instance of
 *  {@link it.unimi.dsi.webgraph.ArrayListMutableGraph} built by copying an offline instance of this class.
 *
 *  <h2>Using {@link ArcListASCIIGraph} to convert your data</h2>
 *
 *  <p>A simple (albeit rather inefficient) way to import data into WebGraph is using ASCII graphs specified by arc lists. Suppose you
 *  create the following file, named <code>example.arcs</code>:
 *  <pre>
 *  0 1
 *  1 2
 *  2 1
 *  </pre>
 *  Then, the command
 *  <pre>
 *  java it.unimi.dsi.webgraph.BVGraph -g ArcListASCIIGraph example.arcs bvexample
 *  </pre>
 *  will produce a compressed graph in {@link it.unimi.dsi.webgraph.BVGraph} format
 *  with basename <code>bvexample</code>. Even more convenient, and extremely
 *  more efficient, is the {@link #loadOnce(InputStream)}
 *  method, which reads from an input stream an arc-list ASCII graph and exposes it for a single traversal. It
 *  can be used, for instance, with the main method of {@link it.unimi.dsi.webgraph.BVGraph} to
 *  generate somehow an arc-list ASCII graph and store it in compressed form on the fly. The previous
 *  example could be then rewritten as
 *  <pre>
 *  java it.unimi.dsi.webgraph.BVGraph -1 -g ArcListASCIIGraph dummy bvexample &lt;example.arcs
 *  </pre>
 *
 */


public class ArcListASCIIGraph extends ImmutableSequentialGraph {
	private final static boolean DEBUG = false;
	private static final Logger LOGGER = LoggerFactory.getLogger(ArcListASCIIGraph.class);

	/** Number of nodes. */
	private int n;
	/** A fast buffered reader containing the description of an ASCII graph (except for the number of nodes) for a read-once ASCII graph; <code>null</code>, otherwise. */
	private final FastBufferedReader fbr;
	/** The shift. All node numbers will be shifted by this value. */
	private final int shift;

	/** Creates a read-once arc-list ASCII graph. Instances created using this constructor can be
	 * only accessed using a single call to {@link #nodeIterator(int)}.
	 *
	 * @param is an input stream containing an arc-list ASCII graph.
	 */

	public ArcListASCIIGraph(final InputStream is, final int shift) throws NumberFormatException, IOException {
		this.shift = shift;
		fbr = new FastBufferedReader(new InputStreamReader(is, "ASCII"));
		n = -1;
	}

	@Override
	public int numNodes() {
		if (n == -1) throw new UnsupportedOperationException("The number of nodes is unknown (you need to complete a traversal)");
		return n;
	}

	@Override
	public NodeIterator nodeIterator(final int from) {
		if (from < 0) throw new IllegalArgumentException();
		try {
			final StreamTokenizer st = new StreamTokenizer(fbr);
			st.eolIsSignificant(true);
			st.parseNumbers();

			return new NodeIterator() {
				/** The maximum node index we ever saw. */
				int maxNodeSeen;
				int following = -1;
				int curr = -1;
				boolean eof;

				IntArrayList successors = new IntArrayList();

				{
					fillNextLine();
					// ALERT: WRONG! This skips from lines, but does not skip up to node from!
					for(int i = 0; i < from; i++) nextInt();
				}


				private void ensureNumberToken() {
					if (st.ttype != StreamTokenizer.TT_NUMBER || st.nval != (int)st.nval) throw new IllegalArgumentException("Expected integer, found " + st.toString());
					if ((int)st.nval + shift < 0) throw new IllegalArgumentException("Integer plus shift is negative: " + st.toString());
				}

				private void fillNextLine() throws IOException {
					if (eof) return;
					if (DEBUG) System.err.println("Filling next line (curr = " + curr + ", following = " + following +")");
					successors.clear();
					if (following == -1) {
						while(st.nextToken() == StreamTokenizer.TT_EOL); // Skip empty lines
						ensureNumberToken();
					}
					if (following > (int)st.nval + shift) throw new IllegalArgumentException("Source nodes must be sorted");
					following = (int)st.nval + shift;
					if (following > maxNodeSeen) maxNodeSeen = following;

					if (DEBUG) System.err.println("New following node: " + following);
					st.nextToken();
					ensureNumberToken();
					int successor = (int)st.nval + shift;
					if (DEBUG) System.err.println("Adding successor " + successor);
					successors.add(successor);
					if (successor > maxNodeSeen) maxNodeSeen = successor;
					st.nextToken();

					for(;;) {
						final int nextToken = st.nextToken();
						if (nextToken == StreamTokenizer.TT_EOF) {
							eof = true;
							n = maxNodeSeen + 1;
							break;
						}
						if (nextToken == StreamTokenizer.TT_EOL) continue; // Skip empty lines
						ensureNumberToken();
						if ((int)st.nval + shift != following) {
							if (following > (int)st.nval + shift) throw new IllegalArgumentException("Source nodes must be sorted");
							if (DEBUG) System.err.println("New source (" + (int)st.nval + "), breaking the loop...");
							if ((int)st.nval + shift > maxNodeSeen) maxNodeSeen = (int)st.nval + shift;
							break;
						}
						st.nextToken();
						ensureNumberToken();
						successor = (int)st.nval + shift;
						if (DEBUG) System.err.println("Adding successor " + successor);
						successors.add(successor);
						if (successor > maxNodeSeen) maxNodeSeen = successor;
						st.nextToken();
					}

					IntArrays.quickSort(successors.elements(), 0, successors.size());
				}

				@Override
				public boolean hasNext() {
					return curr < maxNodeSeen;
				}

				@Override
				public int[] successorArray() {
					if (curr == -1) throw new IllegalStateException();
					return curr == following ? successors.elements() : IntArrays.EMPTY_ARRAY;
				}

				@Override
				public final int nextInt() {
					if (! hasNext()) throw new NoSuchElementException();
					if (++curr > following) try {
						fillNextLine();
					}
					catch (final IOException e) {
						throw new RuntimeException(e);
					}
					return curr;
				}

				@Override
				public int outdegree() {
					if (curr == -1) throw new IllegalStateException();
					return curr == following ? successors.size() : 0;
				}

				@Override
				public NodeIterator copy(final int upperBound) {
					throw new UnsupportedOperationException();
				}

			};
		}
		catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public NodeIterator[] splitNodeIterators(final int howMany) {
		final NodeIterator[] result = new NodeIterator[howMany];
		result[0] = nodeIterator();
		Arrays.fill(result, 1, result.length, NodeIterator.EMPTY);
		return result;
	}

	@Deprecated
	public static ImmutableGraph loadSequential(final CharSequence basename) throws IOException {
		return load(basename);
	}

	@Deprecated
	public static ImmutableGraph loadSequential(final CharSequence basename, final ProgressLogger unused) throws IOException {
		return load(basename);
	}

	public static ImmutableGraph loadOffline(final CharSequence basename) throws IOException {
		return load(basename);
	}

	public static ImmutableGraph loadOffline(final CharSequence basename, final ProgressLogger unused) throws IOException {
		return load(basename);
	}

	public static ImmutableGraph loadMapped(final CharSequence basename) throws IOException {
		return load(basename);
	}

	public static ImmutableGraph loadMapped(final CharSequence basename, final ProgressLogger unused) throws IOException {
		return load(basename);
	}

	public static ArcListASCIIGraph loadOnce(final InputStream is) throws IOException {
		return new ArcListASCIIGraph(is, 0);
	}

	public static ArcListASCIIGraph loadOnce(final InputStream is, final int shift) throws IOException {
		return new ArcListASCIIGraph(is, shift);
	}

	public static ImmutableGraph load(final CharSequence basename) throws IOException {
		return load(basename, null);
	}

	public static ImmutableGraph load(final CharSequence basename, final ProgressLogger unused) throws IOException {
		return new ArrayListMutableGraph(loadOnce(new FastBufferedInputStream(new FileInputStream(basename.toString())))).immutableView();
	}

	public static void store(final ImmutableGraph graph, final CharSequence basename, @SuppressWarnings("unused") final ProgressLogger unused) throws IOException {
		store(graph, basename);
	}


	public static void store(final ImmutableGraph graph, final CharSequence basename) throws IOException {
		store(graph, basename, 0);
	}

	/** Stores an arc-list ASCII graph with a given shift.
	 *
	 * @param graph a graph to be stored.
	 * @param basename the name of the output file.
	 * @param shift a shift that will be added to each node; note that is the <em>opposite</em> of the shift that will
	 * have to be used to load the generated file.
	 */

	public static void store(final ImmutableGraph graph, final CharSequence basename, final int shift) throws IOException {
		final PrintStream ps = new PrintStream(new FastBufferedOutputStream(new FileOutputStream(basename.toString())), false, Charsets.US_ASCII.toString());
		int d, s;
		int[] successor;
		for (final NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext();) {
			s = nodeIterator.nextInt();
			d = nodeIterator.outdegree();
			successor = nodeIterator.successorArray();
			for(int i = 0; i < d; i++) ps.println((s + shift) + "\t" + (successor[i] + shift));
		}
		ps.close();
	}

	public static void main(final String args[]) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, JSAPException  {
		String sourceBasename, destBasename;
		Class<?> graphClass;

		final SimpleJSAP jsap = new SimpleJSAP(ArcListASCIIGraph.class.getName(), "Reads a graph with a given basename and writes it out in ASCII format with another basename",
				new Parameter[] {
						new FlaggedOption("graphClass", GraphClassParser.getParser(), null, JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java class for the source graph"),
						new FlaggedOption("shift", JSAP.INTEGER_PARSER, null, JSAP.NOT_REQUIRED, 'S', "shift", "A shift that will be added to each node index."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new UnflaggedOption("sourceBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the source graph"),
						new UnflaggedOption("destBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the destination graph"),
					}
				);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		graphClass = jsapResult.getClass("graphClass");
		sourceBasename = jsapResult.getString("sourceBasename");
		destBasename = jsapResult.getString("destBasename");

		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);

		final ImmutableGraph graph = graphClass != null
			? (ImmutableGraph)graphClass.getMethod("loadOffline", CharSequence.class, ProgressLogger.class).invoke(null, sourceBasename, pl)
			: ImmutableGraph.loadOffline(sourceBasename, pl);
		if (jsapResult.userSpecified("shift")) ArcListASCIIGraph.store(graph, destBasename, jsapResult.getInt("shift"));
		else ArcListASCIIGraph.store(graph, destBasename);
	}
}
