/*
 * Copyright (C) 2003-2023 Paolo Boldi and Sebastiano Vigna
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
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.logging.ProgressLogger;

/** A simple abstract class representing an immutable graph.
 *
 * <P>Subclasses of this class are used to create and access <em>immutable graphs</em>, that is,
 * graphs that are computed once for all, stored conveniently, and then accessed repeatedly.
 * Moreover, immutable graphs are usually very large&mdash;so large that two such graphs may not
 * fit into central memory (the main example being a sizable portion of the web).
 *
 * <P>A subclass of this class must implement methods to obtain the {@linkplain
 * #numNodes() number of nodes}, the {@linkplain #outdegree(int) outdegree of a
 * node} and the successors of a node (either {@link #successors(int)}
 * or {@link #successorArray(int)}). Additionally, it may provide methods to
 * obtain the {@linkplain #numNodes() number of arcs}, and a {@linkplain #basename() basename}.
 *
 * <P>This class provides {@link #equals(Object)} and {@link #hashCode()} methods that consider
 * two graph equals if they have the same size and all their successor lists are equal.
 *
 * <H2>Iterating on successors</H2>
 *
 * <p>Starting with WebGraph 2.0, the iterator architecture is <em>fully lazy</em>&mdash;you have no
 * <code>hasNext()</code> method. Rather, the {@link LazyIntIterator} returned by {@link #successors(int)}
 * will return -1 when no more successors are available. The idiomatic forms for enumerating successors
 * <i>via</i> iterators are
 * <pre>
 * LazyIntIterator successors = g.successors(x);
 * int d = g.outdegree(x);
 * while(d-- != 0) doSomething(successors.nextInt());
 * </pre>
 * and
 * <pre>
 * LazyIntIterator successors = g.successors(x);
 * int t;
 * while((t = successors.nextInt()) != -1) doSomething(t);
 * </pre>
 *
 * <p>The alternative method {@link #successorArray(int)} provides an array containing the successors
 * <em>and possibly more elements</em>. Use {@link #outdegree(int)} to know how many elements are valid.
 * The efficiency of {@link #successors(int)} and {@link #successorArray(int)} may vary depending on the
 * implementation.
 *
 * <H2>Iterating on a graph in parallel</H2>
 *
 * <p>You can scan a graph sequentially using {@linkplain NodeIterator node iterators}. Starting with version 3.5.0,
 * implementations of this class may return true on {@link #hasCopiableIterators()}, which means that
 * node iterators implement the optional {@link NodeIterator#copy(int) copy(int)} method. Using {@link NodeIterator#copy(int) copy(int)},
 * the method {@link #splitNodeIterators(int)} of this class is able to provide separate, thread-safe iterators on different segments
 * of contiguous nodes of the graph. The class {@link BVGraph}, for example, uses this interface to provide
 * parallel compression. We suggest that all classes providing parallel iteration read the system variable
 * {@value #NUMBER_OF_THREADS_PROPERTY} to override the number of parallel threads.
 *
 *
 * <H2>Building an immutable graph</H2>
 *
 * <P>Due to their large size, immutable
 * graphs have a peculiar serialisation scheme. Every subclass of this class
 * <strong>must</strong> implement a number of static methods that create an immutable
 * graph, given a string (usually a basename for a set of files) and, optionally, a {@link it.unimi.dsi.logging.ProgressLogger}.
 * The signatures that <strong>must</strong> be implemented are
 * <UL>
 * <LI><code>ImmutableGraph load(CharSequence, ProgressLogger)</code>;
 * <LI><code>ImmutableGraph load(CharSequence)</code>;
 * <LI><code>ImmutableGraph loadOffline(CharSequence, ProgressLogger)</code>;
 * <LI><code>ImmutableGraph loadOffline(CharSequence)</code>.
 * <LI><code>ImmutableGraph loadOnce(InputStream)</code>;
 * </UL>
 *
 * <p>Additionally, the following signatures <strong>can</strong> be implemented:
 * <UL>
 * <LI><code>ImmutableGraph loadMapped(CharSequence, ProgressLogger)</code>;
 * <LI><code>ImmutableGraph loadMapped(CharSequence)</code>;
 * </UL>
 *
 * <p>The special semantics associated to <code>loadOffline()</code>
 * is that the immutable graph should be set up, and possibly some metadata could be read from disk, but no
 * actual data is loaded into memory; the class should guarantee that offline sequential access (i.e., by means
 * of {@link #nodeIterator(int)}) is still possible. In other words, in most cases {@link #nodeIterator(int)} will have to be
 * overridden by the subclasses to behave properly even in an offline setting (see {@link #nodeIterator()}).
 * The special semantics associated with <code>loadOnce()</code> is that the graph can be traversed
 * <em>just once</em> using a call to {@link #nodeIterator()}. The special semantics associated with <code>loadMapped()</code>
 * is that metadata could be read from disk, but the graph will be accessed by memory mapping; the class
 * should guarantee that random access is possible.
 *
 * <P>Note that a simple class may just implement all special forms of graph loading delegating to the standard
 * load method (see, e.g., {@link it.unimi.dsi.webgraph.ASCIIGraph}).
 * Specific implementations of {@link ImmutableGraph} may also decide to expose internal load methods
 * to make it easier to write load methods for subclasses
 * (see, e.g., {@link it.unimi.dsi.webgraph.BVGraph#loadInternal(CharSequence, int, ProgressLogger) loadInternal()}).
 *
 * <P>Analogously, a subclass of this class <strong>may</strong> also implement
 * <UL>
 * <LI><code>store(ImmutableGraph, CharSequence, ProgressLogger)</code>;
 * <LI><code>store(ImmutableGraph, CharSequence)</code>.
 * </UL>
 *
 * These methods must store in compressed form a given immutable graph, using the default values
 * for compression parameters, etc. It is likely, however, that more
 * of <code>store</code> methods are available, as parameters vary wildly
 * from subclass to subclass. The method {@link #store(Class, ImmutableGraph, CharSequence, ProgressLogger)}
 * invokes by reflection the methods above on the provided class.
 *
 * <P>The standard method to build a new immutable graph is creating a (possibly anonymous) class
 * that extends this class, and save it using a concrete subclass (e.g., {@link it.unimi.dsi.webgraph.BVGraph}). See
 * the source of {@link it.unimi.dsi.webgraph.Transform} for several examples.
 *
 * <H2>Properties Conventions</H2>
 *
 * <P>To provide a simple way to load an immutable graph without knowing in advance its class,
 * the following convention may be followed: a graph with basename <var><code>name</code></var> may feature
 * a Java property file <code><var>name</var>.properties</code> with a property <code>graphclass</code>
 * containing the actual class of the graph. In this case, you can use the implementation of the load/store
 * methods contained in this class, similarly to the standard Java serialisation scheme. {@link BVGraph}, for instance,
 * follows this convention, but {@link ASCIIGraph} does not.
 *
 * <P>The reason why this convention is not enforced is that it is sometimes useful to write lightweight classes,
 * mostly for debugging purposes, whose graph representation is entirely contained in a single file (e.g., {@link ASCIIGraph}),
 * so that {@link #loadOnce(InputStream)} can be easily implemented.
 *
 * <H2>Facilities for loading an immutable graph</H2>
 *
 * <P>{@link ImmutableGraph} provides ready-made implementations of the load methods that work as follows: they
 * opens a property file with the given basename, and look for the <code>graphclass</code> property; then, they simply
 * delegates the actual load to the specified graph class by reflection.
 *
 * <h2>Thread-safety and flyweight copies</h2>
 *
 * <p>Implementations of this class need not be thread-safe. However, they implement the
 * {@link FlyweightPrototype} pattern: the {@link #copy()} method is
 * thread-safe and will return a lightweight copy of the graph&mdash;usually, all immutable
 * data will be shared between copies. Concurrent access to different copies is safe.
 *
 * <p>Note that by contract {@link #copy()} is guaranteed to work only if {@link #randomAccess()}
 * returns true.
 */


public abstract class ImmutableGraph implements FlyweightPrototype<ImmutableGraph> {
	private final static Logger LOGGER = LoggerFactory.getLogger(ImmutableGraph.class);

	public static final String GRAPHCLASS_PROPERTY_KEY = "graphclass";
	/** The standard extension of property files. */
	public static final String PROPERTIES_EXTENSION = ".properties";
	/** The property used to set the number of parallel compression threads. */
	public static final String NUMBER_OF_THREADS_PROPERTY = "it.unimi.dsi.webgraph.threads";

	private final static class ImmutableGraphNodeIterator extends NodeIterator {
		private final ImmutableGraph graph;
		private final int from;
		private final int to;
		private int curr;

		private ImmutableGraphNodeIterator(final ImmutableGraph graph, final int from, final int to) {
			this.graph = graph;
			this.from = from;
			curr = from - 1;
			this.to = Math.min(graph.numNodes(), to);
		}

		@Override
		public int nextInt() {
			if (! hasNext()) throw new java.util.NoSuchElementException();
			return ++curr;
		}

		@Override
		public boolean hasNext() {
			return curr < to - 1;
		}

		@Override
		public LazyIntIterator successors() {
			if (curr == from - 1) throw new IllegalStateException();
			return graph.successors(curr);
		}

		@Override
		public int outdegree() {
			if (curr == from - 1) throw new IllegalStateException();
			return graph.outdegree(curr);
		}

		@Override
		public NodeIterator copy(final int upperBound) {
			return new ImmutableGraphNodeIterator(graph.copy(), curr + 1, upperBound);
		}
	}

	/** A list of the methods that can be used to load a graph. They are used
	 * by {@link ImmutableGraph} and other classes to represent standard
	 * (i.e., random access), sequential, offline and read-once graph loading. */

	public static enum LoadMethod {
		STANDARD,
		@Deprecated
		SEQUENTIAL,
		OFFLINE,
		ONCE,
		MAPPED;

		public String toMethod() {
			switch(this) {
			case STANDARD: return "load";
			case SEQUENTIAL: return "loadSequential";
			case OFFLINE: return "loadOffline";
			case ONCE: return "loadOnce";
			case MAPPED: return "loadMapped";
			default: throw new AssertionError();
			}
		}
	}

	/** Returns the number of nodes of this graph.
	 *
	 * <p>Albeit this method is not optional, it is allowed that this method throws
	 * an {@link UnsupportedOperationException} if this graph has never been entirely
	 * traversed using a {@link #nodeIterator() node iterator}. This apparently bizarre
	 * behaviour is necessary to support implementations as {@link ArcListASCIIGraph}, which
	 * do not know the actual number of nodes until a traversal has been completed.
	 *
	 * @return the number of nodes.
	 */
	public abstract int numNodes();

	/** Returns the number of arcs of this graph (optional operation).
	 *
	 * @return the number of arcs.
	 */
	public long numArcs() {
		throw new UnsupportedOperationException();
	}

	/** Checks whether this graph provides random access to successor lists.
	 *
	 * @return true if this graph provides random access to successor lists.
	 */
	public abstract boolean randomAccess();

	/**
	 * Whether the node iterators returned by this graph support {@link NodeIterator#copy(int)}.
	 *
	 * @implSpec This implementation just returns {@link #randomAccess()}.
	 *
	 * @return true if this graph provides copiable iterators.
	 */
	public boolean hasCopiableIterators() {
		return randomAccess();
	}

	/**
	 * Returns a symbolic basename for this graph (optional operation).
	 *
	 * @implNote Implementors of this class may provide a basename (usually a pathname from which
	 *           various files storing the graph are stemmed). This method is optional because it is
	 *           sometimes unmeaningful (e.g., for one-off anonymous classes).
	 *
	 * @return the basename.
	 */
	public CharSequence basename() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns a lazy iterator over the successors of a given node. The iteration terminates when -1 is
	 * returned.
	 *
	 * @implSpec This implementation just wraps the array returned by {@link #successorArray(int)}.
	 *           Subclasses are encouraged to override this implementation.
	 *
	 * @apiNote The semantics of this method has been significantly modified in WebGraph 2.0 to take
	 *          advantage of the new, faster lazy architecture.
	 *
	 * @param x a node.
	 * @return a lazy iterator over the successors of the node.
	 */
	public LazyIntIterator successors(final int x) {
		return LazyIntIterators.wrap(successorArray(x), outdegree(x));
	}

	/**
	 * Returns a reference to an array containing the successors of a given node.
	 *
	 * <P>
	 * The returned array may contain more entries than the outdegree of <code>x</code>. However, only
	 * those with indices from 0 (inclusive) to the outdegree of <code>x</code> (exclusive) contain
	 * valid data.
	 *
	 * @implSpec This implementation just unwraps the iterator returned by {@link #successors(int)}.
	 *           Subclasses are encouraged to override this implementation.
	 *
	 * @implNote All implementations <strong>must</strong> guarantee that a distinct array is returned
	 *           for each node. The caller, in turn, must treat the array as a read-only object.
	 *
	 * @param x a node.
	 * @return an array whose first elements are the successors of the node; the array must not be
	 *         modified by the caller.
	 */
	public int[] successorArray(final int x) {
		final int[] successor = new int[outdegree(x)];
		LazyIntIterators.unwrap(successors(x), successor);
		return successor;
	}

	/** Returns the outdegree of a node.
	 *
	 * @param x a node.
	 * @throws IllegalStateException if called without offsets.
	 * @return the outdegree of the given node.
	 */
	public abstract int outdegree(int x);

	/**
	 * Returns a node iterator for scanning the graph sequentially, starting from the given node.
	 *
	 * @implSpec This implementation just calls the random-access methods ({@link #successors(int)} and
	 *           {@link #outdegree(int)}). More specific implementations may choose to maintain some
	 *           extra state to make the enumeration more efficient.
	 *
	 * @param from the node from which the iterator will iterate.
	 * @return a {@link NodeIterator} for accessing nodes and successors sequentially.
	 */
	public NodeIterator nodeIterator(final int from) {
		return new ImmutableGraphNodeIterator(this, from, Integer.MAX_VALUE);
	}

	/** Returns a node iterator for scanning the graph sequentially, starting from the first node.
	 *
	 *  @return a {@link NodeIterator} for accessing nodes and successors sequentially.
	 */
	public NodeIterator nodeIterator() {
		return nodeIterator(0);
	}

	/**
	 * Returns an array of node iterators, scanning each a portion of the nodes of a graph. Iterators
	 * are guaranteed to scan mutually disjoint sets of nodes, and every node is guaranteed to be
	 * scanned by one iterator.
	 *
	 * <p>
	 * This is an optional operation. If implemented, though, the returned iterators must properly
	 * implement {@link NodeIterator#copy(int)}.
	 *
	 * @param howMany the number of iterators to be returned (at the end of the array, some of them may
	 *            be empty).
	 * @return the required iterators; some of them might be {@code null} (e.g., if the graph
	 *         {@linkplain #hasCopiableIterators() does not have copiable iterators}).
	 */
	public NodeIterator[] splitNodeIterators(final int howMany) {
		if (numNodes() == 0 && howMany == 0) return new NodeIterator[0];
		if (howMany < 1) throw new IllegalArgumentException();
		final NodeIterator[] result = new NodeIterator[howMany];
		if (! hasCopiableIterators()) {
			// No possibility to split
			result[0] = nodeIterator();
			return result;
		}
		final int n = numNodes();
		final int m = (int)Math.ceil((double)n / howMany);
		if (randomAccess()) {
			int from, i;
			// This approach is slightly wasteful, but replicating the state should have an infinitesimal cost.
			for (from = i = 0; from < n; from += m, i++) result[i] = nodeIterator(from).copy(from + m);
			Arrays.fill(result, i, result.length, NodeIterator.EMPTY);
			return result;
		} else {
			final NodeIterator nodeIterator = nodeIterator();
			int i = 0;
			int nextNode = 0;
			while (i < result.length && nodeIterator.hasNext()) {
				if (nextNode % m == 0) result[i++] = nodeIterator.copy(nextNode + m);
				final int node = nodeIterator.nextInt();
				assert node == nextNode;
				nextNode++;
			}
			Arrays.fill(result, i, result.length, NodeIterator.EMPTY);
			return result;
		}
	}

	/** Returns a flyweight copy of this immutable graph.
	 *
	 * @return a flyweight copy of this immutable graph.
	 * @throws UnsupportedOperationException if flyweight copies are not supported:
	 * support is guaranteed only if {@link #randomAccess()} returns true.
	 * @see FlyweightPrototype
	 */

	@Override
	public abstract ImmutableGraph copy();

	@Override
	public String toString() {
		final StringBuilder s = new StringBuilder();

		long numArcs = -1;
		try {
			numArcs = numArcs();
		}
		catch(final UnsupportedOperationException ignore) {}

		s.append("Nodes: " + numNodes() + "\nArcs: " + (numArcs == -1 ? "unknown" : Long.toString(numArcs)) + "\n");

		final NodeIterator nodeIterator = nodeIterator();
		LazyIntIterator successors;
		int curr;
		for (int i = numNodes(); i-- != 0;) {
			curr = nodeIterator.nextInt();
			s.append("Successors of " + curr + " (degree " + nodeIterator.outdegree() + "):");
			successors = nodeIterator.successors();
			int d = nodeIterator.outdegree();
			while (d-- != 0) s.append(" " + successors.nextInt());
			s.append('\n');
		}
		return s.toString();
	}

	/** Returns an iterator enumerating the outdegrees of the nodes of this graph.
	 *
	 * @return  an iterator enumerating the outdegrees of the nodes of this graph.
	 */
	public IntIterator outdegrees() {
		return randomAccess() ?
		new IntIterator() {
			private final int n = numNodes();
			private int next = 0;
			@Override
			public boolean hasNext() {
				return next < n;
			}
			@Override
			public int nextInt() {
				if (! hasNext()) throw new NoSuchElementException();
				return outdegree(next++);
			}
		} :
		new IntIterator() {
			private final NodeIterator nodeIterator = nodeIterator();
			@Override
			public boolean hasNext() {
				return nodeIterator.hasNext();
			}
			@Override
			public int nextInt() {
				nodeIterator.nextInt();
				return nodeIterator.outdegree();
			}
		};
	}


	/** Creates a new {@link ImmutableGraph} by loading a graph file from disk to memory, without
	 *  offsets.
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param basename the basename of the graph.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 * @deprecated Use {@link #loadOffline(CharSequence)} or {@link #loadMapped(CharSequence)} instead.
	 */
	@Deprecated
	public static ImmutableGraph loadSequential(final CharSequence basename) throws IOException {
		return load(LoadMethod.SEQUENTIAL, basename, null);
	}

	/** Creates a new {@link ImmutableGraph} by loading a graph file from disk to memory, without
	 *  offsets.
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param basename the basename of the graph.
	 * @param pl a progress logger used while loading the graph, or <code>null</code>.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 * @deprecated Use {@link #loadOffline(CharSequence, ProgressLogger)} or {@link #loadMapped(CharSequence, ProgressLogger)} instead.
	 */
	@Deprecated
	public static ImmutableGraph loadSequential(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.SEQUENTIAL, basename, null, pl);
	}

	/** Creates a new {@link ImmutableGraph} by loading offline a graph file.
	 *
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param basename the basename of the graph.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 */

	public static ImmutableGraph loadOffline(final CharSequence basename) throws IOException {
		return load(LoadMethod.OFFLINE, basename, null);
	}


	/** Creates a new {@link ImmutableGraph} by loading offline a graph file.
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param basename the basename of the graph.
	 * @param pl a progress logger used while loading the graph, or <code>null</code>.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 */

	public static ImmutableGraph loadOffline(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.OFFLINE, basename, null, pl);
	}


	/** Creates a new {@link ImmutableGraph} by memory-mapping a graph file.
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param basename the basename of the graph.
	 * @param pl a progress logger used while loading the offsets, or <code>null</code>.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while memory-mapping the graph or reading the offsets.
	 */

	public static ImmutableGraph loadMapped(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.MAPPED, basename, null, pl);
	}

	/** Creates a new {@link ImmutableGraph} by memory-mapping a graph file.
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param basename the basename of the graph.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while memory-mapping the graph or reading the offsets.
	 */

	public static ImmutableGraph loadMapped(final CharSequence basename) throws IOException {
		return load(LoadMethod.MAPPED, basename, null);
	}


	/**
	 * Creates a new {@link ImmutableGraph} by loading a read-once graph from an input stream.
	 *
	 * @implSpec This implementation just throws a {@link UnsupportedOperationException}. There is no
	 *           way to write a generic implementation, because there is no way to know in advance the
	 *           class that should read the graph.
	 *
	 * @param is an input stream containing the graph.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 * @throws UnsupportedOperationException if this graph class does not support read-once graphs.
	 */

	public static ImmutableGraph loadOnce(final InputStream is) throws IOException {
		throw new UnsupportedOperationException("This class does not support read-once loading");
	}


	/** Creates a new {@link ImmutableGraph} by loading a graph file from disk to memory, with
	 *  all offsets, using no progress logger.
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param basename the basename of the graph.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 */


	public static ImmutableGraph load(final CharSequence basename) throws IOException {
		return load(LoadMethod.STANDARD, basename, null);
	}

	/** Creates a new {@link ImmutableGraph} by loading a graph file from disk to memory, with
	 *  all offsets, using a progress logger.
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param basename the basename of the graph.
	 * @param pl a progress logger used while loading the graph, or <code>null</code>.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 */

	public static ImmutableGraph load(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.STANDARD, basename, null, pl);
	}

	private static final ProgressLogger UNUSED = new ProgressLogger();

	/** Creates a new {@link ImmutableGraph} using the given method and no progress logger.
	 *
	 * @param method the load method.
	 * @param basename the basename of the graph, if <code>method</code> is not {@link LoadMethod#ONCE}.
	 * @param is an input stream the containing the graph, if <code>method</code> is {@link LoadMethod#ONCE}.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 */
	private static ImmutableGraph load(final LoadMethod method, final CharSequence basename, final InputStream is) throws IOException {
		return load(method, basename, is, UNUSED);
	}

	/** Creates a new immutable graph by loading a graph file from disk to memory, delegating the
	 *  actual loading to the class specified in the <code>graphclass</code> property within the property
	 *  file (named <code><var>basename</var>.properties</code>). The exact load method to be used
	 *  depends on the <code>method</code> argument.
	 *
	 * <P>This method uses the properties convention described in the {@linkplain ImmutableGraph introduction}.
	 *
	 * @param method the method to be used to load the graph.
	 * @param basename the basename of the graph, if <code>method</code> is not {@link LoadMethod#ONCE}.
	 * @param is an input stream the containing the graph, if <code>method</code> is {@link LoadMethod#ONCE}.
	 * @param pl the progress logger; it can be <code>null</code>.
	 * @return an {@link ImmutableGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 */
	protected static ImmutableGraph load(final LoadMethod method, final CharSequence basename, final InputStream is, final ProgressLogger pl) throws IOException {
		final FileInputStream propertyFile = new FileInputStream(basename + PROPERTIES_EXTENSION);
		final Properties properties = new Properties();
		String graphClassName;
		properties.load(propertyFile);
		propertyFile.close();

		if ((graphClassName = properties.getProperty(GRAPHCLASS_PROPERTY_KEY)) == null) throw new IOException("The property file for " + basename + " does not contain a graphclass property");

		// Small kludge to fix old usage of toString() instead of getName();
		if (graphClassName.startsWith("class ")) graphClassName = graphClassName.substring(6);

		// Small kludge to try to load small graphs created with the big version.
		if (graphClassName.startsWith("it.unimi.dsi.big.webgraph")) {
			final String standardGraphClassName = graphClassName.replace("it.unimi.dsi.big.webgraph", "it.unimi.dsi.webgraph");
			LOGGER.warn("Replacing class " + graphClassName + " with " + standardGraphClassName);
			graphClassName = standardGraphClassName;
		}

		final Class<?> graphClass;
		ImmutableGraph graph = null;

		try {
			graphClass = Class.forName(graphClassName);

			if (method == LoadMethod.ONCE) graph = (ImmutableGraph)graphClass.getMethod(method.toMethod(), InputStream.class).invoke(null, is);
			else {
				if (pl == UNUSED) graph = (ImmutableGraph)graphClass.getMethod(method.toMethod(), CharSequence.class).invoke(null, basename);
				else graph = (ImmutableGraph)graphClass.getMethod(method.toMethod(), CharSequence.class, ProgressLogger.class).invoke(null, basename, pl);
			}
		} catch (final InvocationTargetException e) {
			if (e.getCause() instanceof IOException) throw (IOException) e.getCause();
			throw new RuntimeException(e);
		} catch(final Exception e) {
			throw new RuntimeException(e);
		}

		return graph;
	}


	/** Stores an immutable graph using a specified subclass and a progress logger.
	 *
	 * <P>This method is a useful shorthand that invoke by reflection the store method of a given subclass.
	 * Note, however, that usually a subclass will provide more refined store methods with more parameters.
	 *
	 * @param graphClass the subclass of {@link ImmutableGraph} that should store the graph.
	 * @param graph the graph to store.
	 * @param basename the basename.
	 * @param pl a progress logger, or <code>null</code>.
	 */

	public static void store(final Class<?> graphClass, final ImmutableGraph graph, final CharSequence basename, final ProgressLogger pl) throws IOException {
		if (! ImmutableGraph.class.isAssignableFrom(graphClass)) throw new ClassCastException(graphClass.getName() + " is not a subclass of ImmutableGraph");
		try {
			if (pl == UNUSED) graphClass.getMethod("store", ImmutableGraph.class, CharSequence.class).invoke(null, graph, basename);
			else graphClass.getMethod("store", ImmutableGraph.class, CharSequence.class, ProgressLogger.class).invoke(null, graph, basename, pl);
		} catch (final InvocationTargetException e) {
			if (e.getCause() instanceof IOException) throw (IOException) e.getCause();
			throw new RuntimeException(e);
		} catch(final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** Stores an immutable graph using a specified subclass.
	 *
	 * @param graphClass the subclass of {@link ImmutableGraph} that should store the graph.
	 * @param graph the graph to store.
	 * @param basename the basename.
	 * @see #store(Class, ImmutableGraph, CharSequence, ProgressLogger)
	 */

	public static void store(final Class<?> graphClass, final ImmutableGraph graph, final CharSequence basename) throws IOException {
		store(graphClass, graph, basename, UNUSED);
	}

	/** Compare this immutable graph to another object.
	 *
	 * @return true iff the given object is an immutable graph of the same size, and
	 * the successor list of every node of this graph is equal to the successor list of the corresponding node of <code>o</code>.
	 */

	@Override
	public boolean equals(final Object o) {
		if (! (o instanceof ImmutableGraph)) return false;
		final ImmutableGraph g = (ImmutableGraph) o;
		int n = numNodes();
		if (n != g.numNodes()) return false;
		final NodeIterator i = nodeIterator(), j = g.nodeIterator();
		int[] s, t;
		int d;
		while(n-- != 0) {
			i.nextInt();
			j.nextInt();
			if ((d = i.outdegree()) != j.outdegree()) return false;
			s = i.successorArray();
			t = j.successorArray();
			while(d-- != 0) if (s[d] != t[d]) return false;
		}

		return true;
	}

	/** Returns a hash code for this immutable graph.
	 *
	 * @return a hash code for this immutable graph.
	 */

	@Override
	public int hashCode() {
		int n = numNodes(), h = -1;
		final NodeIterator i = nodeIterator();
		int[] s;
		int d;
		while(n-- != 0) {
			h = h * 31 + i.nextInt();
			s = i.successorArray();
			d = i.outdegree();
			while(d-- != 0) h = h * 31 + s[d];
		}

		return h;
	}

}
