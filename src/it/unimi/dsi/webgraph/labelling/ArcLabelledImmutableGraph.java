/*
 * Copyright (C) 2007-2023 Paolo Boldi and Sebastiano Vigna
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

import java.io.IOException;
import java.io.InputStream;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

/** An abstract implementation of a graph labelled on its arcs.
 *
 * <p>The main purpose of this class is that of override covariantly the return
 * type of {@link #nodeIterator()} and {@link #nodeIterator(int)} so that
 * it is an {@link ArcLabelledNodeIterator}, and the return type of
 * all static load methods and of {@link #copy()} so that it is an {@link ArcLabelledImmutableGraph} (the
 * methods themselves just delegate to the corresponding method in {@link ImmutableGraph}).
 *
 * <p>The only additional instance methods are {@link #labelArray(int)} and {@link #prototype()}.
 *
 * <h2>Saving labels</h2>
 *
 * <P>A subclass of this class <strong>may</strong> implement
 * <UL>
 * <LI><code>store(ArcLabelledImmutableGraph, CharSequence, CharSequence, ProgressLogger)</code>;
 * <LI><code>store(ArcLabelledImmutableGraph, CharSequence, CharSequence)</code>.
 * </UL>
 *
 * <p>These methods must save the labels of the given arc-labelled graph using the first given character
 * sequence as a basename, and a suitable property file using the second given basename. Note that the graph
 * will <strong>not</strong> be saved&mdash;use the <code>store()</code>
 * method of an {@link ImmutableGraph} implementation for that purpose.
 *
 * <p>For istance, assuming <code>g</code> is an arc-labelled graph the idiomatic way
 * of storing it on disk using {@link BVGraph} for the underlying graph and
 * {@link BitStreamArcLabelledImmutableGraph} for the labels is
 * <pre>
 * BVGraph.store(g, "foo");
 * BitStreamArcLabelledImmutableGraph.store(g, "bar", "foo");
 * </pre>
 *
 * <h2>Underlying graphs</h2>
 *
 * <p>Often, implementations of this class will just wrap an <em>underlying graph</em> (i.e.,
 * an instance of {@link ImmutableGraph}). In that case, we suggest that if the implementation
 * uses property files the basename of the underlying graph is specified using the property
 * key {@link #UNDERLYINGGRAPH_PROPERTY_KEY}. If the basename must be generated starting
 * from the arc-labelled graph basename, we suggest to just add at the end the string
 * {@link #UNDERLYINGGRAPH_SUFFIX}.
 */

public abstract class ArcLabelledImmutableGraph extends ImmutableGraph {

	/** The standard property key for the underlying graph. All implementations decorating
	 * with labels an underlying graph are strongly encouraged to use this property
	 * name to specify the basename of the underlying graph. */
	public static final String UNDERLYINGGRAPH_PROPERTY_KEY = "underlyinggraph";
	/** The standard suffix added to basenames in order to give a basename
	 * to the underlying graph, when needed. */
	public static final String UNDERLYINGGRAPH_SUFFIX = "-underlying";


	@Override
	public abstract ArcLabelledImmutableGraph copy();

	@Override
	public ArcLabelledNodeIterator nodeIterator() {
		return nodeIterator(0);
	}

	/**
	 * Returns a node iterator for scanning the graph sequentially, starting from the given node.
	 *
	 * @implSpec This implementation strengthens that provided in {@link ImmutableGraph}, but calls the
	 *           labelled random-access method {@link #successors(int)}.
	 *
	 * @param from the node from which the iterator will iterate.
	 * @return an {@link ArcLabelledNodeIterator} for accessing nodes, successors and their labels
	 *         sequentially.
	 *
	 * @see ImmutableGraph#nodeIterator()
	 */
	@Override
	public ArcLabelledNodeIterator nodeIterator(final int from) {
		class InternalArcLabelledNodeIterator extends ArcLabelledNodeIterator  {
			private int curr = from - 1;
			private final int n = numNodes();
			private final int hasNextLimit;

			public InternalArcLabelledNodeIterator(final int upperBound) {
				this.hasNextLimit = Math.min(n - 1, upperBound - 1);
			}

			@Override
			public int nextInt() {
				if (! hasNext()) throw new java.util.NoSuchElementException();
				return ++curr;
			}

			@Override
			public boolean hasNext() {
				return curr < hasNextLimit;
			}

			@Override
			public LabelledArcIterator successors() {
				if (curr == from - 1) throw new IllegalStateException();
				return ArcLabelledImmutableGraph.this.successors(curr);
			}

			@Override
			public int outdegree() {
				if (curr == from - 1) throw new IllegalStateException();
				return ArcLabelledImmutableGraph.this.outdegree(curr);
			}

			@Override
			public ArcLabelledNodeIterator copy(final int upperBound) {
				final InternalArcLabelledNodeIterator result = new InternalArcLabelledNodeIterator(upperBound);
				result.curr = curr;
				return result;
			}
		}


		return new InternalArcLabelledNodeIterator(Integer.MAX_VALUE);
	}

	@Override
	public abstract ArcLabelledNodeIterator.LabelledArcIterator successors(int x);

	/** Returns a prototype of the labels used by this graph. The prototype can be
	 * used to produce new copies, but must not be modified by the caller.
	 *
	 * @return a prototype for the labels of this graph.
	 */
	public abstract Label prototype();

	/**
	 * Returns a reference to an array containing the labels of the arcs going out of a given node in
	 * the same order as the order in which the corresponding successors are returned by
	 * {@link #successors(int)}.
	 *
	 * <P>
	 * The returned array may contain more entries than the outdegree of <code>x</code>. However, only
	 * those with indices from 0 (inclusive) to the outdegree of <code>x</code> (exclusive) contain
	 * valid data.
	 *
	 * @implSpec This implementation just unwrap the iterator returned by {@link #successors(int)} and
	 *           writes in a newly allocated array copies of the labels returned by
	 *           {@link LabelledArcIterator#label()}.
	 *
	 * @return an array whose first elements are the labels of the arcs going out of <code>x</code>; the
	 *         array must not be modified by the caller.
	 */

	public Label[] labelArray(final int x) {
		return ArcLabelledNodeIterator.unwrap(successors(x), outdegree(x));
	}

	@Deprecated
	public static ArcLabelledImmutableGraph loadSequential(final CharSequence basename) throws IOException {
		return (ArcLabelledImmutableGraph)ImmutableGraph.loadSequential(basename);
	}

	@Deprecated
	public static ArcLabelledImmutableGraph loadSequential(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return (ArcLabelledImmutableGraph)ImmutableGraph.loadSequential(basename, pl);
	}

	public static ArcLabelledImmutableGraph loadOffline(final CharSequence basename) throws IOException {
		return (ArcLabelledImmutableGraph)ImmutableGraph.loadOffline(basename);
	}

	public static ArcLabelledImmutableGraph loadOffline(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return (ArcLabelledImmutableGraph)ImmutableGraph.loadOffline(basename, pl);
	}

	public static ArcLabelledImmutableGraph load(final CharSequence basename) throws IOException {
		return (ArcLabelledImmutableGraph)ImmutableGraph.load(basename);
	}

	public static ArcLabelledImmutableGraph load(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return (ArcLabelledImmutableGraph)ImmutableGraph.load(basename, pl);
	}

	public static ArcLabelledImmutableGraph loadOnce(final InputStream is) throws IOException {
		return (ArcLabelledImmutableGraph)ImmutableGraph.loadOnce(is);
	}

	@Override
	public String toString() {
		final StringBuilder s = new StringBuilder();

		long numArcs = -1;
		try {
			numArcs = numArcs();
		}
		catch(final UnsupportedOperationException ignore) {}

		s.append("Nodes: " + numNodes() + "\nArcs: " + (numArcs == -1 ? "unknown" : Long.toString(numArcs)) + "\n");

		final ArcLabelledNodeIterator nodeIterator = nodeIterator();
		ArcLabelledNodeIterator.LabelledArcIterator successors;
		int curr;
		for (int i = numNodes(); i-- != 0;) {
			curr = nodeIterator.nextInt();
			s.append("Successors of " + curr + " (degree " + nodeIterator.outdegree() + "):");
			successors = nodeIterator.successors();
			int d = nodeIterator.outdegree();
			while (d-- != 0) s.append(" " + successors.nextInt() + " [" + successors.label() + "]");
			s.append('\n');
		}
		return s.toString();
	}

	@Override
	public boolean equals(final Object x) {
		if (! (x instanceof ArcLabelledImmutableGraph)) return false;
		final ArcLabelledImmutableGraph g = (ArcLabelledImmutableGraph)x;
		if (g.numNodes() != numNodes()) return false;
		final ArcLabelledNodeIterator nodeIterator = nodeIterator();
		final ArcLabelledNodeIterator gNodeIterator = g.nodeIterator();
		while (nodeIterator.hasNext()) {
			nodeIterator.nextInt(); gNodeIterator.nextInt();
			if (nodeIterator.outdegree() != gNodeIterator.outdegree()) return false;
			final LabelledArcIterator arcIterator = nodeIterator.successors();
			final LabelledArcIterator gArcIterator = gNodeIterator.successors();
			int d = nodeIterator.outdegree();
			while (d-- != 0) {
				if (arcIterator.nextInt() != gArcIterator.nextInt()
						|| ! arcIterator.label().equals(gArcIterator.label())) return false;
			}
		}
		return true;
	}
}
