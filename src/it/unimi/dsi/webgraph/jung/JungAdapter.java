/*
 * Copyright (C) 2012-2021 Sebastiano Vigna
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

package it.unimi.dsi.webgraph.jung;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.uci.ics.jung.graph.util.Pair;
import edu.uci.ics.jung.io.PajekNetWriter;
import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLists;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;

/** An adapter exposing an {@link ImmutableGraph} as a <a href="http://jung.sourceforge.net/">Jung</a>
 * {@link DirectedGraph}.
 *
 * <p>Using this adapter it is easy to apply Jung's analysis and visualisation code to {@linkplain ImmutableGraph immutable graphs}.
 *
 * <p>Edges are just {@link Long}s, and their values are the index of the source node, shifted to the left by
 * 32 bits, OR'd with the index of the target node.
 *
 * <p>The main method of this class provides a simple way to translate any immutable graph in {@linkplain PajekNetWriter Pajek format}.
 */

public class JungAdapter implements DirectedGraph<Integer, Long> {
	/** The immutable graph to be exposed. */
	private final ImmutableGraph graph;
	/** The transpose of {@link #graph}. */
	private final ImmutableGraph transpose;
	/** The number of nodes of {@link #graph}. */
	private final int n;

	/** Creates a Jung adapter.
	 *
	 * @param graph a graph.
	 * @param transpose its transpose (look at {@link Transform#transpose(ImmutableGraph)}
	 * and {@link Transform#transposeOffline(ImmutableGraph, int, java.io.File)} for ways
	 * to generate the transpose of a graph).
	 * @throws IllegalArgumentException if <code>graph</code> has more than {@link Integer#MAX_VALUE} arcs (as
	 * {@link #getEdgeCount()} returns an integer).
	 */

	public JungAdapter(final ImmutableGraph graph, final ImmutableGraph transpose) {
		this.graph = graph;
		this.transpose = transpose;
		this.n = graph.numNodes();
		if (graph.numArcs() > Integer.MAX_VALUE) throw new IllegalArgumentException();
	}

	@Override
	public Integer getSource(final Long e) {
		return Integer.valueOf((int)(e.longValue() >>> 32));
	}

	@Override
	public Integer getDest(final Long e) {
		return Integer.valueOf(e.intValue());
	}

	@Override
	public Pair<Integer> getEndpoints(final Long e) {
		return new Pair<>(getSource(e), getDest(e));
	}

	@Override
	public Collection<Long> getInEdges(final Integer x) {
		final int v = x.intValue();
		final ObjectArrayList<Long> list = new ObjectArrayList<>(transpose.outdegree(v));
		final LazyIntIterator pred = transpose.successors(v);
		for(long p; (p = pred.nextInt()) != -1;) list.add(Long.valueOf(p << 32 | v));
		return list;
	}

	@Override
	public Integer getOpposite(final Integer v, final Long e) {
		final int x = e.intValue();
		if (x != v.intValue()) return Integer.valueOf(x);
		else return getSource(e);
	}

	@Override
	public Collection<Long> getOutEdges(final Integer x) {
		final int v = x.intValue();
		final ObjectArrayList<Long> list = new ObjectArrayList<>(graph.outdegree(v));
		final LazyIntIterator succ = graph.successors(v);
		final long nodeShifted = (long)v << 32;
		for(int s; (s = succ.nextInt()) != -1;) list.add(Long.valueOf(nodeShifted | s));
		return list;
	}

	@Override
	public int getPredecessorCount(final Integer x) {
		return transpose.outdegree(x.intValue());
	}

	private static Collection<Integer> getSuccessors(final ImmutableGraph g, final int x) {
		final ObjectArrayList<Integer> list = new ObjectArrayList<>(g.outdegree(x));
		final LazyIntIterator succ = g.successors(x);
		for(int s; (s = succ.nextInt()) != -1;) list.add(Integer.valueOf(s));
		return list;
	}

	@Override
	public Collection<Integer> getPredecessors(final Integer x) {
		return getSuccessors(transpose, x.intValue());
	}

	@Override
	public int getSuccessorCount(final Integer x) {
		return graph.outdegree(x.intValue());
	}

	@Override
	public Collection<Integer> getSuccessors(final Integer x) {
		return getSuccessors(graph, x.intValue());
	}

	@Override
	public int inDegree(final Integer x) {
		return getPredecessorCount(x);
	}

	@Override
	public boolean isDest(final Integer v, final Long e) {
		return e.intValue() == v.intValue();
	}

	public boolean isArc(final int x, final int y) {
		final LazyIntIterator succ = graph.successors(x);
		for(int s; (s = succ.nextInt()) != -1;) if (s == y) return true;
		return false;
	}

	@Override
	public boolean isPredecessor(final Integer x, final Integer y) {
		return isArc(x.intValue(), y.intValue());
	}

	@Override
	public boolean isSource(final Integer v, final Long e) {
		return e.longValue() >>> 32 == v.intValue();
	}

	@Override
	public boolean isSuccessor(final Integer x, final Integer y) {
		return isArc(y.intValue(), x.intValue());
	}

	@Override
	public int outDegree(final Integer x) {
		return graph.outdegree(x.intValue());
	}

	@Override
	public boolean containsEdge(final Long e) {
		return isArc((int)(e.longValue() >>> 32), e.intValue());
	}

	@Override
	public boolean containsVertex(final Integer x) {
		final int v = x.intValue();
		return v >= 0 && v < n;
	}

	@Override
	public int degree(final Integer x) {
		final int v = x.intValue();
		int self = 0;

		final LazyIntIterator succ = graph.successors(v);
		for(int s; (s = succ.nextInt()) != -1;) if (s == v) self++;

		return graph.outdegree(v) + (transpose.outdegree(v) - self);
	}

	@Override
	public Long findEdge(final Integer x, final Integer y) {
		if (! containsVertex(x) || ! containsVertex(y)) return null;
		final Long l = Long.valueOf(x.longValue() << 32 | y.intValue());
		return containsEdge(l) ? l : null;
	}

	@Override
	public Collection<Long> findEdgeSet(final Integer x, final Integer y) {
		final Long e = Long.valueOf(x.longValue() << 32 | y.intValue());
		return containsEdge(e) ? ObjectLists.singleton(e) : ObjectLists.emptyList();
	}

	@Override
	public EdgeType getDefaultEdgeType() {
		return EdgeType.DIRECTED;
	}

	@Override
	public int getEdgeCount() {
		return (int)graph.numArcs();
	}

	@Override
	public int getEdgeCount(final EdgeType x) {
		return EdgeType.DIRECTED.equals(x) ? getEdgeCount() : 0;
	}

	@Override
	public EdgeType getEdgeType(final Long e) {
		return EdgeType.DIRECTED;
	}

	@Override
	public Collection<Long> getEdges() {
		final ObjectArrayList<Long> edges = new ObjectArrayList<>();
		final NodeIterator iterator = graph.nodeIterator();
		for(int i = n; i-- != 0;) {
			final int x = iterator.nextInt();
			final long xShifted = (long)x << 32;
			final int d = iterator.outdegree();
			final int[] s = iterator.successorArray();
			for(int j = 0; j < d; j++) edges.add(Long.valueOf(xShifted | s[j]));
		}
		return edges;
	}

	@Override
	public Collection<Long> getEdges(final EdgeType x) {
		return EdgeType.DIRECTED.equals(x) ? getEdges() : ObjectLists.emptyList();
	}

	@Override
	public int getIncidentCount(final Long e) {
		return e.intValue() != e.longValue() >>> 32 ? 2 : 1;
	}

	@Override
	public Collection<Long> getIncidentEdges(final Integer x) {
		final int v = x.intValue();
		final long vShifted = (long)v << 32;
		final int outdegree = graph.outdegree(v);
		final int indegree = transpose.outdegree(v);
		final LazyIntIterator succ = graph.successors(v);
		final LazyIntIterator pred = transpose.successors(v);

		final ObjectArrayList<Long> res = new ObjectArrayList<>(outdegree + indegree);
		for(int s; (s = succ.nextInt()) != -1;) res.add(Long.valueOf(vShifted | s));
		// We do not add loops again.
		for(int p; (p = pred.nextInt()) != -1;) if (p != v) res.add(Long.valueOf((long)p << 32 | v));
		return res;
	}

	@Override
	public Collection<Integer> getIncidentVertices(final Long e) {
		final int x = (int)(e.longValue() >>> 32);
		final int y = e.intValue();
		if (x == y) return ObjectLists.singleton(Integer.valueOf(x));
		final ObjectArrayList<Integer> res = new ObjectArrayList<>();
		res.add(Integer.valueOf(x));
		res.add(Integer.valueOf(y));
		return res;
	}

	@Override
	public int getNeighborCount(final Integer x) {
		return getNeighbors(x).size();
	}

	@Override
	public Collection<Integer> getNeighbors(final Integer x) {
		final int v = x.intValue();
		final int outdegree = graph.outdegree(v);
		final int indegree = transpose.outdegree(v);
		final LazyIntIterator succ = graph.successors(v);
		final LazyIntIterator pred = transpose.successors(v);

		final ObjectOpenHashSet<Integer> res = new ObjectOpenHashSet<>(outdegree + indegree);
		for(int s; (s = succ.nextInt()) != -1;) res.add(Integer.valueOf(s));
		for(int p; (p = pred.nextInt()) != -1;) res.add(Integer.valueOf(p));
		return res;
	}

	@Override
	public int getVertexCount() {
		return n;
	}

	@Override
	public Collection<Integer> getVertices() {
		return new AbstractObjectList<>() {
			@Override
			public Integer get(final int x) {
				return Integer.valueOf(x);
			}
			@Override
			public int size() {
				return n;
			}
		};
	}

	@Override
	public boolean isIncident(final Integer x, final Long e) {
		final int v = x.intValue();
		return e.intValue() == v || e.longValue() >>> 32 == v;
	}

	@Override
	public boolean isNeighbor(final Integer x, final Integer y) {
		final int v = x.intValue();
		final int w = y.intValue();

		final LazyIntIterator succ = graph.successors(v);
		for(int s; (s = succ.nextInt()) != -1;) if (s == w) return true;
		final LazyIntIterator pred = transpose.successors(v);
		for(int p; (p = pred.nextInt()) != -1;) if (p == w) return true;
		return false;
	}


	/** @throws UnsupportedOperationException */
	@Override
	public boolean removeEdge(final Long e) {
		throw new UnsupportedOperationException();
	}

	/** @throws UnsupportedOperationException */
	@Override
	public boolean removeVertex(final Integer x) {
		throw new UnsupportedOperationException();
	}


	/** @throws UnsupportedOperationException */
	@Override
	public boolean addEdge(final Long e, final Integer y, final Integer arg2) {
		throw new UnsupportedOperationException();
	}

	/** @throws UnsupportedOperationException */
	@Override
	public boolean addEdge(final Long e, final Integer y, final Integer arg2, final EdgeType arg3) {
		throw new UnsupportedOperationException();
	}

	/** @throws UnsupportedOperationException */
	@Override
	public boolean addEdge(final Long e, final Collection<? extends Integer> y) {
		throw new UnsupportedOperationException();
	}

	/** @throws UnsupportedOperationException */
	@Override
	public boolean addEdge(final Long e, final Collection<? extends Integer> y, final EdgeType arg2) {
		throw new UnsupportedOperationException();
	}

	/** @throws UnsupportedOperationException */
	@Override
	public boolean addVertex(final Integer x) {
		throw new UnsupportedOperationException();
	}

	public static void main(final String[] arg) throws IOException, JSAPException {
		final SimpleJSAP simpleJSAP = new SimpleJSAP(JungAdapter.class.getName(), "Reads a graph with a given basename, optionally its transpose, and writes it on standard output in Pajek format.",
				new Parameter[] {
					new Switch("offline", 'o', "offline", "Use the offline load method to reduce memory consumption. It usually works, but your mileage may vary."),
					new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the source graph."),
					new UnflaggedOption("transpose", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose. If unspecified, the JungAdapter constructor will be provided with null as a parameter. This usually works, but your mileage may vary.")
		});

		final JSAPResult jsapResult = simpleJSAP.parse(arg);
		if (simpleJSAP.messagePrinted()) System.exit(1);
		final boolean offline = jsapResult.userSpecified("offline");

		final ImmutableGraph graph = offline ? ImmutableGraph.loadOffline(jsapResult.getString("basename")) : ImmutableGraph.load(jsapResult.getString("basename"));
		final ImmutableGraph transpose = jsapResult.userSpecified("transpose") ? (offline ? ImmutableGraph.loadOffline(jsapResult.getString("transpose")) : ImmutableGraph.load(jsapResult.getString("transpose"))) : null;

		final PrintWriter printWriter = new PrintWriter(System.out);
		new PajekNetWriter<Integer, Long>().save(new JungAdapter(graph, transpose), printWriter);
		printWriter.flush();
	}
}
