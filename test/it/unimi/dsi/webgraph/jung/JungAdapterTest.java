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

package it.unimi.dsi.webgraph.jung;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Test;

import edu.uci.ics.jung.graph.util.EdgeType;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;



public class JungAdapterTest {

	@Test
	public void testSmall() {
		final ImmutableGraph g = new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 1 }, { 1, 2 } }).immutableView();
		final JungAdapter j = new JungAdapter(g, Transform.transpose(g));

		final Integer zero = Integer.valueOf(0);
		final Integer one = Integer.valueOf(1);
		final Integer two = Integer.valueOf(2);
		for(final Long e: j.getOutEdges(zero)) {
			assertEquals(zero, j.getSource(e));
			assertEquals(one, j.getDest(e));
		}
		for(final Long e: j.getInEdges(two)) {
			assertEquals(one, j.getSource(e));
			assertEquals(two, j.getDest(e));
		}

		assertEquals(one, j.getOpposite(zero, j.getOutEdges(zero).iterator().next()));
	}

	@Test
	public void test() {
		final ImmutableGraph graph = new ArrayListMutableGraph(5, new int[][] {
				{ 0, 1 },
				{ 1, 0 },
				{ 1, 2 },
				{ 2, 2 },
				{ 2, 3 },
				{ 3, 0 }
				}).immutableView();
		final ImmutableGraph transpose = Transform.transpose(graph);
		final JungAdapter jungAdapter = new JungAdapter(graph, transpose);

		// Test number of nodes and arcs
		assertEquals(graph.numNodes(), jungAdapter.getVertexCount());
		assertEquals(graph.numArcs(), jungAdapter.getEdgeCount());
		assertEquals(EdgeType.DIRECTED, jungAdapter.getDefaultEdgeType());
		assertEquals(graph.numArcs(), jungAdapter.getEdgeCount(EdgeType.DIRECTED));
		assertEquals(0, jungAdapter.getEdgeCount(EdgeType.UNDIRECTED));
		assertEquals(0, jungAdapter.getEdges(EdgeType.UNDIRECTED).size());

		// Test vertices
		final Collection<Integer> vertices = jungAdapter.getVertices();
		assertEquals(graph.numNodes(), vertices.size());
		for (int i = 0; i < graph.numNodes(); i++) assertTrue(vertices.contains(Integer.valueOf(i)));

		// Test presence / absence of all arcs and nodes
		final Collection<Long> edges = jungAdapter.getEdges();
		assertEquals(graph.numArcs(), edges.size());
		assertEquals(edges, jungAdapter.getEdges(EdgeType.DIRECTED));
		for (int source = 0; source < graph.numNodes(); source++) {
			final Integer sourceVertex = Integer.valueOf(source);
			assertTrue(jungAdapter.containsVertex(sourceVertex));
			final boolean pOut[] = new boolean[graph.numNodes()];
			final boolean pIn[] = new boolean[graph.numNodes()];
			final int outDeg = graph.outdegree(source);
			final int succ[] = graph.successorArray(source);
			final int inDeg = transpose.outdegree(source);
			final int pred[] = transpose.successorArray(source);
			for (int i = 0; i < outDeg; i++) pOut[succ[i]] = true;
			for (int i = 0; i < inDeg; i++) pIn[pred[i]] = true;
			final Collection<Long> incidentEdges = jungAdapter.getIncidentEdges(sourceVertex);
			final Collection<Integer> neighbors = jungAdapter.getNeighbors(sourceVertex);
			assertEquals(neighbors.size(), jungAdapter.getNeighborCount(sourceVertex));

			// Test outedges
			final Collection<Long> outEdges = jungAdapter.getOutEdges(sourceVertex);
			final Collection<Integer> successors = jungAdapter.getSuccessors(sourceVertex);
			assertEquals(outDeg, outEdges.size());
			assertEquals(outDeg, successors.size());
			int countOut = outDeg;
			for (final Long e: outEdges) {
				assertEquals(sourceVertex, jungAdapter.getSource(e));
				final int dest = jungAdapter.getDest(e).intValue();
				assertTrue(pOut[dest]);
				assertTrue(successors.contains(Integer.valueOf(dest)));
				successors.remove(Integer.valueOf(dest));
				assertTrue(incidentEdges.contains(e));
				assertTrue(neighbors.contains(Integer.valueOf(dest)));
				if (source != dest) incidentEdges.remove(e);
				neighbors.remove(Integer.valueOf(dest));
				countOut--;
			}
			assertEquals(0, countOut);

			// Test inedges
			final Collection<Long> inEdges = jungAdapter.getInEdges(sourceVertex);
			final Collection<Integer> predecessors = jungAdapter.getPredecessors(sourceVertex);
			assertEquals(inDeg, inEdges.size());
			assertEquals(inDeg, predecessors.size());
			int countIn = inDeg;
			for (final Long e: inEdges) {
				assertEquals(sourceVertex, jungAdapter.getDest(e));
				final int src = jungAdapter.getSource(e).intValue();
				assertTrue(pIn[src]);
				assertTrue(predecessors.contains(Integer.valueOf(src)));
				predecessors.remove(Integer.valueOf(src));
				assertTrue(incidentEdges.contains(e));
				incidentEdges.remove(e);
				if (! pOut[src]) {
					assertTrue(neighbors.contains(Integer.valueOf(src))); // Because if source->src is an arc and also src->source is an arc, src was already removed in the previous cycle
					neighbors.remove(Integer.valueOf(src));
				}
				countIn--;
			}
			assertEquals(0, countIn);

			assertEquals(0, incidentEdges.size());
			assertEquals(0, neighbors.size());
			if (pOut[source]) assertEquals(inDeg + outDeg - 1, jungAdapter.degree(Integer.valueOf(source)));
			else assertEquals(inDeg + outDeg, jungAdapter.degree(Integer.valueOf(source)));


			assertEquals(outDeg, jungAdapter.getSuccessorCount(sourceVertex));
			assertEquals(outDeg, jungAdapter.outDegree(sourceVertex));
			assertEquals(transpose.outdegree(source), jungAdapter.getPredecessorCount(sourceVertex));
			assertEquals(transpose.outdegree(source), jungAdapter.inDegree(sourceVertex));

			// Test contains edge
			for (int target = 0; target < graph.numNodes(); target++) {
				final Long edge = Long.valueOf((long)source << 32 | target);
				final Integer targetVertex = Integer.valueOf(target);
				if (pOut[target]) {
					assertTrue(jungAdapter.isPredecessor(sourceVertex, targetVertex));
					assertTrue(jungAdapter.isSuccessor(targetVertex, sourceVertex));
					assertTrue(jungAdapter.containsEdge(edge));
					assertEquals(EdgeType.DIRECTED, jungAdapter.getEdgeType(edge));
					final Long foundEdge = jungAdapter.findEdge(Integer.valueOf(source), Integer.valueOf(target));
					assertEquals(edge, foundEdge);
					assertEquals(sourceVertex, jungAdapter.getEndpoints(edge).getFirst());
					assertEquals(targetVertex, jungAdapter.getEndpoints(edge).getSecond());
					assertEquals(targetVertex, jungAdapter.getOpposite(sourceVertex, edge));
					assertEquals(sourceVertex, jungAdapter.getOpposite(targetVertex, edge));
					assertTrue(jungAdapter.isSource(sourceVertex, edge));
					assertTrue(! jungAdapter.isSource(Integer.valueOf(source + 1), edge));
					assertTrue(jungAdapter.isDest(targetVertex, edge));
					assertTrue(! jungAdapter.isDest(Integer.valueOf(target + 1), edge));
					assertEquals(sourceVertex ,jungAdapter.getSource(edge));
					assertEquals(targetVertex ,jungAdapter.getDest(edge));
					final Collection<Long> edgeSet = jungAdapter.findEdgeSet(sourceVertex, targetVertex);
					assertEquals(1, edgeSet.size());
					assertEquals(edge, edgeSet.iterator().next());
					assertTrue(edges.contains(edge));
					edges.remove(edge);
					final Collection<Integer> incidentVertices = jungAdapter.getIncidentVertices(edge);
					if (source != target) {
						assertEquals(2, jungAdapter.getIncidentCount(edge));
						assertEquals(2, incidentVertices.size());
					}
					else {
						assertEquals(1, jungAdapter.getIncidentCount(edge));
						assertEquals(1, incidentVertices.size());
					}
					assertTrue(incidentVertices.contains(sourceVertex));
					assertTrue(incidentVertices.contains(targetVertex));
					assertTrue(jungAdapter.isIncident(sourceVertex, edge));
					assertTrue(jungAdapter.isIncident(targetVertex, edge));
					assertTrue(jungAdapter.isNeighbor(sourceVertex, targetVertex));
					assertTrue(jungAdapter.isNeighbor(targetVertex, sourceVertex));
				}
				else {
					assertTrue(! jungAdapter.containsEdge(edge));
					assertEquals(null, jungAdapter.findEdge(Integer.valueOf(source), Integer.valueOf(target)));
					if (! pIn[target]) { // Means that source->target and target->source both fail to exist
						assertTrue(! jungAdapter.isNeighbor(sourceVertex, targetVertex));
						assertTrue(! jungAdapter.isNeighbor(targetVertex, sourceVertex));
					}
				}
			}
		}
		assertTrue(! jungAdapter.containsVertex(Integer.valueOf(-1)));
		assertTrue(! jungAdapter.containsVertex(Integer.valueOf(graph.numNodes() + 1)));
	}


}
