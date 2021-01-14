/*
   Copyright (C) 2010-2020 Sebastiano Vigna

   ImmutableCompressedGraph

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.


 */

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.uniroma3.dia.gc.CompressedGraph;

import java.io.IOException;
import java.util.Arrays;

public final class ImmutableCompressedGraph extends ImmutableGraph {

	@Override
	public int[] successorArray(int x) {
		final int[] succ = graph.getSuccessors(x);
		Arrays.sort(succ);
		return succ;
	}

	private CompressedGraph graph;

	public ImmutableCompressedGraph(String name) throws Exception {
		graph = new CompressedGraph(name);
	}

	@Override
	public ImmutableGraph copy() {
		return null;
	}

	@Override
	public int numNodes() {
		return graph.getVertexCount();
	}

	@Override
	public long numArcs() {
		return graph.getEdgeCount();
	}

	@Override
	public int outdegree(int x) {
		return graph.outDegree(x);
	}

	@Override
	public boolean randomAccess() {
		return true;
	}

	public static ImmutableGraph load(CharSequence basename) throws IOException {
		try {
			return new ImmutableCompressedGraph(basename.toString());
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static ImmutableGraph load(CharSequence basename, ProgressLogger pl) throws IOException {
		return load(basename);
	}
}
