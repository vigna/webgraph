package it.unimi.dsi.webgraph.scratch;

/*
 * Copyright (C) 2015-2020 Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.AbstractLazyIntIterator;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.UnionImmutableGraph;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

public class MergeAndRenumber {
	private final static Logger LOGGER = LoggerFactory.getLogger(MergeAndRenumber.class);

	public static void main(final String[] arg) throws IOException, JSAPException {

		final SimpleJSAP jsap = new SimpleJSAP(MergeAndRenumber.class.getName(), "Merges multilingual Wikipedia graphs, making nodes associated with the same Wikipedia entity into a clique.",
			new Parameter[] {
			new UnflaggedOption("destBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The destination basename."),
			new UnflaggedOption("sourceBasenames", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The source basenames.")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String[] sourceBasename = jsapResult.getStringArray("sourceBasenames");
		final ImmutableGraph[] source = new ImmutableGraph[sourceBasename.length];
		for(int i = 0; i < sourceBasename.length; i++) source[i] = ImmutableGraph.loadMapped(sourceBasename[i]);

		final int[] offsets = new int[source.length + 1];
		for(int i = 0; i < offsets.length - 1; i++) offsets[i + 1] = offsets[i] + source[i].numNodes();
		final int allNodes = offsets[offsets.length - 1];

		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");
		progressLogger.displayLocalSpeed = true;
		progressLogger.expectedUpdates = allNodes;

		progressLogger.start("Building maps...");
		final int[][] graphId2WikidataId = new int[source.length][];
		final Int2IntOpenHashMap[] wikidataId2GraphId = new Int2IntOpenHashMap[source.length];
		for(int i = 0; i < sourceBasename.length; i++) {
			final int n = source[i].numNodes();
			Int2IntOpenHashMap m = new Int2IntOpenHashMap(n);
			m.defaultReturnValue(-1);
			int[] a = new int[n];
			IntIterator newIds = TextIO.asIntIterator(sourceBasename[i] + ".wikidataid");
			for(int j = 0; j < n; j++) {
				a[j] = newIds.nextInt();
				m.put(a[j], j);
				progressLogger.lightUpdate();
			}
			wikidataId2GraphId[i] = m;
			graphId2WikidataId[i] = a;
		}
		progressLogger.done();

		BVGraph.store(new UnionImmutableGraph(new ImmutableSequentialGraph() {
			@Override
			public int numNodes() {
				return allNodes;
			}

			@Override
			public NodeIterator nodeIterator() {
				return new NodeIterator() {
					int curr = 0;
					int offset = 0;
					int currGraph = 0;

					NodeIterator currIterator = source[0].nodeIterator();

					@Override
					public boolean hasNext() {
						return curr < allNodes;
					}

					@Override
					public int nextInt() {
						if (! hasNext()) throw new NoSuchElementException();
						if (! currIterator.hasNext()) {
							currIterator = source[++currGraph].nodeIterator();
							offset = curr;
							assert offset == offsets[currGraph];
						}
						currIterator.nextInt();
						return curr++;
					}

					@Override
					public LazyIntIterator successors() {
						return new AbstractLazyIntIterator() {
							LazyIntIterator successors = currIterator.successors();
							@Override
							public int nextInt() {
								int s = successors.nextInt();
								return s == -1 ? -1 : s + offset;
							}
						};
					}

					@Override
					public int outdegree() {
						return currIterator.outdegree();
					}

					@Override
					public NodeIterator copy(int upperBound) {
						throw new UnsupportedOperationException();
					}
				};
			}


		},
		new ImmutableGraph() {

			@Override
			public int numNodes() {
				return allNodes;
			}

			@Override
			public boolean randomAccess() {
				return true;
			}

			@Override
			public int outdegree(final int x) {
				// Ugly and inefficient but simple
				return successorArray(x).length;
			}

			@Override
			public int[] successorArray(final int x) {
				final IntArrayList successors = new IntArrayList();
				int g = IntArrays.binarySearch(offsets, x);
				if (g < 0) g = -g - 2;
				final int wikidataId = graphId2WikidataId[g][x - offsets[g]];
				if (wikidataId >= 1 << 24) return IntArrays.EMPTY_ARRAY;
				for(int i = 0; i < source.length; i++) {
					if (i == g) continue;
					final int graphId = wikidataId2GraphId[i].get(wikidataId);
					if (graphId != -1) successors.add(offsets[i] + graphId);
				}

				return successors.toIntArray();
			}

			@Override
			public ImmutableGraph copy() {
				throw new UnsupportedOperationException();
			}

		}
		),
		jsapResult.getString("destBasename"), progressLogger);
	}
}
