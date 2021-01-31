package it.unimi.dsi.webgraph.cosin;

/*
 * Copyright (C) 2016-2021 Sebastiano Vigna
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.AbstractLazyIntIterator;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;


/** Exposes a COSIN graph as an (offline-only) immutable graph.
 *
 * <P>This class implements just {@link #nodeIterator()}, and cannot not be used for random or sequential access.
 *
 * <P>Note that the COSIN graph must be contained in a single file (you can simply <code>cat</code>
 * the various pieces if necessary). Moreover, this class assumes that successors lists are
 * stored contiguously and in order in the successors file.
 */

public class CosinGraph extends ImmutableSequentialGraph {

	public final static boolean ASSERTS = true;

	/** The base name of the COSIN graph. */
	final String basename;
	/** The number of nodes. */
	final int numNodes;

	private CosinGraph(final CharSequence basename) {
		this.basename = basename.toString();
		numNodes = (int)(new File(basename + ".info").length() / (6 * 4));
	}

	@Override
	public int numNodes() {
		return numNodes;
	}

	@Override
	public NodeIterator nodeIterator(final int from) {
		return new NodeIterator() {
			final int n = numNodes();
			final ByteBuffer infoByte, succByte;
			final FileChannel infoChannel, succChannel;
			int curr = -1, outdegree;
			int successorsArray[] = IntArrays.EMPTY_ARRAY;

			IntBuffer infoInt, succInt;

			{
				try {
					if (from != 0) throw new UnsupportedOperationException();
					@SuppressWarnings("resource")
					RandomAccessFile f = new RandomAccessFile(basename + ".info", "r");
					infoChannel = f.getChannel();
					f = new RandomAccessFile(basename + ".succ", "r");
					succChannel = f.getChannel();
					infoByte = ByteBuffer.allocateDirect(6 * 1024 * 1024); // So buffer empties at node boundaries
					succByte = ByteBuffer.allocateDirect(8 * 1024 * 1024);
					infoChannel.read(infoByte);
					succChannel.read(succByte);
					infoByte.flip();
					succByte.flip();
					infoInt = infoByte.order(ByteOrder.nativeOrder()).asIntBuffer();
					succInt = succByte.order(ByteOrder.nativeOrder()).asIntBuffer();
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
			}


			@Override
			public int nextInt() {
				if (! hasNext()) throw new java.util.NoSuchElementException();
				infoInt.get(); // Throw away indegree
				outdegree = infoInt.get();
				infoInt.get();
				infoInt.get();
				infoInt.get();
				infoInt.get(); // Skip offsets for outgoing arcs as an int
				if (! infoInt.hasRemaining()) try {
					infoByte.clear();
					infoChannel.read(infoByte);
					infoByte.flip();
					infoInt = infoByte.order(ByteOrder.nativeOrder()).asIntBuffer();
				}
				catch(final IOException e) {
					throw new RuntimeException(e);
				}

				return ++curr;
			}

			@Override
			public boolean hasNext() {
				return (curr < n - 1);
			}

			@Override
			public LazyIntIterator successors() {
				if (curr == from - 1) throw new IllegalStateException();
				return new AbstractLazyIntIterator() {
					int i = 0;

					@Override
					public int nextInt() {
						if (i == outdegree) return -1;
						i++;

						if (! succInt.hasRemaining()) try {
							succByte.clear();
							succChannel.read(succByte);
							succByte.flip();
							succInt = succByte.order(ByteOrder.nativeOrder()).asIntBuffer();
						}
						catch(final IOException e) {
							throw new RuntimeException(e);
						}

						return succInt.get();
					}
				};
			}

			@Override
			public int[] successorArray() {
				if (curr == from - 1) throw new IllegalStateException();
				successorsArray = IntArrays.ensureCapacity(successorsArray, outdegree,  0);

				if (outdegree <= succInt.remaining())	succInt.get(successorsArray, 0, outdegree);
				else try {
					final int remaining = succInt.remaining();
					succInt.get(successorsArray, 0, remaining);
					succByte.clear();
					succChannel.read(succByte);
					succByte.flip();
					succInt = succByte.order(ByteOrder.nativeOrder()).asIntBuffer();
					succInt.get(successorsArray, remaining, outdegree - remaining);
				}
				catch(final IOException e) {
					throw new RuntimeException(e);
				}

				return successorsArray;
			}

			@Override
			public int outdegree() {
				if (curr == from - 1) throw new IllegalStateException();
				return outdegree;
			}

			@Override
			public NodeIterator copy(final int upperBound) {
				throw new UnsupportedOperationException();
			}
		};
	}

	public static ImmutableGraph load(final CharSequence basename, final ProgressLogger pm) {
		throw new UnsupportedOperationException("COSIN graphs may be loaded offline only");
	}
}
