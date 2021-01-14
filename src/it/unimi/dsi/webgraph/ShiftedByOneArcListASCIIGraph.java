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
import java.io.IOException;
import java.io.InputStream;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.logging.ProgressLogger;

/** An {@link ArcListASCIIGraph} with fixed shift -1. Very useful to read
 * graphs specified as pairs of arcs with node numbering starting from one.
 *
 *  <h2>Using {@link ArcListASCIIGraph} with MatLab-like sparse matrix files</h2>
 *
 *  <p>The main intended usage of this class is that of interfacing easily with MatLab-like
 *  sparse matrix files. Note that for this to happen it is necessary to shift by one all
 *  indices. Assume you have a file named <code>example.arcs</code>:
 *  <pre>
 *  1 2
 *  2 3
 *  3 2
 *  </pre>
 *  Then, the command
 *  <pre>
 *  java it.unimi.dsi.webgraph.BVGraph -1 -g ShiftedByOneArcListASCIIGraph dummy bvexample &lt;example.arcs
 *  </pre>
 *  will generate a {@link BVGraph} as expected (e.g, there is an arc from 0 to 1).
 */

public final class ShiftedByOneArcListASCIIGraph extends ArcListASCIIGraph {

	protected ShiftedByOneArcListASCIIGraph(final InputStream is, final int shift) throws NumberFormatException, IOException {
		super(is, shift);
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
		return new ArcListASCIIGraph(is, -1);
	}

	public static ImmutableGraph load(final CharSequence basename) throws IOException {
		return load(basename, null);
	}

	public static ImmutableGraph load(final CharSequence basename, final ProgressLogger unused) throws IOException {
		return new ArrayListMutableGraph(loadOnce(new FastBufferedInputStream(new FileInputStream(basename.toString())))).immutableView();
	}

	public static void store(final ImmutableGraph graph, final CharSequence basename, final ProgressLogger unused) throws IOException {
		store(graph, basename, 1);
	}

	public static void main(final String arg[]) throws NoSuchMethodException {
		throw new NoSuchMethodException("Please use the main method of " + ArcListASCIIGraph.class.getSimpleName() + ".");
	}
}
