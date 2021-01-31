/*
 * Copyright (C) 2010-2021 Sebastiano Vigna
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


/** A bidirectional chain of cliques.
 *
 * @author Sebastiano Vigna
 */

public final class CliqueGraph extends ImmutableGraph {
	/** The number of nodes in the graph. */
	private final int n;
	/** The number of elements per clique. */
	private final int c;

	/** Creates a new bidirectional chain of cliques of given size.
	 *
	 * @param n the overall number of nodes (will be rounded down to the nearest multiple of <code>c</code>).
	 * @param c the size of each clique.
	 */
	public CliqueGraph(final int n, final int c) {
		this.n = n - n % c;
		this.c = c;

	}

	/** Creates a new clique of given size.
	 *
	 * @param n the size of the clique.
	 */
	public CliqueGraph(final int n) {
		this(n, n);
	}

	/** Creates a new bidirectional chain of cliques of given size.
	 *
	 * @param n the overall number of nodes (will be rounded down to the nearest multiple of <code>c</code>).
	 * @param c the size of each clique.
	 */
	public CliqueGraph(final String n, final String c) {
		this(Integer.parseInt(n), Integer.parseInt(c));
	}

	/** Creates a new clique of given size.
	 *
	 * @param n the size of the clique.
	 */
	public CliqueGraph(final String n) {
		this(Integer.parseInt(n));
	}

	@Override
	public ImmutableGraph copy() {
		return this;
	}

	@Override
	public int numNodes() {
		return n;
	}

	@Override
	public long numArcs() {
		return (long)n * c - n + (n != c ? 2 * (n / c) : 0);
	}

	@Override
	public int outdegree(final int x) {
		return c - 1 + (x % c == 0 && n != c ? 2 : 0);
	}

	@Override
	public boolean randomAccess() {
		return true;
	}

	@Override
	public int[] successorArray(final int x) {
		final int[] succ = new int[outdegree(x)];
		final int start = x - x % c;
		if (succ.length == c - 1) {
			for(int i = 0, j = 0; i < c; i++) if (start+ i != x) succ[j++] = start + i;
		}
		else {
			succ[0] = (x - c + n) % n;
			for(int i = 0, j = 1; i < c; i++) if (start + i != x) succ[j++] = start + i;
			succ[c] = (x + c) % n;
		}

		return succ;
	}

}
