package it.unimi.dsi.webgraph.scratch;

/*
 * Copyright (C) 2007-2020 Sebastiano Vigna
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


import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

import java.io.IOException;

public class ComputeClassSizes {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	public static void main(String arg[]) throws IOException {
		ImmutableGraph g = ImmutableGraph.load(arg[0]);
		int n = g.numNodes(), r, s, c = 1;
		LazyIntIterator a, b;
		for(int i = 1; i < n; i++) {
			a = g.successors(i - 1);
			b = g.successors(i);
			while((r = a.nextInt()) == (s = b.nextInt()) && r != -1);
			if (s == r) c++;
			else {
				System.out.println(c);
				c = 1;
			}
		}

		System.out.println(c);
	}
}
