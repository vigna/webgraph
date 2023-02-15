package it.unimi.dsi.webgraph.scratch;

/*
 * Copyright (C) 2008-2023 Sebastiano Vigna
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


import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

import java.io.IOException;

public class PrintDeltas {
	public static void main(String arg[]) throws IOException {
		ImmutableGraph g = ImmutableGraph.load(arg[0]);
		int d, p, s, x;
		int[] a;
		NodeIterator n = g.nodeIterator();
		while(n.hasNext()) {
			x = n.nextInt();
			a = n.successorArray();
			d = n.outdegree();
			p = -1;
			for(int i = 0; i < d; i++) {
				s = a[i];
				if (p == -1) System.out.println(Fast.int2nat(s - x));
				else System.out.println(s - p - 1);
				p = s;
			}
		}
	}
}
