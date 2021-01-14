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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/** Outputs (onto stdout) an EPS image showing a sketch of the graph adjacency matrix.
 */


public class SmatToPS {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	@SuppressWarnings("boxing")
	public static void main(String arg[]) throws IOException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP(SmatToPS.class.getName(),
				"Outputs a given sparse SQUARE matrix (in SMAT format) to an EPS image.",
				new Parameter[] {
		}
		);

		@SuppressWarnings("unused")
		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);


		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String s[] = br.readLine().split(" "), line;
		int n = Integer.parseInt(s[0]);

		System.out.printf("%%!PS-Adobe-3.0 EPSF-3.0\n%%%%Creator: Webgraph\n%%%%Title: Webgraph\n%%%%CreationDate: %TD\n%%%%DocumentData: Clean7Bit\n%%%%Origin: 0 0\n%%%%BoundingBox: 0 0 %d %d\n%%%%LanguageLevel: 2 \n%%%%Pages: 1\n%%%%Page: 1 1\n",
				new Date(),
				n,
				n);


		while ((line = br.readLine()) != null) {
			s = line.split(" ");
			int x = Integer.parseInt(s[0]);
			int y = Integer.parseInt(s[1]);
			double g = Double.parseDouble(s[2]);
			System.out.printf("newpath\n%d %d moveto\n%d %d lineto\n%d %d lineto\n%d %d lineto\nclosepath\n%.3f setgray\nfill\n",
					y, n - x - 1, y + 1, n - x - 1, y + 1, n - x, y, n - x,
					(g >= 1 ? 0.0 : Math.exp(-3000 * ((1.0 / (1 - g) - 1)))));
		}
		System.out.printf("%%%%EOF\n");

	}
}
