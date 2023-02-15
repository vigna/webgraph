package it.unimi.dsi.webgraph.scratch;

/*
 * Copyright (C) 2009-2023 Sebastiano Vigna
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


import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Scanner;

public class ConvertArcFile {
	@SuppressWarnings("unchecked")
	public static void main(String arg[]) throws IOException, ClassNotFoundException {
		Object2LongFunction<String> convert = (Object2LongFunction<String>)BinIO.loadObject(arg[0]);
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(new FastBufferedInputStream(System.in));
		PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
		scanner.useDelimiter("\t|\r|\n");
		while(scanner.hasNext()) {
			pw.print(convert.getLong(scanner.next()));
			pw.print('\t');
			pw.print(convert.getLong(scanner.next()));
			pw.println();
		}

		pw.close();
	}
}
