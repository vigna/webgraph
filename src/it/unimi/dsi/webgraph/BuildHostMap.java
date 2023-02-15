/*
 * Copyright (C) 2008-2023 Sebastiano Vigna
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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.net.InternetDomainName;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.logging.ProgressLogger;

/** A class computing host-related data given a list of URLs (usually, the URLs of the nodes of a web graph).
 * All processing is performed by the static utility method {@link #run(BufferedReader, PrintStream, DataOutputStream, DataOutputStream, boolean, ProgressLogger)}.
 *
 * <p><strong>Warning:</strong> this class provides a main method that saves the host list to standard output, but it
 * does some logging, too, so be careful not to log to standard output.
 *
 * @author Sebastiano Vigna
 */
public class BuildHostMap {
	private final static Logger LOGGER = LoggerFactory.getLogger(BuildHostMap.class);

	public static final Pattern DOTTED_ADDRESS = Pattern.compile("(([0-9A-Fa-f]+[:])*[0-9A-Fa-f]+)|((((0x[0-9A-Fa-f]+)|([0-9]+))\\.)*((0x[0-9A-Fa-f]+)|([0-9]+)))");

	/** This method reads URLs and writes hosts (or, possibly, top private domains), together with a map
	 * from URLs to hosts and a host count.
	 *
	 * @param br the buffered reader returning the list of URLs.
	 * @param hosts the print stream where hosts will be printed.
	 * @param mapDos the data output stream where the map from URLs to hosts will be written (one integer per URL).
	 * @param countDos the data output stream where the host counts will be written (one integer per host).
	 * @param topPrivateDomain if true, we use {@link InternetDomainName#topPrivateDomain()} to map to top private domains, rather than hosts.
	 * @param pl a progress logger, or {@code null}.
	 */
	public static void run(final BufferedReader br, final PrintStream hosts, final DataOutputStream mapDos, final DataOutputStream countDos, final boolean topPrivateDomain, final ProgressLogger pl) throws IOException, URISyntaxException {
		final Object2IntOpenHashMap<String> map = new Object2IntOpenHashMap<>();
		int[] count = new int[1024];
		map.defaultReturnValue(-1);
		int hostIndex = -1;

		if (pl != null) pl.start("Reading URLS...");
		for(String s, name; (s = br.readLine()) != null;) {
			final URI uri = new URI(s);
			name = uri.getHost();
			if (name == null) throw new IllegalArgumentException();
			if (topPrivateDomain) {
				if (! DOTTED_ADDRESS.matcher(name).matches()) {
					final InternetDomainName idn = InternetDomainName.from(name);
					if (idn.isUnderPublicSuffix()) name = idn.topPrivateDomain().toString();
				}
			}

			if ((hostIndex = map.getInt(name)) == -1) {
				hosts.println(name);
				map.put(name, hostIndex = map.size());
			}
			mapDos.writeInt(hostIndex);
			count = IntArrays.grow(count, hostIndex + 1);
			count[hostIndex]++;
			if (pl != null) pl.lightUpdate();
		}

		BinIO.storeInts(count, 0, map.size(), countDos);
		if (pl != null) pl.done();
	}


	public static void main(final String[] arg) throws IOException, JSAPException, URISyntaxException {

		final SimpleJSAP jsap = new SimpleJSAP(BuildHostMap.class.getName(), "Reads a list of URLs from standard input, computes the host map and counts and saves the host list to standard output.",
			new Parameter[] {
			new Switch("topPrivateDomain", 't', "top-private-domain", "Use top private domains instead of hosts."),
			new UnflaggedOption("map", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the host map will be stored as a list of integers in DataOutput format."),
			new UnflaggedOption("counts", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the host count will be stored as a list of integers in DataOutput format.")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);
		final BufferedReader fbr = new BufferedReader(new InputStreamReader(System.in, Charsets.ISO_8859_1));
		final DataOutputStream mapDos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(jsapResult.getString("map"))));
		final DataOutputStream countDos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(jsapResult.getString("counts"))));
		run(fbr, System.out, mapDos, countDos, jsapResult.getBoolean("topPrivateDomain"), new ProgressLogger(LOGGER));
		mapDos.close();
		countDos.close();
	}
}
