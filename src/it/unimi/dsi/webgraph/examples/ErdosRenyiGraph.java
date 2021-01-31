/*
 * Copyright (C) 2010-2021 Paolo Boldi and Sebastiano Vigna
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

package it.unimi.dsi.webgraph.examples;

import java.io.IOException;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandomGenerator;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.NodeIterator;

/** An Erd&#x151;s&ndash;R&eacute;nyi random graph: the number of nodes
 *  is fixed, and there is a fixed probability that an arc is put
 *  between any two nodes (independently for every pair).
 *
 *  <p>Note that an instance of this class is not {@linkplain ImmutableGraph#randomAccess() random-access}:
 *  you can, however, {@linkplain ArrayListMutableGraph#ArrayListMutableGraph(ImmutableGraph) make a mutable copy of the returned graph}
 *  and then {@linkplain ArrayListMutableGraph#immutableView() take its immutable view}.
 *
 *  <p><strong>Warning</strong>: From version 3.5.2, this classes uses {@link BinomialDistribution}
 *  instead of the previous COLT-based {@code Binomial} class. As a side-effect, the graphs generated
 *  with the same parameters will be different.
 */
public class ErdosRenyiGraph extends ImmutableSequentialGraph {

	private final static Logger LOGGER = LoggerFactory.getLogger(ErdosRenyiGraph.class);

	/** Number of nodes. */
	private final int n;
	/** Probability to put an arc between each pair of nodes. */
	private final double p;
	/** Whether loops should also be generated. */
	private final boolean loops;
	/** The random seed. */
	private final long seed;

	/** Creates an Erd&#x151;s&ndash;R&eacute;nyi graph with given parameters and random seed.
	 *
	 * @param n the number of nodes.
	 * @param p the probability of generating an arc.
	 * @param seed a seed for pseudorandom number generation.
	 * @param loops whether loops are allowed or not.
	 */
	public ErdosRenyiGraph(final int n, final double p, final long seed, final boolean loops) {
		this.n = n;
		this.p = p;
		this.loops = loops;
		this.seed = seed;
	}

	/** Creates an Erd&#x151;s&ndash;R&eacute;nyi graph with given parameters.
	 *
	 * @param n the number of nodes.
	 * @param p the probability of generating an arc.
	 * @param loops whether loops are allowed or not.
	 */
	public ErdosRenyiGraph(final int n, final double p, final boolean loops) {
		this(n, p, Util.randomSeed(), loops);
	}

	/** Creates an Erd&#x151;s&ndash;R&eacute;nyi graph with given parameters and no loops.
	 *
	 * @param n the number of nodes.
	 * @param p the probability of generating an arc.
	 */
	public ErdosRenyiGraph(final int n, final double p) {
		this(n, p, false);
	}

	/** Creates an Erd&#x151;s&ndash;R&eacute;nyi graph with given parameters and random seed.
	 *
	 * <p>This constructor can be used with an {@link ObjectParser}.
	 *
	 * @param n the number of nodes.
	 * @param p the probability of generating an arc.
	 * @param seed a seed for pseudorandom number generation.
	 * @param loops whether loops are allowed or not.
	 */
	public ErdosRenyiGraph(final String n, final String p, final String seed, final String loops) {
		this(Integer.parseInt(n), Double.parseDouble(p), Long.parseLong(seed), Boolean.parseBoolean(loops));
	}

	/** Creates an Erd&#x151;s&ndash;R&eacute;nyi graph with given parameters and no loops.
	 *
	 * <p>This constructor can be used with an {@link ObjectParser}.
	 *
	 * @param n the number of nodes.
	 * @param p the probability of generating an arc.
	 */
	public ErdosRenyiGraph(final String n, final String p) {
		this(Integer.parseInt(n), Double.parseDouble(p));
	}

	/** Creates an Erd&#x151;s&ndash;R&eacute;nyi graph with given parameters.
	 *
	 * <p>This constructor can be used with an {@link ObjectParser}.
	 *
	 * @param n the number of nodes.
	 * @param p the probability of generating an arc.
	 * @param loops whether loops are allowed or not.
	 */
	public ErdosRenyiGraph(final String n, final String p, final String loops) {
		this(Integer.parseInt(n), Double.parseDouble(p), Boolean.parseBoolean(loops));
	}

	/** Creates an Erd&#x151;s&ndash;R&eacute;nyi graph with given parameters and random seed.
	 *
	 * @param n the number of nodes.
	 * @param m the expected number of arcs.
	 * @param seed a seed for pseudorandom number generation.
	 * @param loops whether loops are allowed or not.
	 */
	public ErdosRenyiGraph(final int n, final long m, final long seed, final boolean loops) {
		this(n, (double)m / (loops? (long)n * n : (long)n * (n - 1)), seed, loops);
	}

	/** Creates an Erd&#x151;s&ndash;R&eacute;nyi graph with given parameters and random seed.
	 *
	 * @param n the number of nodes.
	 * @param m the expected number of arcs.
	 * @param loops whether loops are allowed or not.
	 */
	public ErdosRenyiGraph(final int n, final long m, final boolean loops) {
		this(n, m, Util.randomSeed(), loops);
	}

	@Override
	public int numNodes() {
		return n;
	}

	@Override
	public ErdosRenyiGraph copy() {
		return this;
	}

	@Override
	public NodeIterator nodeIterator() {
		return new NodeIterator() {
			private final XoRoShiRo128PlusRandomGenerator random = new XoRoShiRo128PlusRandomGenerator(seed);

			private final BinomialDistribution bg = new BinomialDistribution(random, n - (loops ? 0 : 1), p);

			private int outdegree;
			private int curr = -1;
			private final IntOpenHashSet successors = new IntOpenHashSet();
			private int[] successorArray = new int[1024];

			@Override
			public boolean hasNext() {
				return curr < n - 1;
			}

			@Override
			public int nextInt() {
				curr++;
				outdegree = bg.sample();
				successors.clear();
				if (! loops) successors.add(curr);
				for(int i = 0; i < outdegree; i++) while(! successors.add(random.nextInt(n)));
				if (! loops) successors.remove(curr);
				successorArray = IntArrays.grow(successorArray, outdegree);
				successors.toArray(successorArray);
				IntArrays.quickSort(successorArray, 0, outdegree);
				return curr;
			}

			@Override
			public int outdegree() {
				return outdegree;
			}

			@Override
			public int[] successorArray() {
				return successorArray;
			}

			@Override
			public NodeIterator copy(final int upperBound) {
				throw new UnsupportedOperationException();
			}
		};
	}

	/** Generates an Erd&#x151;s&ndash;R&eacute;nyi graph with the specified seed.
	 *
	 * <p>This method exists only for backward compatibility.
	 *
	 * @param seed the seed for random generation.
	 * @return the generated graph.
	 * @deprecated An instance of this class is already an {@link ImmutableSequentialGraph}.
	 */
	@Deprecated
	public ImmutableSequentialGraph generate(final long seed) {
		LOGGER.debug("Generating with probability " + p);

		return new ImmutableSequentialGraph() {
			@Override
			public int numNodes() {
				return n;
			}

			@Override
			public ImmutableSequentialGraph copy() {
				return this;
			}

			@Override
			public NodeIterator nodeIterator() {
				return new NodeIterator() {
					private final XoRoShiRo128PlusRandomGenerator random = new XoRoShiRo128PlusRandomGenerator(seed);

					private final BinomialDistribution bg = new BinomialDistribution(random, n - (loops ? 0 : 1), p);

					private int outdegree;
					private int curr = -1;
					private final IntOpenHashSet successors = new IntOpenHashSet();
					private int[] successorArray = new int[1024];

					@Override
					public boolean hasNext() {
						return curr < n - 1;
					}

					@Override
					public int nextInt() {
						curr++;
						outdegree = bg.sample();
						successors.clear();
						if (! loops) successors.add(curr);
						for(int i = 0; i < outdegree; i++) while(! successors.add(random.nextInt(n)));
						if (! loops) successors.remove(curr);
						successorArray = IntArrays.grow(successorArray, outdegree);
						successors.toIntArray(successorArray);
						IntArrays.quickSort(successorArray, 0, outdegree);
						return curr;
					}

					@Override
					public int outdegree() {
						return outdegree;
					}

					@Override
					public int[] successorArray() {
						return successorArray;
					}

					@Override
					public NodeIterator copy(final int upperBound) {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	/** Generates an Erd&#x151;s&ndash;R&eacute;nyi graph.
	 *
	 * <p>This method exists only for backward compatibility.
	 *
	 * @return the generated graph.
	 * @deprecated An instance of this class is already an {@link ImmutableSequentialGraph}.
	 */
	@Deprecated
	public ImmutableGraph generate() {
		return generate(Util.randomSeed());
	}


	public static void main(final String arg[]) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(ErdosRenyiGraph.class.getName(), "Generates an Erd\u0151s-R\u00E9nyi random graph and stores it as a BVGraph.",
				new Parameter[] {
			new Switch("loops", 'l', "loops", "Whether the graph should include self-loops."),
			new FlaggedOption("p", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "The probability of generating an arc."),
			new FlaggedOption("m", JSAP.LONGSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'm', "The expected number of arcs."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the output graph file."),
			new UnflaggedOption("n", JSAP.INTEGER_PARSER, JSAP.REQUIRED, "The number of nodes."),
		});
		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String baseName = jsapResult.getString("basename");
		final int n = jsapResult.getInt("n");
		final boolean loops = jsapResult.getBoolean("loops");

		if (jsapResult.userSpecified("p") && jsapResult.userSpecified("m")) {
			System.err.println("Options p and m cannot be specified together");
			System.exit(1);
		}
		if (! jsapResult.userSpecified("p") && ! jsapResult.userSpecified("m")) {
			System.err.println("Exactly one of the options p and m must be specified");
			System.exit(1);
		}

		BVGraph.store((jsapResult.userSpecified("p") ? new ErdosRenyiGraph(n, jsapResult.getDouble("p"), loops) : new ErdosRenyiGraph(n, jsapResult.getLong("m"), loops)), baseName, new ProgressLogger());
	}
}
