package it.unimi.dsi.webgraph.scratch;

/*
 * Copyright (C) 2010-2021 Sebastiano Vigna
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

import it.unimi.dsi.Util;
import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.stat.SummaryStats;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;

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

/** Simulates the push-pull gossiping algorithm with a single source on a given <em>undirected</em> graph. The algorithm works in rounds: at each round,
 *  some nodes possess an information, and some other do not (at the beginning, the information is possessed by a single node, called
 *  the <em>source</em>). At each round, every node <var>x</var> chooses one of its neighbors <var>x'</var>; if <var>x</var> is informed, it communicates
 *  the information to <var>x'</var>, that thereby becomes informed (push); if <var>x</var> is not informed, it tries to get the information from <var>x'</var>,
 *  and thereby becomes informed if <var>x'</var> was (pull). The algorithm terminates when a certain fraction &theta; of the nodes have the information (in
 *  the original version, &theta;=1, which requires the graph to be connected).
 *  The algorithm is performed a certain number of times, and the distribution of rounds required for termination is considered.
 *
 * @author Paolo Boldi
 */

public class PushPullGossip {
	private static final Logger LOGGER = LoggerFactory.getLogger(PushPullGossip.class);

	/** A progress logger, or <code>null</code>. */
	private final ProgressLogger pl;
	/** The graph on which the algorithm is run. */
	private final ImmutableGraph graph;
	/** Number of nodes. */
	private int n;
	/** The bitvector of informed nodes at the beginning of this phase. */
	private BitVector informed;
	/** The bitvector of informed nodes at the end of this phase. */
	private BitVector willBeInformed;
	/** The object used to generate random bits. */
	private Random r;


	/** Creates a new instance for the algorithm execution.
	 *
	 * @param g the graph on which the algorithm is going to be run.
	 * @param pl the progress logger to be used, if any.
	 */
	public PushPullGossip(final ImmutableGraph g, final Random r, final ProgressLogger pl) {
		this.graph = g;
		this.n = g.numNodes();
		this.pl = pl;
		this.r = r;
		informed = LongArrayBitVector.ofLength(n);
		willBeInformed = LongArrayBitVector.ofLength(n);
	}

	/** Performs a step of execution.
	 *
	 * @return the number of nodes that have become informed after this step.
	 */
	protected int step() {
		NodeIterator it = graph.nodeIterator();
		int changed = 0;
		while (it.hasNext()) {
			int x = it.nextInt();
			int d = it.outdegree();
			if (d == 0) continue;
			int[] neighbors = it.successorArray();
			int xPrime = neighbors[r.nextInt(d)];
			boolean xInformed = informed.getBoolean(x);
			boolean xPrimeInformed = informed.getBoolean(xPrime);
			if (xInformed && !xPrimeInformed) {
					changed++;
					willBeInformed.set(xPrime);
				}
			if (!xInformed && xPrimeInformed) {
					changed++;
					willBeInformed.set(x);
				}
		}
		LOGGER.debug("DIFFERENZA: " + (willBeInformed.count() - informed.count()));
		BitVector temp = informed;
		informed = willBeInformed;
		willBeInformed = temp;
		willBeInformed.or(informed);
		return changed;
	}

	/** Initializes both informed arrays to the state where only a single source is informed.
	 *
	 */
	public void init(final int source) {
		informed.fill(false);
		willBeInformed.fill(false);
		informed.set(source);
		willBeInformed.set(source);
	}

	/** Performs an execution of the algorithm with given source and threshold.
	 *
	 * @param source the only node that is informed at the beginning.
	 * @param theta the fraction of nodes that need to be informed (the algorithm stops as soon as this many nodes have been informed).
	 * @return the number of execution steps performed.
	 */
	public int execution(final int source, final double theta) {
		init(source);
		int informed = 1;
		int steps = 0;
		LOGGER.info("A new simulation is run with threshold " + theta);
		while (informed < n * theta) {
			steps++;
			int changed = step();
			informed += changed;
			LOGGER.debug("Nodes that have been informed: " + changed + "; total: " + informed + " (" + (double)informed/n + ")");
		}
		return steps;
	}

	/** Simulates the algorithm many times on the same graph and returs statistics about the number of iterations required.
	 *
	 * @param source the only node that is informed at the beginning of each execution (if negative, it is generated every
	 * time at random).
	 * @param theta the fraction of nodes that need to be informed.
	 * @param numberExperiments the number of experiments to be performed.
	 * @return a bin containing data about the number of iterations performed during each simulation.
	 */
	public SummaryStats simulate(int source, final double theta, final int numberExperiments) {
		SummaryStats result = new SummaryStats();
		if (pl != null) {
			pl.itemsName = "executions";
			pl.expectedUpdates = numberExperiments;
			pl.start("Performing " + numberExperiments + " simulations");
		}
		for (int i = 0; i< numberExperiments; i++) {
			if (source < 0) source = r.nextInt(n);
			int steps = execution(source, theta);
			result.add(steps);
			LOGGER.info("Execution #" + i + " took " + steps + " iterations");
			pl.update();
		}
		return result;
	}


	public static void main(String arg[]) throws IOException, JSAPException, IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
		SimpleJSAP jsap = new SimpleJSAP(PushPullGossip.class.getName(), "Prints an approximation of the neighbourhood function.",
			new Parameter[] {
			new FlaggedOption("theta", JSAP.DOUBLE_PARSER, "1", JSAP.NOT_REQUIRED, 't', "theta", "The fraction of nodes that must be informed for the algorithm to end. Use 1 only if the graph is connected!"),
			new FlaggedOption("seed", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "seed", "The random seed."),
			new FlaggedOption("exp", JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 'e', "experiments", "The number of experiments"),
			new Switch("spec", 's', "spec", "The source is not a basename but rather a specification of the form <ImmutableGraphImplementation>(arg,arg,...)."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph: it must be bidirectional."),
			}
		);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final double theta = jsapResult.getDouble("theta");
		final String basename = jsapResult.getString("basename");
		final ProgressLogger pl = new ProgressLogger(LOGGER);
		final long seed = jsapResult.userSpecified("seed") ? jsapResult.getLong("seed") : Util.randomSeed();
		final Random random = new Random(seed);
		final int numberExperiments = jsapResult.getInt("exp");
		final boolean spec = jsapResult.getBoolean("spec");

		final ImmutableGraph graph =  spec ? ObjectParser.fromSpec(basename, ImmutableGraph.class, GraphClassParser.PACKAGE) : ImmutableGraph.load(basename, new ProgressLogger());

		PushPullGossip ppg = new PushPullGossip(graph, random, pl);
		SummaryStats bin = ppg.simulate(-1, theta, numberExperiments);
		System.out.println("Number of nodes: " + graph.numNodes());
		System.out.println("Mean: " + bin.mean());
		System.out.println("Min: " + bin.min() + "\tMax: " + bin.max());
		System.out.println("Standard deviation: " + bin.standardDeviation());
		System.out.println("Relative standard deviation: " + bin.relativeStandardDeviation());
	}
}
