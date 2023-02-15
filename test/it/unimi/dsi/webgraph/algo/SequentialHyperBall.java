/*
 * Copyright (C) 2010-2023 Paolo Boldi and Sebastiano Vigna
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

package it.unimi.dsi.webgraph.algo;

import static it.unimi.dsi.bits.LongArrayBitVector.word;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.SafelyCloseable;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.HyperLogLogCounterArray;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

/** <p>Computes the approximate neighbourhood function of a graph using a sequential version of HyperBall.
 *
 * @author Paolo Boldi and Sebastiano Vigna
 */

public class SequentialHyperBall extends HyperLogLogCounterArray implements SafelyCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(SequentialHyperBall.class);
	private static final boolean ASSERTS = true;

	private static final long serialVersionUID = 1L;

	protected final static int NODE_MASK = (int)(CHUNK_MASK >>> 6);

	/** The graph whose neighbourhood function we are going to approximate. */
	private final ImmutableGraph g;
	/** The number of nodes of {@link #g}, cached. */
	private final int numNodes;
	/** The square of {@link #numNodes}, cached. */
	private final double squareNumNodes;
	/** The name of the temporary file that will be used to dump the new set of counters. */
	private final File tempFile;
	/** The file output stream on {@link #tempFile} for writing newly computed registers. */
	private final FileOutputStream fos;
	/** A data output stream wrapping {@link FileOutputStream }. */
	private final DataOutputStream dos;
	/** An input stream on {@link #tempFile} for reading newly computed registers. */
	private final FastBufferedInputStream fbis;
	/** A progress logger, or <code>null</code>. */
	private final ProgressLogger pl;
	/** A temporary array used by {@link #subtract(long[], long[], int)}. */
	private final long accumulator[];
	/** A temporary array used by {@link #subtract(long[], long[], int)}. */
	private final long mask[];
	/** The value computed by the last call to {@link #iterate()} . */
	private double last;
	/** Whether this approximator has been already closed. */
	private boolean closed;

	private final static int ensureEnoughRegisters(final int log2m) {
		if (log2m < 4) throw new IllegalArgumentException("There must be at least 16 registers per counter");
		return log2m;
	}

	/** Creates a new approximator for the neighbourhood function.
	 *
	 * @param g the graph whosee neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public SequentialHyperBall(final ImmutableGraph g, final int log2m, final ProgressLogger pl, final long seed) throws IOException {
		super(g.numNodes(), g.numNodes(), ensureEnoughRegisters(log2m), seed);

		if (pl != null) pl.logger().info("Precision: " + Util.format(100 * HyperLogLogCounterArray.relativeStandardDeviation(log2m)) + "% (" + m  + " registers/counter, " + registerSize + " bits/counter)");

		this.g = g;
		this.pl = pl;

		numNodes = g.numNodes();
		squareNumNodes = (double)numNodes * numNodes;

		tempFile = File.createTempFile(SequentialHyperBall.class.getName(), "temp");
		tempFile.deleteOnExit();
		dos = new DataOutputStream(new FastBufferedOutputStream(fos = new FileOutputStream(tempFile)));
		fbis = new FastBufferedInputStream(new FileInputStream(tempFile));

		accumulator = new long[counterLongwords];
		mask = new long[counterLongwords];
	}

	/** Initialises the approximator.
	 *
	 * <p>This method must be call before a series of {@linkplain #iterate() iterations}.
	 */
	public void init() {
		if (pl != null) {
			pl.itemsName = "iterates";
			pl.start("Iterating...");
		}
		for(final long[] a: bits) Arrays.fill(a, 0);
		for(int i = numNodes; i-- != 0;) add(i, i);
		last = numNodes;
	}

	@Override
	public void close() throws IOException {
		if (closed) return;
		closed = true;
		dos.close();
		fbis.close();
		tempFile.delete();
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void finalize() throws Throwable {
		try {
			if (! closed) {
				LOGGER.warn("This " + this.getClass().getName() + " [" + toString() + "] should have been closed.");
				close();
			}
		}
		finally {
			super.finalize();
		}
	}


	/** Performs a multiple precision subtraction, leaving the result in the first operand.
	 *
	 * @param x a vector of longs.
	 * @param y a vector of longs that will be subtracted from <code>x</code>.
	 * @param l the length of <code>x</code> and <code>y</code>.
	 */
	private final static void subtract(final long[] x, final long[] y, final int l) {
		boolean borrow = false;

		for(int i = 0; i < l; i++) {
			if (! borrow || x[i]-- != 0) borrow = x[i] < y[i] ^ x[i] < 0 ^ y[i] < 0; // This expression returns the result of an unsigned strict comparison.
			x[i] -= y[i];
		}
	}

	/** Computes the register-by-register maximum of two bit vectors.
	 *
	 * @param x first vector of longs, representing a bit vector in {@link LongArrayBitVector} format, where the result will be stored.
	 * @param y a second vector of longs, representing a bit vector in {@link LongArrayBitVector} format, that will be maximised with <code>x</code>.
	 * @param r the register size.
	 */

	private final void max(final long[] x, final long[] y, final int r) {
		final int l = x.length;

		// Local copies of vectors used to store intermediate results.
		final long[] accumulator = this.accumulator;
		final long[] mask = this.mask;
		final long[] msbMask = this.msbMask;

		/* We work in two phases. Let H_r (msbMask) by the mask with the
		 * highest bit of each register (of size r) set, and L_r (lsbMask)
		 * be the mask with the lowest bit of each register set.
		 * We describe the algorithm on a single word.
		 *
		 * If the first phase we perform an unsigned strict register-by-register
		 * comparison of x and y, using the formula
		 *
		 * z = ((((x | H_r) - (y & ~H_r)) | (x ^ y))^ (x | ~y)) & H_r
		 *
		 * Then, we generate a register-by-register mask of all ones or
		 * all zeroes, depending on the result of the comparison, using the
		 * formula
		 *
		 * (((z >> r-1 | H_r) - L_r) | H_r) ^ z
		 *
		 * At that point, it is trivial to select from x and y the right values.
		 */

		// We load x | H_r into the accumulator
		for(int i = l; i-- != 0;) accumulator[i] = x[i] | msbMask[i];
		// We subtract y & ~H_r, using mask as temporary storage
		for(int i = l; i-- != 0;) mask[i] = y[i] & ~msbMask[i];
		subtract(accumulator, mask, l);

		// We OR with x ^ y, XOR with (x | ~y), and finally AND with H_r.
		for(int i = l; i-- != 0;) accumulator[i] = ((accumulator[i] | (x[i] ^ y[i])) ^ (x[i] | ~y[i])) & msbMask[i];

		if (ASSERTS) {
			final LongBigList a = LongArrayBitVector.wrap(x).asLongBigList(r);
			final LongBigList b = LongArrayBitVector.wrap(y).asLongBigList(r);
			for(long i = 0; i < a.size64(); i++) {
				final long pos = (i + 1) * r - 1;
				assert (a.getLong(i) < b.getLong(i)) == ((accumulator[word(pos)] & 1L << pos) != 0);
			}
		}

		// We shift by r - 1 places and put the result into mask
		final int rMinus1 = r - 1;
		for(int i = l - 1; i-- != 0;) mask[i] = accumulator[i] >>> rMinus1 | accumulator[i + 1] << (Long.SIZE - rMinus1) | msbMask[i];
		mask[l - 1] = accumulator[l - 1] >>> rMinus1 | msbMask[l - 1];

		// We subtract L_r from mask
		subtract(mask, lsbMask, l);

		// We OR with H_r and XOR with the accumulator
		for(int i = l; i-- != 0;) mask[i] = (mask[i] | msbMask[i]) ^ accumulator[i];

		if (ASSERTS) {
			final long[] t = x.clone();
			final LongBigList a = LongArrayBitVector.wrap(t).asLongBigList(r);
			final LongBigList b = LongArrayBitVector.wrap(y).asLongBigList(r);
			for(int i = 0; i < Long.SIZE * l / r; i++) a.set(i, Math.max(a.getLong(i), b.getLong(i)));
			// Note: this must be kept in sync with the line computing the result.
			for(int i = l; i-- != 0;) assert t[i] == (mask[i] & x[i] | ~mask[i] & y[i]);
		}

		// Finally, we use mask to select the right bits from x and y and store the result.
		for(int i = l; i-- != 0;) x[i] = mask[i] & x[i] | ~mask[i] & y[i];

	}

	private final void copyToLocal(final LongArrayBitVector chunk, final long[] t, final int node) {
		// Offset in bits
		final long counterLongwords = t.length;
		long offset = (node << log2m & CHUNK_MASK) * registerSize;
		// Note that we might copy a few bits in excess, but they will not be used anyway.
		for(int i = 0; i < counterLongwords; i++, offset += Long.SIZE) t[i] = chunk.getLong(offset, Math.min(offset + Long.SIZE, chunk.length()));
	}

	/** Performs a new iteration of HyperBall.
	 *
	 * @return an approximation of the following value of the neighbourhood function (the
	 * first returned value is for distance one).
	 */
	public double iterate() throws IOException {
		final LongArrayBitVector bitVector[] = new LongArrayBitVector[bits.length];
		for(int i = bits.length; i-- != 0;) bitVector[i] = LongArrayBitVector.wrap(bits[i]);

		final NodeIterator nodeIterator = g.nodeIterator();
		final int counterBits = registerSize << log2m;
		final int nodeShift = this.counterShift;

		final long t[] = new long[counterLongwords];
		final long u[] = new long[counterLongwords];

		final ProgressLogger nodeProgressLogger = pl == null ? null : new ProgressLogger(LOGGER, 10, TimeUnit.MINUTES, "nodes");

		fbis.flush();
		dos.flush();
		fos.getChannel().position(0);

		if (nodeProgressLogger != null) {
			nodeProgressLogger.expectedUpdates = numNodes;
			nodeProgressLogger.start("Scanning graph...");
		}

		for(int i = 0; i < numNodes; i++) {
			nodeIterator.nextInt();
			int d = nodeIterator.outdegree();
			final int[] successor = nodeIterator.successorArray();
			copyToLocal(bitVector[i >>> nodeShift], t, i);
			while(d-- != 0) {
				final int s = successor[d];
				if (s != i) { // Self-loops to not influence the computation
					copyToLocal(bitVector[s >>> nodeShift], u, s);
					max(t, u, registerSize);
				}
			}

			if (ASSERTS)  {
				final LongBigList test = LongArrayBitVector.wrap(t).asLongBigList(registerSize);
				for(int rr = 0; rr < m; rr++) {
					int max = (int)registers[(int)((((long)i << log2m) + rr) >> CHUNK_SHIFT)].getLong((((long)i << log2m) + rr) & CHUNK_MASK);
					for(int j = nodeIterator.outdegree(); j-- != 0;) max = Math.max(max, (int)registers[(int)((((long)successor[j] << log2m) + rr) >> CHUNK_SHIFT)].getLong((((long)successor[j] << log2m) + rr) & CHUNK_MASK));
					assert max == test.getLong(rr) : max + "!=" + test.getLong(rr) + " [" + rr + "]";
				}
			}

			// We store long-size padded bits.
			BinIO.storeLongs(t, dos);

			if (nodeProgressLogger != null) nodeProgressLogger.lightUpdate();
		}

		if (nodeProgressLogger != null) nodeProgressLogger.done();

		dos.flush();
		fbis.position(0);
		final DataInputStream dis = new DataInputStream(fbis);

		for(int i = 0; i < bitVector.length; i++) {
			final int numCounters = (int)(registers[i].size64() >> log2m);
			bitVector[i].clear();
			for(int j = 0; j < numCounters; j++) {
				// We read long-size padded bits and store just the useful part.
				BinIO.loadLongs(dis, t);
				bitVector[i].append(LongArrayBitVector.wrap(t).subVector(0, counterBits));
			}
		}

		double result = 0, c = 0, y, z;
		// Kahan summation
		for(int i = numNodes; i-- != 0;) {
			y = count(i) - c;
			z = result + y;
			c = (z - result) - y;
			result = z;
		}

		if (pl != null) {
			pl.update();
			pl.logger().info("Pairs: " + result + " (" + 100.0 * result / squareNumNodes + "%)");
		}

		if (result < last) result = last;
		last = result;
		return result;
	}

	/** Returns an approximation of the neighbourhood function.
	 *
	 * @param upperBound an upper bound to the number of iterations.
	 * @param threshold a value that will be used to stop the computation either by absolute or relative increment.
	 * @return an approximation of the neighbourhood function.
	 */
	public double[] approximateNeighbourhoodFunction(long upperBound, final double threshold) throws IOException {
		final DoubleArrayList approximateNeighbourhoodFunction = new DoubleArrayList();
		upperBound = Math.min(upperBound, numNodes);
		double last;
		approximateNeighbourhoodFunction.add(last = numNodes);
		init();

		for(long i = 0; i < upperBound; i++) {
			final double current = iterate();
			LOGGER.info("Absolute increment: " + (current - last));
			if (current - last <= threshold) {
				LOGGER.info("Terminating approximation after " + i + " iteration(s) by absolute bound");
				break;
			}

			LOGGER.info("Relative increment: " + (current / last));
			if (i > 3 && current / last < (1 + threshold)) {
				LOGGER.info("Terminating approximation after " + i + " iteration(s) by relative bound");
				break;
			}
			approximateNeighbourhoodFunction.add(last = current);
		}

		if (pl != null) pl.done();
		return approximateNeighbourhoodFunction.toDoubleArray();
	}

	public static void main(final String arg[]) throws IOException, JSAPException, IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
		final SimpleJSAP jsap = new SimpleJSAP(SequentialHyperBall.class.getName(), "Prints an approximation of the neighbourhood function.",
			new Parameter[] {
				new FlaggedOption("log2m", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'l', "log2m", "The logarithm of the number of registers."),
				new FlaggedOption("upperBound", JSAP.LONGSIZE_PARSER, Long.toString(Long.MAX_VALUE), JSAP.NOT_REQUIRED, 'u', "upper-bound", "An upper bound to the number of iteration (default: the graph size)."),
				new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(1E-3), JSAP.NOT_REQUIRED, 't', "threshould", "A threshould that will be used to stop the computation by absolute or relative increment."),
				new Switch("spec", 's', "spec", "The source is not a basename but rather a specification of the form <ImmutableGraphImplementation>(arg,arg,...)."),
				new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean spec = jsapResult.getBoolean("spec");
		final String basename = jsapResult.getString("basename");
		final ProgressLogger pl = new ProgressLogger(LOGGER);
		final int log2m = jsapResult.getInt("log2m");

		final ImmutableGraph graph = spec ? ObjectParser.fromSpec(basename, ImmutableGraph.class, GraphClassParser.PACKAGE) : ImmutableGraph.loadOffline(basename);

		final SequentialHyperBall shb = new SequentialHyperBall(graph, log2m, pl, Util.randomSeed());
		TextIO.storeDoubles(shb.approximateNeighbourhoodFunction(jsapResult.getLong("upperBound"), jsapResult.getDouble("threshold")), System.out);
		shb.close();
	}
}
