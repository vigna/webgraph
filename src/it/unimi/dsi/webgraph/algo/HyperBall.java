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

package it.unimi.dsi.webgraph.algo;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.ints.AbstractInt2DoubleFunction;
import it.unimi.dsi.fastutil.ints.Int2DoubleFunction;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.SafelyCloseable;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.HyperLogLogCounterArray;
import it.unimi.dsi.util.KahanSummation;
import it.unimi.dsi.util.XoRoShiRo128PlusRandomGenerator;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;

/** <p>Computes an approximation of the neighbourhood function, of the size of the reachable sets,
 * and of (discounted) positive geometric centralities of a graph using HyperBall.
 *
 * <p>HyperBall is an algorithm computing by dynamic programming an approximation
 * of the sizes of the balls of growing radius around the nodes of a graph. Starting from
 * these data, it can approximate the <em>neighbourhood function</em> of a graph, that is, the function returning
 * for each <var>t</var> the number of pairs of nodes at distance at most <var>t</var>,
 * the number of nodes reachable from each node, Bavelas's closeness centrality, Lin's index, and
 * <em>harmonic centrality</em> (studied by Paolo Boldi and Sebastiano Vigna in &ldquo;<a href ="http://vigna.di.unimi.it/papers.php#BoVAC">Axioms for Centrality</a>&rdquo;, <i>Internet Math.</i>,
 * 10(3-4):222&minus;262, 2014).
 * HyperBall can also compute <em>discounted centralities</em>, in which the <em>discount</em> assigned to a node is some
 * specified function of its distance. All centralities are computed in their <em>positive</em> version (i.e.,
 * using distance <em>from</em> the source: see below how to compute the more usual, and useful, <em>negative</em> version).
 *
 * <p>HyperBall has been described by Paolo Boldi and Sebastiano Vigna in
 * &ldquo;In-Core Computation of Geometric Centralities with HyperBall: A Hundred Billion Nodes and Beyond&rdquo;,
 * <i>Proc. of 2013 IEEE 13th International Conference on Data Mining Workshops (ICDMW 2013)</i>, IEEE, 2013,
 * and it is a generalization of the method described in &ldquo;HyperANF: Approximating the Neighbourhood Function of Very Large Graphs
 * on a Budget&rdquo;, by Paolo Boldi, Marco Rosa and Sebastiano Vigna,
 * <i>Proceedings of the 20th international conference on World Wide Web</i>, pages 625&minus;634, ACM, (2011).
 *
 * <p>Incidentally, HyperBall (actually, HyperANF) has been used to show that Facebook has just <a href="http://vigna.dsi.unimi.it/papers.php#BBRFDS">four degrees of separation</a>.
 *
 * <p>At step <var>t</var>, for each node we (approximately) keep track (using {@linkplain HyperLogLogCounterArray HyperLogLog counters})
 * of the set of nodes at distance at most <var>t</var>. At each iteration, the sets associated with the successors of each node are merged,
 * thus obtaining the new sets. A crucial component in making this process efficient and scalable is the usage of
 * <em>broadword programming</em> to implement the join (merge) phase, which requires maximising in parallel the list of registers associated with
 * each successor (the implementation is geared towards 64-bits processors).
 *
 * <p>Using the approximate sets, for each <var>t</var> we estimate the number of pairs of nodes (<var>x</var>,<var>y</var>) such
 * that the distance from <var>x</var> to <var>y</var> is at most <var>t</var>. Since during the computation we are also
 * in possession of the number of nodes at distance <var>t</var> &minus; 1, we can also perform computations
 * using the number of nodes at distance <em>exactly</em> <var>t</var> (e.g., centralities).
 *
 * <p>To use this class, you must first create an instance.
 * Then, you call {@link #init()} (once) and then {@link #iterate()} as much as needed (you can init/iterate several times, if you want so).
 * A {@linkplain #run(long, double) commodity method} will do everything for you.
 * Finally, you <strong>must</strong> {@link #close()} the instance. The method {@link #modified()} will tell you whether the internal state of
 * the algorithm has changed.
 *
 * <p>If you additionally pass to the constructor (or on the command line) the <em>transpose</em> of your graph (you can compute it using {@link Transform#transposeOffline(ImmutableGraph,int)}
 * or {@link Transform#transposeOffline(ImmutableGraph, int)}), when three quarters of the nodes stop changing their value
 * HyperBall will switch to a <em>systolic</em> computation: using the transpose, when a node changes it will signal back
 * to its predecessors that at the next iteration they could change. At the next scan, only the successors of
 * signalled nodes will be scanned. In particular,
 * when a very small number of nodes is modified by an iteration, HyperBall will switch to a systolic <em>local</em> mode,
 * in which all information about modified nodes is kept in (traditional) dictionaries, rather than being represented as arrays of booleans.
 * This strategy makes the last phases of the computation orders of magnitude faster, and makes
 * in practice the running time of HyperBall proportional to the theoretical bound
 * <i>O</i>(<var>m</var> log <var>n</var>), where <var>n</var>
 * is the number of nodes and <var>m</var> is the number of the arcs of the graph. Note that
 * graphs with a large diameter require a correspondingly large number of iterations, and these iterations will have to
 * pass over all nodes if you do not provide the tranpose.
 *
 * <p>Deciding when to stop iterating is a rather delicate issue. The only safe way is to iterate until {@link #modified()} is zero,
 * and systolic (local) computation makes this goal easily attainable.
 * However, in some cases one can assume that the graph is not pathological, and stop when the relative increment of the number of pairs goes below
 * some threshold.
 *
 * <h2>Computing Centralities</h2>
 *
 * <p>Note that usually one is interested in the <em>negative</em> version of a centrality measure, that is, the version
 * that depends on the <em>incoming</em> arcs. HyperBall can compute only <em>positive</em> centralities: if you are
 * interested (as it usually happens) in the negative version, you must pass to HyperBall the <em>transpose</em> of the graph
 * (and if you want to run in systolic mode, the original graph, which is the transpose of the transpose). Note that the
 * neighbourhood function of the transpose is identical to the neighbourhood function of the original graph, so the exchange
 * does not alter its computation.
 *
 * <h2>Node weights</h2>
 *
 * <p>HyperBall can manage to a certain extent a notion of <em>node weight</em> in its computation of centralities. Weights must
 * be nonnegative integers, and the initialization phase requires generating a random integer for each unit of overall
 * weight, as weight are simulated by loading the counter of a node with multiple elements. Combining this feature
 * with discounts, one can compute <em>discounted-gain centralities</em> as defined in the HyperBall paper.
 *
 * <h2>Configuring the JVM</h2>
 *
 * <p>HyperBall computations go against all basic assumptions of Java garbage collection. It is thus
 * essential that you reconfigure your JVM properly. A good starting point is the following command line:
 * <pre>
 * java -server -Xss256K -Xms100G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G \
 *      -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB \
 *      -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=99 -XX:+UseCMSInitiatingOccupancyOnly \
 *      -verbose:gc -Xloggc:gc.log ...
 * </pre>
 *
 * <ul>
 * <li><code>-Xss256K</code> reduces the stack memory used by each thread.
 * <li><code>-Xms100G</code> size the heap: the more memory, the more counter per registers
 * you can use (the amount, of course, depends on your hardware); please note that we set the
 * <em>starting</em> heap size as expansion of large heaps is very expensive.
 * <li><code>-XX:PretenureSizeThreshold=512M</code> forces the allocation of registers directly into the old generation.
 * <li><code>-XX:MaxNewSize=4G</code> leaves almost all memory for the old generation.
 * <li><code>-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=99 -XX:+UseCMSInitiatingOccupancyOnly</code>
 * set the concurrent garbage collector, and impose that no collection is performed until 99% of the permanent
 * generation is filled.
 * <li><code>-XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB</code> usually improve performance, but your mileage may vary.
 * </ul>
 * <p>Check the garbage collector logs (<code>gc.log</code>) to be sure that your
 * minor and major collections are very infrequent (as they should be).
 *
 * <h2>Performance issues</h2>
 *
 * <p>To use HyperBall effectively, you should aim at filling a large percentage of the available core memory. This requires,
 * of course, to size properly the heap, but also to configure some parameters.
 *
 * <p>Most of the memory goes into storing HyperLogLog registers. By tuning the number of registers per counter, you can
 * modify the memory allocated for them. The amount of memory is logged, and you should check that the number of registers you
 * chose almost fills up the heap memory you allocated, possibly leaving space for the graph(s) (but read below).
 * Note that you can only choose a number of registers per counter that is
 * a power of two, so your latitude in adjusting the memory used for registers is somewhat limited.
 *
 * <p>If you have little memory, this class can perform <em>external</em> computations: instead of keeping in core memory
 * an old and a new copy of the counters, it can dump on disk an <em>update list</em> containing pairs &lt;<var>node</var>,&nbsp;<var>counter</var>&gt;.
 * At the end of an iteration, the update list is loaded and applied to the counters in memory.
 * The process is of course slower, but the core memory used is halved.
 *
 * <p>Then, some memory is necessary to load the graph (and possibly its tranpose). We suggest to check the offline
 * option, which will map the graph into memory, rather than loading it. If you map the graph into memory, take care of
 * leaving some free memory, beside that allocated for the heap, as the operating system will need some space to buffer
 * the memory-mapped graph(s).
 *
 * <p>If there are several available cores, the runs of {@link #iterate()} will be <em>decomposed</em> into relatively
 * small tasks (small blocks of nodes) and each task will be assigned to the first available core. Since all tasks are completely
 * independent, this behaviour ensures a very high degree of parallelism. Be careful, however, because this feature requires a graph with
 * a reasonably fast random access (e.g., in the case of a {@link BVGraph}, short reference chains), as many
 * calls to {@link ImmutableGraph#nodeIterator(int)} will be made. The <em>granularity</em> of the decomposition
 * is the number of nodes assigned to each task.
 *
 * <p>In any case, when attacking very large graphs (in particular, in external mode) some system tuning (e.g.,
 * increasing the filesystem commit time) is a good idea. Also experimenting with granularity and buffer sizes
 * can be useful. Smaller buffers reduce the waits on I/O calls, but increase the time spent in disk seeks.
 * Large buffers improve I/O, but they use a lot of memory. The best possible setup is the one in which
 * the cores are 100% busy during the graph scan, and the I/O time
 * logged at the end of a scan is roughly equal to the time that is necessary to reload the counters from disk:
 * in such a case, essentially, you are computing as fast as possible.
 *
 * @author Sebastiano Vigna
 * @author Paolo Boldi
 * @author Marco Rosa
 */

public class HyperBall extends HyperLogLogCounterArray implements SafelyCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(HyperBall.class);
	public static final boolean ASSERTS = false;
	private static final long serialVersionUID = 1L;

	/** An abstract discount function is a facility to implement a discount function (so that only
	 *  the {@link #get(int)} method must be actually implemented).
	 *
	 *  <p>Note that by contract {@link #get(int)} will never be called with argument (i.e., distance) zero.
	 */
	public static abstract class AbstractDiscountFunction extends AbstractInt2DoubleFunction {
		private static final long serialVersionUID = 1L;
		@Override
		public int size() { return -1; }
		@Override
		public boolean containsKey(final int key) { return true; }
	}

	public static Int2DoubleFunction INV_SQUARE_DISCOUNT = new AbstractDiscountFunction() {
		private static final long serialVersionUID = 1L;
		@Override
		public double get(final int distance) { return 1. / ((long)distance * distance); }
	};

	public static Int2DoubleFunction INV_LOG_DISCOUNT = new AbstractDiscountFunction() {
		private static final long serialVersionUID = 1L;
		@Override
		public double get(final int distance) { return 1 / Fast.log2(distance + 1); }
	};

	/** The default granularity of a task. */
	public static final int DEFAULT_GRANULARITY = 16 * 1024;
	/** The default size of a buffer in bytes. */
	public static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
	/** True if we have the transpose graph. */
	protected final boolean gotTranspose;
	/** An array of nonnegative node weights, or {@code null}. */
	protected final int[] weight;
	/** True if we started a systolic computation. */
	protected boolean systolic;
	/** True if we are preparing a local computation (we are {@link #systolic} and less than 1% nodes were modified). */
	protected boolean preLocal;
	/** True if we started a local computation. */
	protected boolean local;
	/** Whether the sum of distances from each node (inverse of <strong>positive</strong> closeness centrality) should be computed; if false, {@link #sumOfDistances} is <code>null</code>. */
	protected  final boolean doSumOfDistances;
	/** Whether the sum of inverse distances from each node (<strong>positive</strong> harmonic centrality) should be computed; if false, {@link #sumOfInverseDistances} is <code>null</code>. */
	protected boolean doSumOfInverseDistances;
	/** The neighbourhood function, if requested. */
	public final DoubleArrayList neighbourhoodFunction;
	/** The sum of the distances from every given node, if requested. */
	public final float[] sumOfDistances;
	/** The sum of inverse distances from each given node, if requested. */
	public final float[] sumOfInverseDistances;
	/** A number of discounted centralities to be computed, possibly none. */
	public final Int2DoubleFunction[] discountFunction;
	/** The overall discounted centrality, for every {@link #discountFunction}. */
	public final float[][] discountedCentrality;
	/** The number of nodes of the graph, cached. */
	protected final int numNodes;
	/** The number of arcs of the graph, cached. */
	protected long numArcs;
	/** The square of {@link #numNodes}, cached. */
	protected final double squareNumNodes;
	/** The number of cores used in the computation. */
	protected final int numberOfThreads;
	/** The size of an I/O buffer, in counters. */
	protected final int bufferSize;
	/** The number of actually scanned nodes per task in a multithreaded environment. <strong>Must</strong> be a multiple of {@link Long#SIZE}. */
	protected final int granularity;
	/** The number of nodes per task (obtained by adapting {@link #granularity} to the current ratio of modified nodes). <strong>Must</strong> be a multiple of {@link Long#SIZE}. */
	protected long adaptiveGranularity;
	/** The value computed by the last iteration. */
	protected double last;
	/** The value computed by the current iteration. */
	protected double current;
	/** The current iteration. */
	protected int iteration;
	/** If {@link #external} is true, the name of the temporary file that will be used to write the update list. */
	protected final File updateFile;
	/** If {@link #external} is true, a file channel used to write to the update list. */
	protected final FileChannel fileChannel;
	/** If {@link #external} is true, the random-access file underlying {@link #fileChannel}. */
	protected RandomAccessFile randomAccessFile;
	/** The cumulative list of outdegrees. */
	protected final EliasFanoCumulativeOutdegreeList cumulativeOutdegrees;
	/** A progress logger, or <code>null</code>. */
	protected final ProgressLogger pl;
	/** The lock protecting all critical sections. */
	protected final ReentrantLock lock;
	/** A condition that is notified when all iteration threads are waiting to be started. */
	protected final Condition allWaiting;
	/** The condition on which all iteration threads wait before starting a new phase. */
	protected final Condition start;
	/** The current computation phase. */
	public int phase;
	/** Whether this approximator has been already closed. */
	protected boolean closed;
	/** The threads performing the computation. */
	protected final IterationThread thread[];
	/** An atomic integer keeping track of the number of node processed so far. */
	protected final AtomicInteger nodes;
	/** An atomic integer keeping track of the number of arcs processed so far. */
	protected final AtomicLong arcs;
	/** A variable used to wait for all threads to complete their iteration. */
	protected volatile int aliveThreads;
	/** True if the computation is over. */
	protected volatile boolean completed;
	/** Total number of write operation performed on {@link #fileChannel}. */
	protected volatile long numberOfWrites;
	/** Total wait time in milliseconds of I/O activity on {@link #fileChannel}. */
	protected volatile long totalIoMillis;
	/** The starting node of the next chunk of nodes to be processed. */
	protected int nextNode;
	/** The number of arcs before {@link #nextNode}. */
	protected long nextArcs;
	/** The number of register modified by the last call to {@link #iterate()}. */
	protected final AtomicInteger modified;
	/** Counts the number of unwritten entries when {@link #external} is true, or
	 * the number of counters that did not change their value. */
	protected final AtomicInteger unwritten;
	/** The relative increment of the neighbourhood function for the last iteration. */
	protected double relativeIncrement;
	/** Whether we should used an update list on disk, instead of computing results in core memory. */
	protected boolean external;
	/** If {@link #external} is false, the arrays where results are stored. */
	protected final long[][] resultBits;
	/** If {@link #external} is false, a {@link #registerSize}-bit views of {@link #resultBits}. */
	protected final LongBigList resultRegisters[];
	/** For each counter, whether it has changed its value. We use an array of boolean (instead of a {@link LongArrayBitVector}) just for access speed. */
	protected boolean[] modifiedCounter;
	/** For each newly computed counter, whether it has changed its value. {@link #modifiedCounter}
	 * will be updated with the content of this bit vector by the end of the iteration. */
	protected boolean[] modifiedResultCounter;
	/** For each counter, whether it has changed its value. We use an array of boolean (instead of a {@link LongArrayBitVector}) just for access speed. */
	protected boolean[] nextMustBeChecked;
	/** For each newly computed counter, whether it has changed its value. {@link #modifiedCounter}
	 * will be updated with the content of this bit vector by the end of the iteration. */
	protected boolean[] mustBeChecked;
	/** If {@link #local} is true, the sorted list of nodes that should be scanned. */
	protected int[] localCheckList;
	/** If {@link #preLocal} is true, the set of nodes that should be scanned on the next iteration. Note that this set is synchronized. */
	protected final IntSet localNextMustBeChecked;
	/** One of the throwables thrown by some of the threads, if at least one thread has thrown a throwable. */
	protected volatile Throwable threadThrowable;

	protected final static int ensureRegisters(final int log2m) {
		if (log2m < 4) throw new IllegalArgumentException("There must be at least 16 registers per counter");
		if (log2m > 60) throw new IllegalArgumentException("There can be at most 2^60 registers per counter");
		return log2m;
	}

	/** Computes the number of threads.
	 *
	 * <p>If the specified number of threads is zero, {@link Runtime#availableProcessors()} will be returned.
	 *
	 * @param suggestedNumberOfThreads
	 * @return the actual number of threads.
	 */
	private final static int numberOfThreads(final int suggestedNumberOfThreads) {
		if (suggestedNumberOfThreads != 0) return suggestedNumberOfThreads;
		return Runtime.getRuntime().availableProcessors();
	}

	/** Creates a new HyperBall instance.
	 *
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the transpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 * @param numberOfThreads the number of threads to be used (0 for automatic sizing).
	 * @param bufferSize the size of an I/O buffer in bytes (0 for {@link #DEFAULT_BUFFER_SIZE}).
	 * @param granularity the number of node per task in a multicore environment (it will be rounded to the next multiple of 64), or 0 for {@link #DEFAULT_GRANULARITY}.
	 * @param external if true, results of an iteration will be stored on disk.
	 */
	public HyperBall(final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl, final int numberOfThreads, final int bufferSize, final int granularity, final boolean external) throws IOException {
		this(g, gt, log2m, pl, numberOfThreads, bufferSize, granularity, external, false, false, null, Util.randomSeed());
	}

	/** Creates a new HyperBall instance using default values.
	 *
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the transpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 */
	public HyperBall(final ImmutableGraph g, final ImmutableGraph gt, final int log2m) throws IOException {
		this(g, gt, log2m, null, 0, 0, 0, false);
	}

	/** Creates a new HyperBall instance using default values.
	 *
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the transpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public HyperBall(final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl) throws IOException {
		this(g, null, log2m, pl, 0, 0, 0, false);
	}

	/** Creates a new HyperBall instance using default values and disabling systolic computation.
	 *
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 */
	public HyperBall(final ImmutableGraph g, final int log2m) throws IOException {
		this(g, null, log2m);
	}

	/** Creates a new HyperBall instance using default values and disabling systolic computation.
	 *
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param seed the random seed passed to {@link HyperLogLogCounterArray#HyperLogLogCounterArray(long, long, int, long)}.
	 */
	public HyperBall(final ImmutableGraph g, final int log2m, final long seed) throws IOException {
		this(g, null, log2m, null, 0, 0, 0, false, false, false, null, seed);
	}

	/** Creates a new HyperBall instance using default values and disabling systolic computation.
	 *
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public HyperBall(final ImmutableGraph g, final int log2m, final ProgressLogger pl) throws IOException {
		this(g, null, log2m, pl);
	}

	/** Creates a new HyperBall instance without weights.
	 *
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the transpose of <code>g</code>, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 * @param numberOfThreads the number of threads to be used (0 for automatic sizing).
	 * @param bufferSize the size of an I/O buffer in bytes (0 for {@link #DEFAULT_BUFFER_SIZE}).
	 * @param granularity the number of node per task in a multicore environment (it will be rounded to the next multiple of 64), or 0 for {@link #DEFAULT_GRANULARITY}.
	 * @param external if true, results of an iteration will be stored on disk.
	 * @param doSumOfDistances whether the sum of distances from each node should be computed.
	 * @param doSumOfInverseDistances whether the sum of inverse distances from each node should be computed.
	 * @param discountFunction an array (possibly <code>null</code>) of discount functions.
	 * @param seed the random seed passed to {@link HyperLogLogCounterArray#HyperLogLogCounterArray(long, long, int, long)}.
	 */
	public HyperBall(final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl,
			final int numberOfThreads, final int bufferSize, final int granularity, final boolean external,
			final boolean doSumOfDistances, final boolean doSumOfInverseDistances, final Int2DoubleFunction[] discountFunction, final long seed) throws IOException {
		this(g, gt, log2m, pl, numberOfThreads, bufferSize, granularity, external, doSumOfDistances, doSumOfInverseDistances, discountFunction, null, seed);
	}

	private static long totalWeight(final int numNodes, final int[] weight) {
		if (weight == null) return numNodes;
		if (weight.length != numNodes) throw new IllegalArgumentException("The weight array length (" + weight.length + ") and the number of nodes of the graph (" + numNodes + ") do not match");

		long totalWeight = 0;
		for(final int w: weight) {
			if (w < 0) throw new IllegalArgumentException("Negative weight: " + w);
			totalWeight += w;
		}
		return totalWeight;
	}

	/** Creates a new HyperBall instance.
	 *
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the transpose of <code>g</code>, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 * @param numberOfThreads the number of threads to be used (0 for automatic sizing).
	 * @param bufferSize the size of an I/O buffer in bytes (0 for {@link #DEFAULT_BUFFER_SIZE}).
	 * @param granularity the number of node per task in a multicore environment (it will be rounded to the next multiple of 64), or 0 for {@link #DEFAULT_GRANULARITY}.
	 * @param external if true, results of an iteration will be stored on disk.
	 * @param doSumOfDistances whether the sum of distances from each node should be computed.
	 * @param doSumOfInverseDistances whether the sum of inverse distances from each node should be computed.
	 * @param discountFunction an array (possibly <code>null</code>) of discount functions.
	 * @param weight an array of nonnegative node weights.
	 * @param seed the random seed passed to {@link HyperLogLogCounterArray#HyperLogLogCounterArray(long, long, int, long)}.
	 */
	public HyperBall(final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl,
			final int numberOfThreads, final int bufferSize, final int granularity, final boolean external,
			final boolean doSumOfDistances, final boolean doSumOfInverseDistances, final Int2DoubleFunction[] discountFunction, final int[] weight, final long seed) throws IOException {
		super(g.numNodes(), totalWeight(g.numNodes(), weight), ensureRegisters(log2m), seed);

		info("Seed : " + Long.toHexString(seed));

		gotTranspose = gt != null;
		localNextMustBeChecked = gotTranspose ? IntSets.synchronize(new IntOpenHashSet(Hash.DEFAULT_INITIAL_SIZE, Hash.VERY_FAST_LOAD_FACTOR)) : null;
		this.weight = weight;

		numNodes = g.numNodes();
		try {
			numArcs = g.numArcs();
		}
		catch(final UnsupportedOperationException e) {
			// No number of arcs. We have to enumerate.
			long a = 0;
			final NodeIterator nodeIterator = g.nodeIterator();
			for(int i = g.numNodes(); i-- != 0;) {
				nodeIterator.nextInt();
				a += nodeIterator.outdegree();
			}
			numArcs = a;
		}
		squareNumNodes = (double)numNodes * numNodes;

		cumulativeOutdegrees = new EliasFanoCumulativeOutdegreeList(g, numArcs, Math.max(0, 64 / m - 1));

		modifiedCounter = new boolean[numNodes];
		modifiedResultCounter = external ? null : new boolean[numNodes];
		if (gt != null) {
			mustBeChecked = new boolean[numNodes];
			nextMustBeChecked = new boolean[numNodes];
			if (gt.numNodes() != g.numNodes()) throw new IllegalArgumentException("The graph and its transpose have a different number of nodes");
			if (gt.numArcs() != g.numArcs()) throw new IllegalArgumentException("The graph and its transpose have a different number of arcs");
		}

		this.pl = pl;
		this.external = external;
		this.doSumOfDistances = doSumOfDistances;
		this.doSumOfInverseDistances = doSumOfInverseDistances;
		this.discountFunction = discountFunction == null ? new Int2DoubleFunction[0] : discountFunction;
		this.numberOfThreads = numberOfThreads(numberOfThreads);
		this.granularity = numberOfThreads == 1 ? numNodes : granularity == 0 ? DEFAULT_GRANULARITY : ((granularity + Long.SIZE - 1) & -Long.SIZE);
		this.bufferSize = Math.max(1, (bufferSize == 0 ? DEFAULT_BUFFER_SIZE : bufferSize) / (Long.BYTES * (counterLongwords + 1)));

		info("Relative standard deviation: " + Util.format(100 * HyperLogLogCounterArray.relativeStandardDeviation(log2m)) + "% (" + m  + " registers/counter, " + registerSize + " bits/register, " + Util.format(m * registerSize / 8.) + " bytes/counter)");
		if (external) info("Running " + this.numberOfThreads + " threads with a buffer of " + Util.formatSize(this.bufferSize) + " counters");
		else info("Running " + this.numberOfThreads + " threads");

		thread = new IterationThread[this.numberOfThreads];

		if (external) {
			info("Creating update list...");
			updateFile = File.createTempFile(HyperBall.class.getName(), "-temp");
			updateFile.deleteOnExit();
			fileChannel = (randomAccessFile = new RandomAccessFile(updateFile, "rw")).getChannel();
		}
		else {
			updateFile = null;
			fileChannel = null;
		}

		nodes = new AtomicInteger();
		arcs = new AtomicLong();
		modified = new AtomicInteger();
		unwritten = new AtomicInteger();

		neighbourhoodFunction = new DoubleArrayList();
		sumOfDistances = doSumOfDistances ? new float[numNodes] : null;
		sumOfInverseDistances = doSumOfInverseDistances ? new float[numNodes] : null;
		discountedCentrality = new float[this.discountFunction.length][];
		for (int i = 0; i < this.discountFunction.length; i++) discountedCentrality[i] = new float[numNodes];

		info("HyperBall memory usage: " + Util.formatSize2(usedMemory()) + " [not counting graph(s)]");

		if (! external) {
			info("Allocating result bit vectors...");
			// Allocate vectors that will store the result.
			resultBits = new long[bits.length][];
			resultRegisters = new LongBigList[bits.length];
			for(int i = bits.length; i-- != 0;) resultRegisters[i] = (LongArrayBitVector.wrap(resultBits[i] = new long[bits[i].length])).asLongBigList(registerSize);
		}
		else {
			resultBits = null;
			resultRegisters = null;
		}

		lock = new ReentrantLock();
		allWaiting = lock.newCondition();
		start = lock.newCondition();
		aliveThreads = this.numberOfThreads;

		if (this.numberOfThreads == 1) (thread[0] = new IterationThread(g, gt, 0)).start();
		else for(int i = 0; i < this.numberOfThreads; i++) (thread[i] = new IterationThread(g.copy(), gt != null ? gt.copy() : null, i)).start();

		// We wait for all threads being read to start.
		lock.lock();
		try {
			while (aliveThreads != 0) allWaiting.await();
		}
		catch (final InterruptedException e) {
			throw new RuntimeException(e);
		}
		finally {
			lock.unlock();
		}
	}

	private void info(final String s) {
		if (pl != null) pl.logger().info(s);
	}

	private long usedMemory() {
		long bytes = 0;
		for (final long[] a : bits) bytes += a.length * (long)Long.BYTES;
		if (! external) bytes *= 2;
		if (sumOfDistances != null) bytes += sumOfDistances.length * (long)Float.BYTES;
		if (sumOfInverseDistances != null) bytes += sumOfInverseDistances.length * (long)Float.BYTES;
		for (int i = discountFunction.length; i-- != 0;) bytes += discountedCentrality[i].length * (long)Float.BYTES;
		if (modifiedCounter != null) bytes += modifiedCounter.length;
		if (modifiedResultCounter != null) bytes += modifiedResultCounter.length;
		if (nextMustBeChecked != null) bytes += nextMustBeChecked.length;
		if (mustBeChecked != null) bytes += mustBeChecked.length;
		return bytes;
	}

	private void ensureOpen() {
		if (closed) throw new IllegalStateException("This " + HyperBall.class.getSimpleName() + " has been closed.");
	}

	/** Initialises the approximator.
	 *
	 * <p>This method must be call before a series of {@linkplain #iterate() iterations}.
	 * Note that it will <em>not</em> change the seed used by the underlying {@link HyperLogLogCounterArray}.
	 *
	 * @see #init(long)
	 */
	public void init() {
		init(seed);
	}

	/** Initialises the approximator, providing a new seed to the underlying {@link HyperLogLogCounterArray}.
	 *
	 * <p>This method must be call before a series of {@linkplain #iterate() iterations}.
	 * @param seed passed to {@link #clear(long)}.
	 */
	public void init(final long seed) {
		ensureOpen();
		info("Clearing all registers...");
		clear(seed);

		if (weight == null) {
			// We load the counter i with node i.
			for(int i = numNodes; i-- != 0;) add(i, i);
		}
		else {
			final XoRoShiRo128PlusRandomGenerator random = new XoRoShiRo128PlusRandomGenerator(seed);
			// We load the counter i with node weight[i] random values.
			for(int i = numNodes; i-- != 0;)
				for(int j = weight[i]; j-- != 0;)
					add(i, random.nextLong());
		}

		iteration = -1;
		completed = systolic = local = preLocal = false;

		if (! external) for(final long[] a: resultBits) Arrays.fill(a, 0);

		if (sumOfDistances != null) Arrays.fill(sumOfDistances, 0);
		if (sumOfInverseDistances != null) Arrays.fill(sumOfInverseDistances, 0);
		for (int i = 0; i < discountFunction.length; i++) Arrays.fill(discountedCentrality[i], 0);

		// The initial value (the iteration for this value does not actually happen).
		neighbourhoodFunction.add(last = numNodes);

		Arrays.fill(modifiedCounter, true); // Initially, all counters are modified.

		if (pl != null) {
			pl.displayFreeMemory = true;
			pl.itemsName = "iterates";
			pl.start("Iterating...");
		}
	}

	public long[] expand(final long[] bits) {
		int remaining = Long.SIZE;
		int word = 0;
		long curr = bits[word];
		final long[] result = new long[m];
		final int registerSize = this.registerSize;
		final int mask = (1 << registerSize) - 1;

		for (int j = m; j-- != 0;) {
			if (remaining >= registerSize) {
				result[j] = curr & mask;
				curr >>>= registerSize;
				remaining -= registerSize;
			}
			else {
				result[j] = (curr | bits[++word] << remaining) & mask;
				curr = bits[word] >>> registerSize - remaining;
				remaining += Long.SIZE - registerSize;
			}
		}

		return result;
	}

	@Override
	public void close() throws IOException {
		if (closed) return;
		closed = true;

		lock.lock();
		try {
			completed = true;
			for(final IterationThread t: thread) t.threadShouldWait = false;
			start.signalAll();
		}
		finally {
			lock.unlock();
		}

		for(final Thread t: thread)
			try {
				t.join();
			}
			catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}

			if (external) {
				randomAccessFile.close();
				fileChannel.close();
				updateFile.delete();
			}
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

	private final class IterationThread extends Thread {
		/** A copy of the graph for this thread only. */
		private final ImmutableGraph g;
		/** A copy of the transpose graph for this thread only. */
		private final ImmutableGraph gt;
		/** The index of this thread (just used to identify the thread). */
		private final int index;
		/** True if we should wait for the end of the current phase. */
		private boolean threadShouldWait;

		/** Create a new iteration thread.
		 * @param index the index of this thread (just used to identify the thread).
		 */
		private IterationThread(final ImmutableGraph g, final ImmutableGraph gt, final int index) {
			this.g = g;
			this.gt = gt;
			this.index = index;
		}

		private final boolean synchronize(final int phase) throws InterruptedException {
			lock.lock();
			try {
				threadShouldWait = true;
				if (--aliveThreads == 0) allWaiting.signal();
				if (aliveThreads < 0) throw new IllegalStateException();
				while (threadShouldWait) start.await();
				if (completed) return true;
				if (phase != HyperBall.this.phase) throw new IllegalStateException("Main thread is in phase " + HyperBall.this.phase + ", but thread " + index + " is heading to phase " + phase);
				return false;
			}
			finally {
				lock.unlock();
			}
		}


		@Override
		public void run() {
			try {
				// Lots of local caching.
				final int registerSize = HyperBall.this.registerSize;
				final int counterLongwords = HyperBall.this.counterLongwords;
				final boolean external = HyperBall.this.external;
				final ImmutableGraph g = this.g;
				final boolean doSumOfDistances = HyperBall.this.doSumOfDistances;
				final boolean doSumOfInverseDistances = HyperBall.this.doSumOfInverseDistances;
				final int numberOfDiscountFunctions = HyperBall.this.discountFunction.length;
				final boolean doCentrality = doSumOfDistances || doSumOfInverseDistances || numberOfDiscountFunctions != 0;

				final long[] accumulator = new long[counterLongwords];
				final long[] mask = new long[counterLongwords];

				final long t[] = new long[counterLongwords];
				final long prevT[] = new long[counterLongwords];
				final long u[] = new long[counterLongwords];

				final ByteBuffer byteBuffer = external ? ByteBuffer.allocate(Long.BYTES * bufferSize * (counterLongwords + 1)) : null;
				if (external) byteBuffer.clear();

				for(;;) {

					if (synchronize(0)) return;

					// These variables might change across executions of the loop body.
					final long granularity = HyperBall.this.adaptiveGranularity;
					final long arcGranularity = (long)Math.ceil((double)numArcs * granularity / numNodes);
					final long bits[][] = HyperBall.this.bits;
					final long resultBits[][] = HyperBall.this.resultBits;
					final boolean[] modifiedCounter = HyperBall.this.modifiedCounter;
					final boolean[] modifiedResultCounter = HyperBall.this.modifiedResultCounter;
					final boolean[] mustBeChecked = HyperBall.this.mustBeChecked;
					final boolean[] nextMustBeChecked = HyperBall.this.nextMustBeChecked;
					final boolean systolic = HyperBall.this.systolic;
					final boolean local = HyperBall.this.local;
					final boolean preLocal = HyperBall.this.preLocal;
					final int localCheckShift = 6 - log2m;
					final int[] localCheckList = HyperBall.this.localCheckList;
					final IntSet localNextMustBeChecked = HyperBall.this.localNextMustBeChecked;

					int start = -1;
					int end = -1;
					int modified = 0; // The number of registers that have been modified during the computation of the present task.
					int unwritten = 0; // The number of counters not written to disk.

					// In a local computation tasks are based on the content of localCheckList.
					final int upperLimit = local ? localCheckList.length : numNodes;

					/* During standard iterations, cumulates the neighbourhood function for the nodes scanned
					 * by this thread. During systolic iterations, cumulates the *increase* of the
					 * neighbourhood function for the nodes scanned by this thread. */
					final KahanSummation neighbourhoodFunctionDelta = new KahanSummation();

					for(;;) {

						// Try to get another piece of work.
						synchronized(HyperBall.this.cumulativeOutdegrees) {
							if (nextNode == upperLimit) break;
							start = nextNode;
							if (local) {
								nextNode++;
								if (log2m < 6) {
									/* We cannot split the list unless the boundary crosses a
									 * multiple of 1 << localCheckShift. Otherwise, we might create
									 * race conditions with other threads. */
									while(nextNode < upperLimit) {
										if (((localCheckList[nextNode - 1] ^ localCheckList[nextNode]) >>> localCheckShift) != 0) break;
										nextNode++;
									}
								}
							}
							else {
								final long target = nextArcs + arcGranularity;
								if (target >= numArcs) nextNode = numNodes;
								else {
									nextArcs = cumulativeOutdegrees.skipTo(target);
									nextNode = cumulativeOutdegrees.currentIndex();
								}
							}
							end = nextNode;
						}

						final NodeIterator nodeIterator = local || systolic ? null : g.nodeIterator(start);
						long arcs = 0;

						for(int i = start; i < end; i++) {
							final int node = local ? localCheckList[i] : i;
							/* The three cases in which we enumerate successors:
							 * 1) A non-systolic computation (we don't know anything, so we enumerate).
							 * 2) A systolic, local computation (the node is by definition to be checked, as it comes from the local check list).
							 * 3) A systolic, non-local computation in which the node should be checked.
							 */
							if (! systolic || local || mustBeChecked[node]) {
								int d;
								int[] successor = null;
								LazyIntIterator successors = null;

								if (local || systolic) {
									d = g.outdegree(node);
									successors = g.successors(node);
								}
								else {
									nodeIterator.nextInt();
									d = nodeIterator.outdegree();
									successor = nodeIterator.successorArray();
								}

								final int chunk = chunk(node);
								getCounter(bits[chunk], node, t);
								// Caches t's values into prevT
								System.arraycopy(t, 0, prevT, 0, counterLongwords);

								boolean counterModified = false;

								for(int j = d; j-- != 0;) {
									final int s = local || systolic ? successors.nextInt() : successor[j];
									/* Neither self-loops nor unmodified counter do influence the computation. */
									if (s != node && modifiedCounter[s]) {
										counterModified = true; // This is just to mark that we entered the loop at least once.
										getCounter(bits[chunk(s)], s, u);
										max(t, u, accumulator, mask);
									}
								}

								arcs += d;

								if (ASSERTS)  {
									final LongBigList test = LongArrayBitVector.wrap(t).asLongBigList(registerSize);
									for(int rr = 0; rr < m; rr++) {
										int max = (int)registers[chunk(node)].getLong(((long)node << log2m) + rr);
										if (local || systolic) successors = g.successors(node);
										for(int j = d; j-- != 0;) {
											final int s = local || systolic ? successors.nextInt() : successor[j];
											max = Math.max(max, (int)registers[chunk(s)].getLong(((long)s << log2m) + rr));
										}
										assert max == test.getLong(rr) : max + "!=" + test.getLong(rr) + " [" + rr + "]";
									}
								}

								if (counterModified) {
									/* If we enter this branch, we have maximised with at least one successor.
									 * We must thus check explicitly whether we have modified the counter. */
									counterModified = false;
									for(int p = counterLongwords; p-- != 0;)
										if (prevT[p] != t[p]) {
											counterModified = true;
											break;
										}
								}

								double post = Double.NaN;

								/* We need the counter value only if the iteration is standard (as we're going to
								 * compute the neighbourhood function cumulating actual values, and not deltas) or
								 * if the counter was actually modified (as we're going to cumulate the neighbourhood
								 * function delta, or at least some centrality). */
								if (! systolic || counterModified) post = count(t, 0);
								if (! systolic) neighbourhoodFunctionDelta.add(post);

								if (counterModified) {
									final long[] prevRegister = expand(prevT);
									final long[] register = expand(t);
									long s = 0, snz = 0;
									int nonzero = 0;
									for(int p = 0; p < m; p++) {
										final long r = register[p], pr = prevRegister[p];
										s += r;
										if (r != pr) {
											nonzero++;
											snz += r;
										}
									}
									s = (s + m / 2) >>> log2m;
									snz = nonzero == 0 ? 0 : (snz + nonzero / 2) / nonzero;
								}

								// Here counterModified is true only if the counter was *actually* modified.
								if (counterModified && (systolic || doCentrality)) {
									final double pre = count(node);
									if (systolic) {
										neighbourhoodFunctionDelta.add(-pre);
										neighbourhoodFunctionDelta.add(post);
									}

									if (doCentrality) {
										final double delta = post - pre;
										// Note that this code is executed only for distances > 0.
										if (delta > 0) { // Force monotonicity
											if (doSumOfDistances) sumOfDistances[node] += delta * (iteration + 1);
											if (doSumOfInverseDistances) sumOfInverseDistances[node] += delta / (iteration + 1);
											for (int j = numberOfDiscountFunctions; j-- != 0;) discountedCentrality[j][node] += delta * discountFunction[j].get(iteration + 1);
										}
									}
								}

								if (counterModified) {
									/* We keep track of modified counters in the result if we are
									 * not in external mode (in external mode modified counters are
									 * computed when the update list is reloaded). Note that we must
									 * add the current node to the must-be-checked set for the next
									 * local iteration if it is modified, as it might need a copy to
									 * the result array at the next iteration. */
									if (preLocal) localNextMustBeChecked.add(node);
									if (! external) modifiedResultCounter[node] = true;

									if (systolic) {
										final LazyIntIterator predecessors = gt.successors(node);
										int p;
										/* In systolic computations we must keep track of which counters must
										 * be checked on the next iteration. If we are preparing a local computation,
										 * we do this explicitly, by adding the predecessors of the current
										 * node to a set. Otherwise, we do this implicitly, by setting the
										 * corresponding entry in an array. */
										if (preLocal) while((p = predecessors.nextInt()) != -1) localNextMustBeChecked.add(p);
										else while((p = predecessors.nextInt()) != -1) nextMustBeChecked[p] = true;
									}

									modified++;
								}

								if (external) {
									if (counterModified) {
										byteBuffer.putLong(node);
										for(int p = counterLongwords; p-- != 0;) byteBuffer.putLong(t[p]);

										if (! byteBuffer.hasRemaining()) {
											byteBuffer.flip();
											long time = -System.currentTimeMillis();
											fileChannel.write(byteBuffer);
											time += System.currentTimeMillis();
											totalIoMillis += time;
											numberOfWrites++;
											byteBuffer.clear();
										}
									}
									else unwritten++;
								}
								else {
									/* This is slightly subtle: if a counter is not modified, and
									 * the present value was not a modified value in the first place,
									 * then we can avoid updating the result altogether. */
									if (counterModified || modifiedCounter[node]) setCounter(t, resultBits[chunk], node);
									else unwritten++;
								}
							}
							else if (! external) {
								/* Even if we cannot possibly have changed our value, still our copy
								 * in the result vector might need to be updated because it does not
								 * reflect our current value. */
								if (modifiedCounter[node]) {
									final int chunk = chunk(node);
									transfer(bits[chunk], resultBits[chunk], node);
								}
								else unwritten++;
							}
						}

						// Update the global progress counter.
						HyperBall.this.arcs.addAndGet(arcs);
						nodes.addAndGet(end - start);
					}

					if (external) {
						// If we can avoid at all calling FileChannel.write(), we do so.
						if(byteBuffer.position() != 0) {
							byteBuffer.flip();
							long time = -System.currentTimeMillis();
							fileChannel.write(byteBuffer);
							time += System.currentTimeMillis();
							totalIoMillis += time;
							numberOfWrites++;
							byteBuffer.clear();
						}
					}

					HyperBall.this.modified.addAndGet(modified);
					HyperBall.this.unwritten.addAndGet(unwritten);

					synchronized(HyperBall.this) {
						current += neighbourhoodFunctionDelta.value();
					}

					if (external) {
						synchronize(1);
						/* Read into memory newly computed counters, updating modifiedCounter.
						 * Note that if m is less than 64 copyFromLocal(), being unsynchronised, might
						 * cause race conditions (when maximising each thread writes in a longword-aligned
						 * block of memory, so no race conditions can arise). Since synchronisation would
						 * lead to significant contention (as we cannot synchronise at a level finer than
						 * a bit vector, and update lists might be quite dense and local), we prefer simply
						 * to do the update with thread 0 only. */
						if (index == 0 || m >= Long.SIZE) for(;;) {
							byteBuffer.clear();
							if (fileChannel.read(byteBuffer) <= 0) break;
							byteBuffer.flip();
							while(byteBuffer.hasRemaining()) {
								final int node = (int)byteBuffer.getLong();
								for(int p = counterLongwords; p-- != 0;) t[p] = byteBuffer.getLong();
								setCounter(t, bits[chunk(node)], node);
								modifiedCounter[node] = true;
							}
						}
					}

				}
			}
			catch(final Throwable t) {
				t.printStackTrace();
				threadThrowable = t;
				lock.lock();
				try {
					if (--aliveThreads == 0) allWaiting.signal();
				}
				finally {
					lock.unlock();
				}
			}
		}

		@Override
		public String toString() {
			return "Thread " + index;
		}
	}

	/** Performs a new iteration of HyperBall. */
	public void iterate() throws IOException {
		ensureOpen();
		try {
			iteration++;

			// Let us record whether the previous computation was systolic or local.
			final boolean previousWasSystolic = systolic;
			final boolean previousWasLocal = local;

			/* If less than one fourth of the nodes have been modified, and we have the transpose,
			 * it is time to pass to a systolic computation. */
			systolic = gotTranspose && iteration > 0 && modified.get() < numNodes / 4;

			/* Non-systolic computations add up the value of all counter.
			 * Systolic computations modify the last value by compensating for each modified counter. */
			current = systolic ? last : 0;

			// If we completed the last iteration in pre-local mode, we MUST run in local mode.
			local = preLocal;

			// We run in pre-local mode if we are systolic and few nodes where modified.
			preLocal = systolic && modified.get() < .1 * numNodes * numNodes / numArcs;

			info("Starting " + (systolic ? "systolic iteration (local: " + local + "; pre-local: " + preLocal + ")"  : "standard " + (external ? "external " : "") + "iteration"));

			if (! external) {
				if (previousWasLocal) for(final int x: localCheckList) modifiedResultCounter[x] = false;
				else Arrays.fill(modifiedResultCounter, false);
			}

			if (local) {
				/* In case of a local computation, we convert the set of must-be-checked for the
				 * next iteration into a check list. */
				localCheckList = localNextMustBeChecked.toIntArray();
				IntArrays.parallelQuickSort(localCheckList);
				localNextMustBeChecked.clear();
			}
			else if (systolic) {
				// Systolic, non-local computations store the could-be-modified set implicitly into this array.
				Arrays.fill(nextMustBeChecked, false);
				// If the previous computation wasn't systolic, we must assume that all registers could have changed.
				if (! previousWasSystolic) Arrays.fill(mustBeChecked, true);
			}

			adaptiveGranularity = granularity;

			if (numberOfThreads > 1 && ! local) {
				if (iteration > 0) {
					adaptiveGranularity = (long)Math.min(Math.max(1, numNodes / numberOfThreads), granularity * (numNodes / Math.max(1., modified())));
					adaptiveGranularity = (adaptiveGranularity + Long.SIZE - 1) & -Long.SIZE;
				}
				info("Adaptive granularity for this iteration: " + adaptiveGranularity);
			}

			modified.set(0);
			totalIoMillis = 0;
			numberOfWrites = 0;
			final ProgressLogger npl = pl == null ? null : new ProgressLogger(LOGGER, 1, TimeUnit.MINUTES, "arcs");

			if (npl != null) {
				arcs.set(0);
				npl.expectedUpdates = systolic || local ? -1 : numArcs;
				npl.start("Scanning graph...");
			}

			nodes.set(0);
			nextArcs = nextNode = 0;
			unwritten.set(0);
			if (external) fileChannel.position(0);

			// Start all threads.
			lock.lock();
			try {
				phase = 0;
				aliveThreads = numberOfThreads;
				for(final IterationThread t: thread) t.threadShouldWait = false;
				start.signalAll();

				// Wait for all threads to complete their tasks, logging some stuff in the mean time.
				while(aliveThreads != 0) {
					allWaiting.await(1, TimeUnit.MINUTES);
					if (threadThrowable != null) throw new RuntimeException(threadThrowable);
					final int aliveThreads = this.aliveThreads;
					if (npl != null && aliveThreads != 0) {
						if (arcs.longValue() != 0) npl.set(arcs.longValue());
						if (external && numberOfWrites > 0) {
							final long time = npl.millis();
							info("Writes: " + numberOfWrites + "; per second: " + Util.format(1000.0 * numberOfWrites / time));
							info("I/O time: " + Util.format((totalIoMillis / 1000.0)) + "s; per write: " + (totalIoMillis / 1000.0) / numberOfWrites + "s");
						}
						if (aliveThreads != 0) info("Alive threads: " + aliveThreads + " (" + Util.format(100.0 * aliveThreads / numberOfThreads) + "%)");
					}
				}
			}
			finally {
				lock.unlock();
			}

			if (npl != null) {
				npl.done(arcs.longValue());
				if (! external) info("Unwritten counters: " + Util.format(unwritten.intValue()) + " (" + Util.format(100.0 * unwritten.intValue() / numNodes) + "%)");
				info("Unmodified counters: " + Util.format(numNodes - modified.intValue()) + " (" + Util.format(100.0 * (numNodes - modified.intValue()) / numNodes) + "%)");
			}

			if (external) {
				if (npl != null) {
					npl.itemsName = "counters";
					npl.start("Updating counters...");
				}

				// Read into memory the newly computed counters.

				fileChannel.truncate(fileChannel.position());
				fileChannel.position(0);

				// In pre-local mode, we do not clear modified counters.
				if (! preLocal) Arrays.fill(modifiedCounter, false);

				lock.lock();
				try {
					phase = 1;
					aliveThreads = numberOfThreads;
					for(final IterationThread t: thread) t.threadShouldWait = false;
					start.signalAll();
					// Wait for all threads to complete the counter update.
					while(aliveThreads != 0) allWaiting.await();
					if (threadThrowable != null) throw new RuntimeException(threadThrowable);
				}
				finally {
					lock.unlock();
				}

				if (npl != null) {
					npl.count = modified();
					npl.done();
				}
			}
			else {
				// Switch the bit vectors.
				for(int i = 0; i < bits.length; i++) {
					if (npl != null) npl.update(bits[i].length);
					final LongBigList r = registers[i];
					registers[i] = resultRegisters[i];
					resultRegisters[i] = r;
					final long[] b = bits[i];
					bits[i] = resultBits[i];
					resultBits[i] = b;
				}

				// Switch modifiedCounters and modifiedResultCounters.
				final boolean[] t = modifiedCounter;
				modifiedCounter = modifiedResultCounter;
				modifiedResultCounter = t;
			}

			if (systolic) {
				// Switch mustBeChecked and nextMustBeChecked.
				final boolean[] t = mustBeChecked;
				mustBeChecked = nextMustBeChecked;
				nextMustBeChecked = t;
			}

			last = current;
			/* We enforce monotonicity. Non-monotonicity can only be caused
			 * by approximation errors. */
			final double lastOutput = neighbourhoodFunction.getDouble(neighbourhoodFunction.size() - 1);
			if (current < lastOutput) current = lastOutput;
			relativeIncrement = current / lastOutput;

			if (pl != null) {
				pl.logger().info("Pairs: " + current + " (" + current * 100.0 / squareNumNodes + "%)");
				pl.logger().info("Absolute increment: " + (current - lastOutput));
				pl.logger().info("Relative increment: " + relativeIncrement);
			}

			neighbourhoodFunction.add(current);

			if (pl != null) pl.updateAndDisplay();
		}
		catch (final InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/** Returns the number of HyperLogLog counters that were modified by the last call to {@link #iterate()}.
	 *
	 * @return the number of HyperLogLog counters that were modified by the last call to {@link #iterate()}.
	 */
	public int modified() {
		return modified.get();
	}

	/** Runs HyperBall. The computation will stop when {@link #modified()} returns false. */
	public void run() throws IOException {
		run(Long.MAX_VALUE);
	}

	/** Runs HyperBall.
	 *
	 * @param upperBound an upper bound to the number of iterations.
	 */
	public void run(final long upperBound) throws IOException {
		run(upperBound, -1);
	}

	/** Runs HyperBall.
	 *
	 * @param upperBound an upper bound to the number of iterations.
	 * @param threshold a value that will be used to stop the computation by relative increment if the neighbourhood function is being computed; if you specify -1,
	 * the computation will stop when {@link #modified()} returns false.
	 */
	public void run(final long upperBound, final double threshold) throws IOException {
		run(upperBound, threshold, seed);
	}

	/** Runs HyperBall.
	 *
	 * @param upperBound an upper bound to the number of iterations.
	 * @param threshold a value that will be used to stop the computation by relative increment if the neighbourhood function is being computed; if you specify -1,
	 * the computation will stop when {@link #modified()} returns false.
	 * @param seed the random seed passed to {@link HyperLogLogCounterArray#HyperLogLogCounterArray(long, long, int, long)}.
	 */
	public void run(long upperBound, final double threshold, final long seed) throws IOException {
		upperBound = Math.min(upperBound, numNodes);

		init(seed);

		for(long i = 0; i < upperBound; i++) {
			iterate();

			if (modified() == 0) {
				info("Terminating approximation after " + i + " iteration(s) by stabilisation");
				break;
			}

			if (i > 3 && relativeIncrement < (1 + threshold)) {
				info("Terminating approximation after " + i + " iteration(s) by relative bound on the neighbourhood function");
				break;
			}
		}

		if (pl != null) pl.done();
	}

	/** Throws a {@link NotSerializableException}, as this class implements {@link Serializable}
	 * because it extends {@link HyperLogLogCounterArray}, but it's not really. */
	private void writeObject(@SuppressWarnings("unused") final ObjectOutputStream oos) throws IOException {
        throw new NotSerializableException();
    }


	public static void main(final String arg[]) throws IOException, JSAPException, IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
		final SimpleJSAP jsap = new SimpleJSAP(HyperBall.class.getName(), "Runs HyperBall on the given graph, possibly computing positive geometric centralities.\n\nPlease note that to compute negative centralities on directed graphs (which is usually what you want) you have to compute positive centralities on the transpose.",
			new Parameter[] {
			new FlaggedOption("log2m", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'l', "log2m", "The logarithm of the number of registers."),
			new FlaggedOption("upperBound", JSAP.LONGSIZE_PARSER, Long.toString(Long.MAX_VALUE), JSAP.NOT_REQUIRED, 'u', "upper-bound", "An upper bound to the number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, "-1", JSAP.NOT_REQUIRED, 't', "threshold", "A threshold that will be used to stop the computation by relative increment. If it is -1, the iteration will stop only when all registers do not change their value (recommended)."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new FlaggedOption("granularity", JSAP.INTSIZE_PARSER, Integer.toString(DEFAULT_GRANULARITY), JSAP.NOT_REQUIRED, 'g',  "granularity", "The number of node per task in a multicore environment."),
			new FlaggedOption("bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize(DEFAULT_BUFFER_SIZE), JSAP.NOT_REQUIRED, 'b',  "buffer-size", "The size of an I/O buffer in bytes."),
			new FlaggedOption("neighbourhoodFunction", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n',  "neighbourhood-function", "Store an approximation the neighbourhood function in text format."),
			new FlaggedOption("sumOfDistances", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd',  "sum-of-distances", "Store an approximation of the sum of distances from each node as a binary list of floats."),
			new FlaggedOption("harmonicCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'h',  "harmonic-centrality", "Store an approximation of the positive harmonic centrality (the sum of the reciprocals of distances from each node) as a binary list of floats."),
			new FlaggedOption("discountedGainCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'z',  "discounted-gain-centrality", "A positive discounted gain centrality to be approximated and stored; it is specified as O:F where O is the spec of an object of type Int2DoubleFunction and F is the name of the file where the binary list of floats will be stored. The spec can be either the name of a public field of HyperBall, or a constructor invocation of a class implementing Int2DoubleFunction.").setAllowMultipleDeclarations(true),
			new FlaggedOption("closenessCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c',  "closeness-centrality", "Store an approximation of the positive closeness centrality of each node (the reciprocal of sum of the distances from each node) as a binary list of floats. Terminal nodes will have centrality equal to zero."),
			new FlaggedOption("linCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'L',  "lin-centrality", "Store an approximation of the positive Lin centrality of each node (the reciprocal of sum of the distances from each node multiplied by the square of the number of nodes reachable from the node) as a binary list of floats. Terminal nodes will have centrality equal to one."),
			new FlaggedOption("nieminenCentrality", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'N',  "nieminen-centrality", "Store an approximation of the positive Nieminen centrality of each node (the square of the number of nodes reachable from each node minus the sum of the distances from the node) as a binary list of floats."),
			new FlaggedOption("reachable", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r',  "reachable", "Store an approximation of the number of nodes reachable from each node as a binary list of floats."),
			new FlaggedOption("seed", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "seed", "The random seed."),
			new FlaggedOption("weights", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'W', "weights", "A binary list of nonnegative integers representing the weight of each node."),
			new Switch("spec", 's', "spec", "The basename is not a basename but rather a specification of the form <ImmutableGraphImplementation>(arg,arg,...)."),
			new Switch("offline", 'o', "offline", "Do not load the graph in main memory. If this option is used, the graph will be loaded in offline (for one thread) or mapped (for several threads) mode."),
			new Switch("external", 'e', "external", "Use an external dump file instead of core memory to store new counter values. Note that the file might be very large: you might need to set suitably the Java temporary directory (-Djava.io.tmpdir=DIR)."),
			new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("basenamet", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose graph for systolic computations (strongly suggested). If it is equal to <basename>, the graph will be assumed to be symmetric and will be loaded just once."),
			}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean spec = jsapResult.getBoolean("spec");
		final boolean external = jsapResult.getBoolean("external");
		final boolean offline = jsapResult.getBoolean("offline");
		final String neighbourhoodFunctionFile = jsapResult.getString("neighbourhoodFunction");
		final boolean neighbourhoodFunction = jsapResult.userSpecified("neighbourhoodFunction");
		final String sumOfDistancesFile = jsapResult.getString("sumOfDistances");
		final boolean sumOfDistances = jsapResult.userSpecified("sumOfDistances");
		final String harmonicCentralityFile = jsapResult.getString("harmonicCentrality");
		final boolean harmonicCentrality = jsapResult.userSpecified("harmonicCentrality");
		final String closenessCentralityFile = jsapResult.getString("closenessCentrality");
		final boolean closenessCentrality = jsapResult.userSpecified("closenessCentrality");
		final String linCentralityFile = jsapResult.getString("linCentrality");
		final boolean linCentrality = jsapResult.userSpecified("linCentrality");
		final String nieminenCentralityFile = jsapResult.getString("nieminenCentrality");
		final boolean nieminenCentrality = jsapResult.userSpecified("nieminenCentrality");
		final String reachableFile = jsapResult.getString("reachable");
		final boolean reachable = jsapResult.userSpecified("reachable");
		final String weightFile = jsapResult.getString("weights");
		final String basename = jsapResult.getString("basename");
		final String basenamet = jsapResult.getString("basenamet");
		final ProgressLogger pl = new ProgressLogger(LOGGER);
		final int log2m = jsapResult.getInt("log2m");
		final int threads = jsapResult.getInt("threads");
		final int bufferSize = jsapResult.getInt("bufferSize");
		final int granularity = jsapResult.getInt("granularity");
		final long seed = jsapResult.userSpecified("seed") ? jsapResult.getLong("seed") : Util.randomSeed();

		final String[] discountedGainCentralitySpec = jsapResult.getStringArray("discountedGainCentrality");
		final Int2DoubleFunction[] discountFunction = new Int2DoubleFunction[discountedGainCentralitySpec.length];
		final String[] discountedGainCentralityFile = new String[discountedGainCentralitySpec.length];
		for (int i = 0; i < discountedGainCentralitySpec.length; i++) {
			final int pos = discountedGainCentralitySpec[i].indexOf(':');
			if (pos < 0) throw new IllegalArgumentException("Wrong spec <" + discountedGainCentralitySpec[i] + ">");
			discountedGainCentralityFile[i] = discountedGainCentralitySpec[i].substring(pos + 1);
			final String gainSpec = discountedGainCentralitySpec[i].substring(0, pos);
			Int2DoubleFunction candidateFunction;
			try {
				candidateFunction = (Int2DoubleFunction)HyperBall.class.getField(gainSpec).get(null);
			}
			catch (final SecurityException e) {
				throw new IllegalArgumentException("Field " + gainSpec + " exists but cannot be accessed", e);
			}
			catch (final ClassCastException e) {
				throw new IllegalArgumentException("Field " + gainSpec + " exists but it is not of type Int2DoubleFunction", e);
			}
			catch (final NoSuchFieldException e) {
				candidateFunction = null;
			}
			discountFunction[i] = candidateFunction == null? ObjectParser.fromSpec(gainSpec, Int2DoubleFunction.class) : candidateFunction;
		}

		final int[] weight = weightFile != null ? BinIO.loadInts(weightFile) : null;

		final ImmutableGraph graph = spec
				? ObjectParser.fromSpec(basename, ImmutableGraph.class, GraphClassParser.PACKAGE)
				: offline
					? ((numberOfThreads(threads) == 1 && basenamet == null ? ImmutableGraph.loadOffline(basename) : ImmutableGraph.loadMapped(basename, new ProgressLogger())))
					: ImmutableGraph.load(basename, new ProgressLogger());

		final ImmutableGraph grapht = basenamet == null ? null : basenamet.equals(basename) ? graph : spec ? ObjectParser.fromSpec(basenamet, ImmutableGraph.class, GraphClassParser.PACKAGE) :
			offline ? ImmutableGraph.loadMapped(basenamet, new ProgressLogger()) : ImmutableGraph.load(basenamet, new ProgressLogger());

		final HyperBall hyperBall = new HyperBall(graph, grapht, log2m, pl, threads, bufferSize, granularity, external, sumOfDistances || closenessCentrality || linCentrality || nieminenCentrality, harmonicCentrality, discountFunction, weight, seed);
		hyperBall.run(jsapResult.getLong("upperBound"), jsapResult.getDouble("threshold"));
		hyperBall.close();

		if (neighbourhoodFunction) {
			final PrintStream stream = new PrintStream(new FastBufferedOutputStream(new FileOutputStream(neighbourhoodFunctionFile)));
			for(final DoubleIterator i = hyperBall.neighbourhoodFunction.iterator(); i.hasNext();) stream.println(BigDecimal.valueOf(i.nextDouble()).toPlainString());
			stream.close();
		}

		if (sumOfDistances) BinIO.storeFloats(hyperBall.sumOfDistances, sumOfDistancesFile);
		if (harmonicCentrality) BinIO.storeFloats(hyperBall.sumOfInverseDistances, harmonicCentralityFile);
		for (int i = 0; i < discountedGainCentralitySpec.length; i++) BinIO.storeFloats(hyperBall.discountedCentrality[i], discountedGainCentralityFile[i]);
		if (closenessCentrality) {
			final int n = graph.numNodes();
			final DataOutputStream dos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(closenessCentralityFile)));
			for (int i = 0; i < n; i++) dos.writeFloat(hyperBall.sumOfDistances[i] == 0 ? 0 : 1 / hyperBall.sumOfDistances[i]);
			dos.close();
		}
		if (linCentrality) {
			final int n = graph.numNodes();
			final DataOutputStream dos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(linCentralityFile)));
			for (int i = 0; i < n; i++) {
				// Lin's index for isolated nodes is by (our) definition one (it's smaller than any other node).
				if (hyperBall.sumOfDistances[i] == 0) dos.writeFloat(1);
				else {
					final double count = hyperBall.count(i);
					dos.writeFloat((float)(count * count / hyperBall.sumOfDistances[i]));
				}
			}
			dos.close();
		}
		if (nieminenCentrality) {
			final int n = graph.numNodes();
			final DataOutputStream dos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(nieminenCentralityFile)));
			for (int i = 0; i < n; i++) {
				final double count = hyperBall.count(i);
				dos.writeFloat((float)(count * count - hyperBall.sumOfDistances[i]));
			}
			dos.close();
		}
		if (reachable) {
			final int n = graph.numNodes();
			final DataOutputStream dos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(reachableFile)));
			for (int i = 0; i < n; i++) dos.writeFloat((float)hyperBall.count(i));
			dos.close();
		}
	}
}
