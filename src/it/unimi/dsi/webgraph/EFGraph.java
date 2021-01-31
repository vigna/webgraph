/*
 * Copyright (C) 2013-2021 Sebastiano Vigna
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

import static it.unimi.dsi.bits.Fast.MSBS_STEP_8;
import static it.unimi.dsi.bits.Fast.ONES_STEP_4;
import static it.unimi.dsi.bits.Fast.ONES_STEP_8;
import static it.unimi.dsi.bits.LongArrayBitVector.bit;
import static it.unimi.dsi.bits.LongArrayBitVector.word;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.DecimalFormat;
import java.util.NoSuchElementException;
import java.util.Properties;
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
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import it.unimi.dsi.util.ByteBufferLongBigList;

/** An immutable graph based on the Elias&ndash;Fano representation of monotone sequences.
 *
 * @author Sebastiano Vigna
 */

public class EFGraph extends ImmutableGraph {
	private static final Logger LOGGER = LoggerFactory.getLogger(EFGraph.class);

	/** The standard extension for the graph longword bit stream. */
	public static final String GRAPH_EXTENSION = ".graph";
	/** The standard extension for the graph-offsets bit stream. */
	public static final String OFFSETS_EXTENSION = ".offsets";
	/** The standard extension for the cached {@link LongBigList} containing the graph offsets. */
	public static final String OFFSETS_BIG_LIST_EXTENSION = ".obl";
	/** The default size of the bit cache. */
	public final static int DEFAULT_CACHE_SIZE = 16 * 1024 * 1024;
	/** This number classifies the present graph format. When new features require introducing binary
	 * incompatibilities, this number is bumped so to ensure that old classes do not try to read
	 * graphs they cannot understand. */
	public final static int EFGRAPH_VERSION = 0;
	/** The default base-two logarithm of the quantum. */
	public static final int DEFAULT_LOG_2_QUANTUM = 8;

	/** The number of nodes of the graph. */
	protected final int n;
	/** The upper bound used during the graph construction (greater than or equal to {@link #n}. */
	protected final int upperBound;
	/** The number of arcs of the graph. */
	protected final long m;
	/** The list containing the graph. */
	protected final LongBigList graph;
	/** An Elias&ndash;Fano monotone list containing the pointers of the bit streams stored in
	 * {@link #graph}. */
	protected final LongBigList offsets;
	/** The basename of this graph (or possibly <code>null</code>). */
	protected final CharSequence basename;
	/** A longword bit reader used to read outdegrees. */
	protected final LongWordBitReader outdegreeLongWordBitReader;
	/** The base-two logarithm of the indexing quantum. */
	protected final int log2Quantum;
	/** If not {@link Integer#MIN_VALUE}, the node whose degree is cached in {@link #cachedOutdegree}. */
	protected int cachedNode;
	/** If {@link #cachedNode} is not {@link Integer#MIN_VALUE}, its cached outdegree. */
	protected int cachedOutdegree;
	/** If {@link #cachedNode} is not {@link Integer#MIN_VALUE}, the position immediately after the
	 * coding of the outdegree of {@link #cachedNode}. */
	protected long cachedPointer;

	protected EFGraph(final CharSequence basename, final int n, final long m, final int upperBound, final int log2Quantum, final LongBigList graph, final LongBigList offsets) {
		this.basename = basename;
		this.n = n;
		this.m = m;
		this.upperBound = upperBound;
		this.log2Quantum = log2Quantum;
		this.graph = graph;
		this.offsets = offsets;
		outdegreeLongWordBitReader = new LongWordBitReader(graph, 0);
		cachedNode = Integer.MIN_VALUE;
	}

	@Override
	public CharSequence basename() {
		return basename;
	}

	/** Returns the number of lower bits for the Elias&ndash;Fano encoding of a list of given length,
	 * upper bound and strictness.
	 *
	 * @param length the number of elements of the list.
	 * @param upperBound an upper bound for the elements of the list.
	 * @return the number of bits for the Elias&ndash;Fano encoding of a list with the specified
	 * parameters. */
	public static int lowerBits(final long length, final long upperBound) {
		return length == 0 ? 0 : Math.max(0, Fast.mostSignificantBit(upperBound / length));
	}

	/** Returns the size in bits of forward or skip pointers to the Elias&ndash;Fano encoding of a
	 * list of given length, upper bound and strictness.
	 *
	 * @param length the number of elements of the list.
	 * @param upperBound an upper bound for the elements of the list.
	 * @return the size of bits of forward or skip pointers the Elias&ndash;Fano encoding of a list
	 * with the specified parameters. */
	public static int pointerSize(final long length, final long upperBound) {
		return Math.max(0, Fast.ceilLog2(length + (upperBound >>> lowerBits(length, upperBound))));
	}

	/** Returns the number of forward or skip pointers to the Elias&ndash;Fano encoding of a list of
	 * given length, upper bound and strictness.
	 *
	 * @param length the number of elements of the list.
	 * @param upperBound an upper bound for the elements of the list.
	 * @param log2Quantum the logarithm of the quantum size.
	 * @return an upper bound on the number of skip pointers or the (exact) number of forward
	 * pointers. */
	public static long numberOfPointers(final long length, final long upperBound, final int log2Quantum) {
		if (length == 0) return 0;
		return (upperBound >>> lowerBits(length, upperBound)) >>> log2Quantum;
	}

	protected final static class LongWordCache implements Closeable {
		/** The spill file. */
		private final File spillFile;
		/** A channel opened on {@link #spillFile}. */
		private final FileChannel spillChannel;
		/** A cache for longwords. Will be spilled to {@link #spillChannel} in case more than
		 * {@link #cacheLength} bits are added. */
		private final ByteBuffer cache;
		/** The current bit buffer. */
		private long buffer;
		/** The current number of free bits in {@link #buffer}. */
		private int free;
		/** The length of the cache, in bits. */
		private final long cacheLength;
		/** The number of bits currently stored. */
		private long length;
		/** Whether {@link #spillChannel} should be repositioned at 0 before usage. */
		private boolean spillMustBeRewind;

		@SuppressWarnings("resource")
		public LongWordCache(final int cacheSize, final String suffix) throws IOException {
			spillFile = File.createTempFile(EFGraph.class.getName(), suffix);
			spillFile.deleteOnExit();
			spillChannel = new RandomAccessFile(spillFile, "rw").getChannel();
			cache = ByteBuffer.allocateDirect(cacheSize).order(ByteOrder.nativeOrder());
			cacheLength = cacheSize * 8L;
			free = Long.SIZE;
		}

		private void flushBuffer() throws IOException {
			cache.putLong(buffer);
			if (!cache.hasRemaining()) {
				if (spillMustBeRewind) {
					spillMustBeRewind = false;
					spillChannel.position(0);
				}
				cache.flip();
				spillChannel.write(cache);
				cache.clear();
			}
		}

		public int append(final long value, final int width) throws IOException {
			assert width == Long.SIZE || (-1L << width & value) == 0;
			buffer |= value << -free;
			length += width;

			if (width < free) free -= width;
			else {
				flushBuffer();

				if (width == free) {
					buffer = 0;
					free = Long.SIZE;
				}
				else {
					// free < Long.SIZE
					buffer = value >>> free;
					free = Long.SIZE - width + free; // width > free
				}
			}
			return width;
		}

		public void clear() {
			length = buffer = 0;
			free = Long.SIZE;
			cache.clear();
			spillMustBeRewind = true;
		}

		@Override
		public void close() throws IOException {
			spillChannel.close();
			spillFile.delete();
		}

		public long length() {
			return length;
		}

		public void writeUnary(int l) throws IOException {
			if (l >= free) {
				// Phase 1: align
				l -= free;
				length += free;
				flushBuffer();

				// Phase 2: jump over longwords
				buffer = 0;
				free = Long.SIZE;
				while (l >= Long.SIZE) {
					flushBuffer();
					l -= Long.SIZE;
					length += Long.SIZE;
				}
			}

			append(1L << l, l + 1);
		}

		public long readLong() throws IOException {
			if (!cache.hasRemaining()) {
				cache.clear();
				spillChannel.read(cache);
				cache.flip();
			}
			return cache.getLong();
		}

		public void rewind() throws IOException {
			if (free != Long.SIZE) cache.putLong(buffer);

			if (length > cacheLength) {
				cache.flip();
				spillChannel.write(cache);
				spillChannel.position(0);
				cache.clear();
				spillChannel.read(cache);
				cache.flip();
			}
			else cache.rewind();
		}
	}

	public final static class LongWordOutputBitStream {
		private static final int BUFFER_SIZE = 64 * 1024;

		/** The 64-bit buffer, whose upper {@link #free} bits do not contain data. */
		private long buffer;
		/** The Java nio buffer used to write with prescribed endianness. */
		private final ByteBuffer byteBuffer;
		/** The number of upper free bits in {@link #buffer} (strictly positive). */
		private int free;
		/** The output channel. */
		private final WritableByteChannel writableByteChannel;

		public LongWordOutputBitStream(final WritableByteChannel writableByteChannel, final ByteOrder byteOrder) {
			this.writableByteChannel = writableByteChannel;
			byteBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE).order(byteOrder);
			free = Long.SIZE;
		}

		public int append(final long value, final int width) throws IOException {
			assert width == Long.SIZE || (-1L << width & value) == 0;
			buffer |= value << -free;

			if (width < free) free -= width;
			else {
				byteBuffer.putLong(buffer); // filled
				if (!byteBuffer.hasRemaining()) {
					byteBuffer.flip();
					writableByteChannel.write(byteBuffer);
					byteBuffer.clear();
				}

				if (width == free) {
					buffer = 0;
					free = Long.SIZE;
				}
				else {
					// free < Long.SIZE
					buffer = value >>> free;
					free = Long.SIZE - width + free; // width > free
				}
			}
			return width;
		}

		public long append(final long[] value, final long length) throws IOException {
			long l = length;
			for (int i = 0; l > 0; i++) {
				final int width = (int)Math.min(l, Long.SIZE);
				append(value[i], width);
				l -= width;
			}

			return length;
		}

		public long append(final LongBigList value, final long length) throws IOException {
			long l = length;
			for (long i = 0; l > 0; i++) {
				final int width = (int)Math.min(l, Long.SIZE);
				append(value.getLong(i), width);
				l -= width;
			}

			return length;
		}

		public long append(final LongArrayBitVector bv) throws IOException {
			return append(bv.bits(), bv.length());
		}

		public long append(final LongWordCache cache) throws IOException {
			long l = cache.length();
			cache.rewind();
			while (l > 0) {
				final int width = (int)Math.min(l, Long.SIZE);
				append(cache.readLong(), width);
				l -= width;
			}

			return cache.length();
		}

		public int align() throws IOException {
			if (free != Long.SIZE) {
				byteBuffer.putLong(buffer); // partially filled
				if (!byteBuffer.hasRemaining()) {
					byteBuffer.flip();
					writableByteChannel.write(byteBuffer);
					byteBuffer.clear();
				}

				final int result = free;
				buffer = 0;
				free = Long.SIZE;
				return result;
			}

			return 0;
		}

		public int writeNonZeroGamma(final long value) throws IOException {
			if (value <= 0) throw new IllegalArgumentException("The argument " + value + " is not strictly positive.");
			final int msb = Fast.mostSignificantBit(value);
			final long unary = 1L << msb;
			append(unary, msb + 1);
			append(value ^ unary, msb);
			return 2 * msb + 1;
		}

		public int writeGamma(final long value) throws IOException {
			if (value < 0) throw new IllegalArgumentException("The argument " + value + " is negative.");
			return writeNonZeroGamma(value + 1);
		}

		public void close() throws IOException {
			byteBuffer.putLong(buffer);
			byteBuffer.flip();
			writableByteChannel.write(byteBuffer);
			writableByteChannel.close();
		}
	}

	protected final static class Accumulator implements Closeable {
		/** The minimum size in bytes of a {@link LongWordCache}. */
		private static final int MIN_CACHE_SIZE = 16;
		/** The accumulator for successors (to zeros or ones). */
		private final LongWordCache successors;
		/** The accumulator for high bits. */
		private final LongWordCache upperBits;
		/** The accumulator for low bits. */
		private final LongWordCache lowerBits;
		/** The number of lower bits. */
		private int l;
		/** A mask extracting the {@link #l} lower bits. */
		private long lowerBitsMask;
		/** The number of elements that will be added to this list. */
		private long length;
		/** The current length of the list. */
		private long currentLength;
		/** The current prefix sum (decremented by {@link #currentLength} if {@link #strict} is
		 * true). */
		private long currentPrefixSum;
		/** An upper bound to the sum of all values that will be added to the list (decremented by
		 * {@link #currentLength} if {@link #strict} is true). */
		private long correctedUpperBound;
		/** The logarithm of the indexing quantum. */
		private int log2Quantum;
		/** The indexing quantum. */
		private long quantum;
		/** The size of a pointer (the ceiling of the logarithm of {@link #maxUpperBits}). */
		private int pointerSize;
		/** The last position where a one was set. */
		private long lastOnePosition;
		/** The expected number of points. */
		private long expectedNumberOfPointers;
		/** The number of bits used for the upper-bits array. */
		public long bitsForUpperBits;
		/** The number of bits used for the lower-bits array. */
		public long bitsForLowerBits;
		/** The number of bits used for forward/skip pointers. */
		public long bitsForPointers;

		public Accumulator(int bufferSize, final int log2Quantum) throws IOException {
			// A reasonable logic to allocate space.
			bufferSize = bufferSize & -bufferSize; // Ensure power of 2.
			/* Very approximately, half of the cache for lower, half for upper, and a small fraction
			 * (8/quantum) for pointers. This will generate a much larger cache than expected if
			 * quantum is very small. */
			successors = new LongWordCache(Math.max(MIN_CACHE_SIZE, bufferSize >>> Math.max(3, log2Quantum - 3)), "pointers");
			lowerBits = new LongWordCache(Math.max(MIN_CACHE_SIZE, bufferSize / 2), "lower");
			upperBits = new LongWordCache(Math.max(MIN_CACHE_SIZE, bufferSize / 2), "upper");
		}

		public int lowerBits() {
			return l;
		}

		public int pointerSize() {
			return pointerSize;
		}

		public long numberOfPointers() {
			return expectedNumberOfPointers;
		}

		public void init(final long length, final long upperBound, final boolean strict, final boolean indexZeroes, final int log2Quantum) {
			this.log2Quantum = log2Quantum;
			this.length = length;
			quantum = 1L << log2Quantum;
			successors.clear();
			lowerBits.clear();
			upperBits.clear();
			correctedUpperBound = upperBound - (strict ? length : 0);
			final long correctedLength = length + (!strict && indexZeroes ? 1 : 0); // The length, including the final terminator
			if (correctedUpperBound < 0) throw new IllegalArgumentException();

			currentPrefixSum = 0;
			currentLength = 0;
			lastOnePosition = -1;

			l = EFGraph.lowerBits(correctedLength, upperBound);


			lowerBitsMask = (1L << l) - 1;

			pointerSize = EFGraph.pointerSize(correctedLength, upperBound);
			expectedNumberOfPointers = EFGraph.numberOfPointers(correctedLength, upperBound, log2Quantum);
			// System.err.println("l = " + l + " numberOfPointers = " + expectedNumberOfPointers +
			// " pointerSize = " + pointerSize);
		}

		public void add(final long x) throws IOException {
			if (currentLength != 0 && x == 0) throw new IllegalArgumentException();
			// System.err.println("add(" + x + "), l = " + l + ", length = " + length);
			currentPrefixSum += x;
			if (currentPrefixSum > correctedUpperBound) throw new IllegalArgumentException("Too large prefix sum: " + currentPrefixSum + " >= " + correctedUpperBound);
			if (l != 0) lowerBits.append(currentPrefixSum & lowerBitsMask, l);
			final long onePosition = (currentPrefixSum >>> l) + currentLength;

			upperBits.writeUnary((int)(onePosition - lastOnePosition - 1));

			long zeroesBefore = lastOnePosition - currentLength + 1;
			for (long position = lastOnePosition + (zeroesBefore & -1L << log2Quantum) + quantum - zeroesBefore; position < onePosition; position += quantum, zeroesBefore += quantum)
				successors.append(position + 1, pointerSize);

			lastOnePosition = onePosition;
			currentLength++;
		}

		public long dump(final LongWordOutputBitStream lwobs) throws IOException {
			if (currentLength != length) throw new IllegalStateException();
			// Add last fictional document pointer equal to the number of documents.
			add(correctedUpperBound - currentPrefixSum);
			assert pointerSize == 0 || successors.length() / pointerSize == expectedNumberOfPointers : "Expected " + expectedNumberOfPointers + " pointers, found " + successors.length() / pointerSize;
			// System.err.println("pointerSize :" + pointerSize);
			bitsForPointers = lwobs.append(successors);
			// System.err.println("pointers: " + bitsForPointers);
			bitsForLowerBits = lwobs.append(lowerBits);
			// System.err.println("lower: " + bitsForLowerBits);
			bitsForUpperBits = lwobs.append(upperBits);
			// System.err.println("upper: " + bitsForUpperBits);
			return bitsForLowerBits + bitsForUpperBits + bitsForPointers;
		}

		@Override
		public void close() throws IOException {
			successors.close();
			upperBits.close();
			lowerBits.close();
		}
	}

	/** Creates a new {@link EFGraph} by loading a compressed graph file from disk to memory, with no
	 * progress logger and all offsets.
	 *
	 * @param basename the basename of the graph.
	 * @return a {@link EFGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph. */
	public static EFGraph load(final CharSequence basename) throws IOException {
		return loadInternal(basename, false, null);
	}

	/** Creates a new {@link EFGraph} by loading a compressed graph file from disk to memory, with
	 * all offsets.
	 *
	 * @param basename the basename of the graph.
	 * @param pl a progress logger used while loading the graph, or <code>null</code>.
	 * @return a {@link EFGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph. */
	public static EFGraph load(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return loadInternal(basename, false, pl);
	}

	/** Creates a new {@link EFGraph} by memory-mapping a graph file.
	 *
	 * @param basename the basename of the graph.
	 * @return an {@link EFGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while memory-mapping the graph or reading the
	 * offsets. */
	public static EFGraph loadMapped(final CharSequence basename) throws IOException {
		return loadInternal(basename, true, null);
	}

	/** Creates a new {@link EFGraph} by memory-mapping a graph file.
	 *
	 * @param basename the basename of the graph.
	 * @param pl a progress logger used while loading the offsets, or <code>null</code>.
	 * @return an {@link EFGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while memory-mapping the graph or reading the
	 * offsets. */
	public static EFGraph loadMapped(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return loadInternal(basename, true, pl);
	}

	/** Creates a new {@link EFGraph} by loading a compressed graph file from disk to memory, without
	 * offsets.
	 *
	 * @param basename the basename of the graph.
	 * @param pl a progress logger used while loading the graph, or <code>null</code>.
	 * @return a {@link EFGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the graph.
	 * @deprecated Use {@link #loadOffline(CharSequence, ProgressLogger)} or {@link #loadMapped(CharSequence, ProgressLogger)} instead.
	 */
	@Deprecated
	public static EFGraph loadSequential(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return EFGraph.load(basename, pl);
	}


	/** Creates a new {@link EFGraph} by loading a compressed graph file from disk to memory, with no
	 * progress logger and without offsets.
	 *
	 * @param basename the basename of the graph.
	 * @return a {@link EFGraph} containing the specified graph.
	 * @deprecated Use {@link #loadOffline(CharSequence)} or {@link #loadMapped(CharSequence)} instead.
	 */
	@Deprecated
	public static EFGraph loadSequential(final CharSequence basename) throws IOException {
		return EFGraph.load(basename);
	}

	/** Creates a new {@link EFGraph} by loading just the metadata of a compressed graph file.
	 *
	 * @param basename the basename of the graph.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return a {@link EFGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the metadata. */
	public static EFGraph loadOffline(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return EFGraph.loadMapped(basename, pl);
	}



	/** Creates a new {@link EFGraph} by loading just the metadata of a compressed graph file.
	 *
	 * @param basename the basename of the graph.
	 * @return a {@link EFGraph} containing the specified graph.
	 * @throws IOException if an I/O exception occurs while reading the metadata. */
	public static EFGraph loadOffline(final CharSequence basename) throws IOException {
		return EFGraph.loadMapped(basename, null);
	}

	/** An iterator returning the offsets. */
	private final static class OffsetsLongIterator implements LongIterator {
		private final InputBitStream offsetIbs;
		private final long n;
		private long offset;
		private long i;

		private OffsetsLongIterator(final InputBitStream offsetIbs, final long n) {
			this.offsetIbs = offsetIbs;
			this.n = n;
		}

		@Override
		public boolean hasNext() {
			return i <= n;
		}

		@Override
		public long nextLong() {
			if (!hasNext()) throw new NoSuchElementException();
			i++;
			try {
				return offset += offsetIbs.readLongDelta();
			}
			catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}
	}


	/** Commodity method for loading a big list of binary longs with specified endianness into a
	 * {@linkplain LongBigArrays long big array}.
	 *
	 * @param filename the file containing the longs.
	 * @param byteOrder the endianness of the longs.
	 * @return a big list of longs containing the longs in <code>filename</code>. */
	public static LongBigArrayBigList loadLongBigList(final CharSequence filename, final ByteOrder byteOrder) throws IOException {
		final long length = new File(filename.toString()).length() / Long.BYTES;
		@SuppressWarnings("resource")
		final ReadableByteChannel channel = new FileInputStream(filename.toString()).getChannel();
		final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(64 * 1024).order(byteOrder);
		final LongBuffer longBuffer = byteBuffer.asLongBuffer();
		final long[][] array = LongBigArrays.newBigArray(length);

		long pos = 0;
		while (channel.read(byteBuffer) > 0) {
			byteBuffer.flip();
			final int remainingLongs = byteBuffer.remaining() / Long.BYTES;
			longBuffer.clear();
			longBuffer.limit(remainingLongs);
			longBuffer.get(array[BigArrays.segment(pos)], BigArrays.displacement(pos), remainingLongs);
			pos += remainingLongs;
			byteBuffer.clear();
		}

		channel.close();
		return LongBigArrayBigList.wrap(array);

	}


	/** Loads a compressed graph file from disk into this graph. Note that this method should be
	 * called <em>only</em> on a newly created graph.
	 *
	 * @param basename the basename of the graph.
	 * @param mapped whether we want to memory-map the file.
	 * @param pl a progress logger used while loading the graph, or <code>null</code>.
	 * @return this graph. */
	protected static EFGraph loadInternal(final CharSequence basename, final boolean mapped, final ProgressLogger pl) throws IOException {
		// First of all, we read the property file to get the relevant data.
		final FileInputStream propertyFile = new FileInputStream(basename + PROPERTIES_EXTENSION);
		final Properties properties = new Properties();
		properties.load(propertyFile);
		propertyFile.close();

		// Soft check--we accept big stuff, too.
		if (!EFGraph.class.getName().equals(properties.getProperty(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY).replace("it.unimi.dsi.big.webgraph", "it.unimi.dsi.webgraph"))) throw new IOException(
				"This class (" + EFGraph.class.getName() + ") cannot load a graph stored using class \"" + properties.getProperty(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY) + "\"");

		if (properties.getProperty("version") == null) throw new IOException("Missing format version information");
		else if (Integer.parseInt(properties.getProperty("version")) > EFGRAPH_VERSION) throw new IOException("This graph uses format " + properties.getProperty("version")
				+ ", but this class can understand only graphs up to format " + EFGRAPH_VERSION);
		final long nodes = Long.parseLong(properties.getProperty("nodes"));
		if (nodes > Integer.MAX_VALUE) throw new IllegalArgumentException("The standard version of WebGraph cannot handle graphs with " + nodes + " (>2^31) nodes");
		final int n = (int)nodes;
		final long m = Long.parseLong(properties.getProperty("arcs"));
		final int upperBound = properties.containsKey("upperbound") ? Integer.parseInt(properties.getProperty("upperbound")) : n;
		final long quantum = Long.parseLong(properties.getProperty("quantum"));
		final int log2Quantum = Fast.mostSignificantBit(quantum);
		if (1L << log2Quantum != quantum) throw new IllegalArgumentException("Illegal quantum (must be a power of 2): " + quantum);

		final ByteOrder byteOrder;
		if (properties.get("byteorder").equals(ByteOrder.BIG_ENDIAN.toString())) byteOrder = ByteOrder.BIG_ENDIAN;
		else if (properties.get("byteorder").equals(ByteOrder.LITTLE_ENDIAN.toString())) byteOrder = ByteOrder.LITTLE_ENDIAN;
		else throw new IllegalArgumentException("Unknown byte order " + properties.get("byteorder"));

		final FileInputStream graphIs = new FileInputStream(basename + GRAPH_EXTENSION);
		final LongBigList graph;
		if (mapped) graph = ByteBufferLongBigList.map(graphIs.getChannel(), byteOrder);
		else {
			if (pl != null) {
				pl.itemsName = "bytes";
				pl.start("Loading graph...");
			}

			graph = loadLongBigList(basename + GRAPH_EXTENSION, byteOrder);

			if (pl != null) {
				pl.count = graph.size64() * Long.BYTES;
				pl.done();
			}

			graphIs.close();
		}

		if (pl != null) {
			pl.itemsName = "deltas";
			pl.start("Loading offsets...");
		}

		// We try to load a cached big list.
		final File offsetsBigListFile = new File(basename + OFFSETS_BIG_LIST_EXTENSION);
		LongBigList offsets = null;

		if (offsetsBigListFile.exists()) {
			if (new File(basename + OFFSETS_EXTENSION).lastModified() > offsetsBigListFile.lastModified()) LOGGER
					.warn("A cached long big list of offsets was found, but the corresponding offsets file has a later modification time");
			else try {
				offsets = (LongBigList)BinIO.loadObject(offsetsBigListFile);
			}
			catch (final ClassNotFoundException e) {
				LOGGER.warn("A cached long big list of offsets was found, but its class is unknown", e);
			}
		}

		if (offsets == null) {
			final InputBitStream offsetIbs = new InputBitStream(basename + OFFSETS_EXTENSION);
			offsets = new EliasFanoMonotoneLongBigList(n + 1, graph.size64() * Long.SIZE + 1, new OffsetsLongIterator(offsetIbs, n));
			offsetIbs.close();
		}

		if (pl != null) {
			pl.count = n + 1;
			pl.done();
			if (offsets instanceof EliasFanoMonotoneLongBigList) pl.logger().info("Pointer bits per node: " + Util.format(((EliasFanoMonotoneLongBigList)offsets).numBits() / (n + 1.0)));
		}

		return new EFGraph(basename, n, m, upperBound, log2Quantum, graph, offsets);
	}


	public static void store(final ImmutableGraph graph, final int upperBound, final CharSequence basename, final ProgressLogger pl) throws IOException {
		store(graph, upperBound, basename, DEFAULT_LOG_2_QUANTUM, DEFAULT_CACHE_SIZE, ByteOrder.nativeOrder(), pl);
	}

	public static void store(final ImmutableGraph graph, final CharSequence basename, final ProgressLogger pl) throws IOException {
		store(graph, basename, DEFAULT_LOG_2_QUANTUM, DEFAULT_CACHE_SIZE, ByteOrder.nativeOrder(), pl);
	}

	public static void store(final ImmutableGraph graph, final CharSequence basename) throws IOException {
		store(graph, basename, null);
	}

	private static double stirling(final double n) {
		return n * Math.log(n) - n + (1. / 2) * Math.log(2 * Math.PI * n);
	}

	public static void store(final ImmutableGraph graph, final CharSequence basename, final int log2Quantum, final int cacheSize, final ByteOrder byteOrder, final ProgressLogger pl) throws IOException {
		store(graph, graph.numNodes(), basename, log2Quantum, cacheSize, byteOrder, pl);
	}

	public static void store(final ImmutableGraph graph, final int upperBound, final CharSequence basename, final int log2Quantum, final int cacheSize, final ByteOrder byteOrder, final ProgressLogger pl)
			throws IOException {
		if (log2Quantum < 0) throw new IllegalArgumentException(Integer.toString(log2Quantum));

		final Accumulator successorsAccumulator = new Accumulator(cacheSize, log2Quantum);
		final FileOutputStream graphOs = new FileOutputStream(basename + GRAPH_EXTENSION);
		final FileChannel graphChannel = graphOs.getChannel();
		final LongWordOutputBitStream graphStream = new LongWordOutputBitStream(graphChannel, byteOrder);
		final OutputBitStream offsets = new OutputBitStream(basename + OFFSETS_EXTENSION);

		long numberOfArcs = 0;
		long bitsForOutdegrees = 0;
		long bitsForSuccessors = 0;
		offsets.writeLongDelta(0);

		if (pl != null) {
			pl.itemsName = "nodes";
			try {
				pl.expectedUpdates = graph.numNodes();
			}
			catch (final UnsupportedOperationException ignore) {}
			pl.start("Storing...");
		}

		for (final NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext();) {
			nodeIterator.nextInt();
			final long outdegree = nodeIterator.outdegree();
			numberOfArcs += outdegree;
			long lastSuccessor = 0;
			final int outdegreeBits = graphStream.writeGamma(outdegree);
			bitsForOutdegrees += outdegreeBits;
			successorsAccumulator.init(outdegree, upperBound, false, true, log2Quantum);
			final LazyIntIterator successors = nodeIterator.successors();
			for (long successor; (successor = successors.nextInt()) != -1;) {
				successorsAccumulator.add(successor - lastSuccessor);
				lastSuccessor = successor;
			}

			final long successorsBits = successorsAccumulator.dump(graphStream);
			bitsForSuccessors += successorsBits;
			offsets.writeLongDelta(outdegreeBits + successorsBits);

			if (pl != null) pl.lightUpdate();
		}

		successorsAccumulator.close();
		graphStream.close();
		graphOs.close();
		offsets.close();

		final long n = graph.numNodes();

		if (pl != null) {
			pl.done();
			if (pl.count != n) throw new IllegalStateException("The graph claimed to have " + graph.numNodes() + " nodes, but the node iterator returned " + pl.count);
		}

		final DecimalFormat format = new java.text.DecimalFormat("0.###");
		final long writtenBits = new File(basename + GRAPH_EXTENSION).length() * 8;

		final Properties properties = new Properties();
		properties.setProperty("nodes", String.valueOf(n));
		properties.setProperty("arcs", String.valueOf(numberOfArcs));
		if (upperBound != n) properties.setProperty("upperbound", String.valueOf(upperBound));
		properties.setProperty("quantum", String.valueOf(1L << log2Quantum));
		properties.setProperty("byteorder", byteOrder.toString());
		properties.setProperty("bitsperlink", format.format((double)writtenBits / numberOfArcs));
		properties.setProperty("compratio", format.format(writtenBits * Math.log(2) / (stirling((double)n * n) - stirling(numberOfArcs) - stirling((double)n * n - numberOfArcs))));
		properties.setProperty("bitspernode", format.format((double)writtenBits / n));
		properties.setProperty("avgbitsforoutdegrees", format.format((double)bitsForOutdegrees / n));
		properties.setProperty("bitsforoutdegrees", Long.toString(bitsForOutdegrees));
		properties.setProperty("bitsforsuccessors", Long.toString(bitsForSuccessors));
		properties.setProperty(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY, EFGraph.class.getName());
		properties.setProperty("version", String.valueOf(EFGRAPH_VERSION));
		final FileOutputStream propertyFile = new FileOutputStream(basename + PROPERTIES_EXTENSION);
		properties.store(propertyFile, "EFGraph properties");
		propertyFile.close();
	}


	protected final static class LongWordBitReader {

		private static final boolean DEBUG = false;

		/** The underlying list. */
		private final LongBigList list;
		/** The extraction width for {@link #extract()} and {@link #extract(long)}. */
		private final int l;
		/** {@link Long#SIZE} minus {@link #l}, cached. */
		private final int longSizeMinusl;
		/** The extraction mask for {@link #l} bits. */
		private final long mask;

		/** The 64-bit buffer, whose lower {@link #filled} bits contain data. */
		private long buffer;
		/** The number of lower used bits {@link #buffer}. */
		private int filled;
		/** The current position in the list. */
		private long curr;

		public LongWordBitReader(final LongBigList list, final int l) {
			assert l < Long.SIZE;
			this.list = list;
			this.l = l;
			this.longSizeMinusl = Long.SIZE - l;
			mask = (1L << l) - 1;
			curr = -1;
		}

		public LongWordBitReader position(final long position) {
			if (DEBUG) System.err.println(this + ".position(" + position + ") [buffer = " + Long.toBinaryString(buffer) + ", filled = " + filled + "]");

			buffer = list.getLong(curr = word(position));
			final int bitPosition = bit(position);
			buffer >>>= bitPosition;
			filled = Long.SIZE - bitPosition;

			if (DEBUG) System.err.println(this + ".position() filled: " + filled + " buffer: " + Long.toBinaryString(buffer));
			return this;
		}

		public long position() {
			return curr * Long.SIZE + Long.SIZE - filled;
		}

		private long extractInternal(final int width) {
			if (DEBUG) System.err.println(this + ".extract(" + width + ") [buffer = " + Long.toBinaryString(buffer) + ", filled = " + filled + "]");

			if (width <= filled) {
				final long result = buffer & (1L << width) - 1;
				filled -= width;
				buffer >>>= width;
				return result;
			}
			else {
				long result = buffer;
				buffer = list.getLong(++curr);

				final int remainder = width - filled;
				// Note that this WON'T WORK if remainder == Long.SIZE, but that's not going to
				// happen.
				result |= (buffer & (1L << remainder) - 1) << filled;
				buffer >>>= remainder;
				filled = Long.SIZE - remainder;
				return result;
			}
		}

		public long extract() {
			if (DEBUG) System.err.println(this + ".extract() " + l + " bits [buffer = " + Long.toBinaryString(buffer) + ", filled = " + filled + "]");

			if (l <= filled) {
				final long result = buffer & mask;
				filled -= l;
				buffer >>>= l;
				return result;
			}
			else {
				long result = buffer;
				buffer = list.getLong(++curr);
				result |= buffer << filled & mask;
				// Note that this WON'T WORK if remainder == Long.SIZE, but that's not going to
				// happen.
				buffer >>>= l - filled;
				filled += longSizeMinusl;
				return result;
			}
		}

		public long extract(final long position) {
			if (DEBUG) System.err.println(this + ".extract(" + position + ") [l=" + l + "]");

			final int bitPosition = bit(position);
			final int totalOffset = bitPosition + l;
			final long result = list.getLong(curr = word(position)) >>> bitPosition;

			if (totalOffset <= Long.SIZE) {
				buffer = result >>> l;
				filled = Long.SIZE - totalOffset;
				return result & mask;
			}

			final long t = list.getLong(++curr);

			buffer = t >>> totalOffset;
			filled = 2 * Long.SIZE - totalOffset;

			return result | t << -bitPosition & mask;
		}

		public int readUnary() {
			if (DEBUG) System.err.println(this + ".readUnary() [buffer = " + Long.toBinaryString(buffer) + ", filled = " + filled + "]");

			int accumulated = 0;

			for (;;) {
				if (buffer != 0) {
					final int msb = Long.numberOfTrailingZeros(buffer);
					filled -= msb + 1;
					/* msb + 1 can be Long.SIZE, so we must break down the shift. */
					buffer >>>= msb;
					buffer >>>= 1;
					if (DEBUG) System.err.println(this + ".readUnary() => " + (msb + accumulated));
					return msb + accumulated;
				}
				accumulated += filled;
				buffer = list.getLong(++curr);
				filled = Long.SIZE;
			}

		}

		public long readNonZeroGamma() {
			final int msb = readUnary();
			return extractInternal(msb) | (1L << msb);
		}

		public long readGamma() {
			return readNonZeroGamma() - 1;
		}
	}


	@Override
	public int numNodes() {
		return n;
	}

	@Override
	public long numArcs() {
		return m;
	}

	@Override
	public boolean randomAccess() {
		return true;
	}

	@Override
	public boolean hasCopiableIterators() {
		return true;
	}

	@Override
	public int outdegree(final int x) {
		if (x == cachedNode) return cachedOutdegree;
		cachedOutdegree = (int)outdegreeLongWordBitReader.position(offsets.getLong(cachedNode = x)).readGamma();
		cachedPointer = outdegreeLongWordBitReader.position();
		return cachedOutdegree;
	}


	protected final static class EliasFanoSuccessorReader extends AbstractLazyIntIterator implements LazyIntSkippableIterator {
		private final static int SKIPPING_THRESHOLD = 8;
		/** The number of nodes in the graph. */
		private final long n;
		/** The upper bound used at construction time. */
		private final int upperBound;
		/** The underlying list. */
		protected final LongBigList graph;
		/** The longword bit reader for pointers. */
		protected final LongWordBitReader skipPointers;
		/** The starting position of the pointers. */
		protected final long skipPointersStart;
		/** The starting position of the upper bits. */
		protected final long upperBitsStart;
		/** The longword bit reader for the lower bits. */
		private final LongWordBitReader lowerBits;
		/** The starting position of the lower bits. */
		private final long lowerBitsStart;
		/** The logarithm of the quantum, cached from the graph. */
		protected final int log2Quantum;
		/** The quantum, cached from the graph. */
		protected final int quantum;
		/** The size of a pointer. */
		protected final int pointerSize;
		/** The outdegree. */
		protected final long outdegree;
		/** The 64-bit window. */
		protected long window;
		/** The current word position in the list of upper bits. */
		protected long curr;
		/** The index of the current prefix sum. */
		public long currentIndex;
		/** The number of lower bits. */
		private final int l;
		/** The last value returned by {@link #nextInt()}, {@link Integer#MIN_VALUE} if the list has
		 * never be accessed, or {@link LazyIntSkippableIterator#END_OF_LIST} if the list has been
		 * exhausted. */
		private int last;

		public EliasFanoSuccessorReader(final long n, final int upperBound, final LongBigList graph, final long outdegree, final long skipPointersStart, final int log2Quantum) {
			this.n = n;
			this.upperBound = upperBound;
			this.graph = graph;
			this.log2Quantum = log2Quantum;
			this.quantum = 1 << log2Quantum;
			this.outdegree = outdegree;
			this.skipPointersStart = skipPointersStart;

			l = lowerBits(outdegree + 1, upperBound);
			final long numberOfPointers = numberOfPointers(outdegree + 1, upperBound, log2Quantum);
			pointerSize = pointerSize(outdegree + 1, upperBound);

			lowerBitsStart = skipPointersStart + pointerSize * numberOfPointers;
			upperBitsStart = lowerBitsStart + l * (outdegree + 1);

			skipPointers = numberOfPointers == 0 ? null : new LongWordBitReader(graph, pointerSize);
			(lowerBits = new LongWordBitReader(graph, l)).position(lowerBitsStart);
			position(upperBitsStart);
			last = Integer.MIN_VALUE;
		}

		private void position(final long position) {
			window = graph.getLong(curr = word(position)) & -1L << position;
		}

		private long getNextUpperBits() {
			while (window == 0)
				window = graph.getLong(++curr);
			final long upperBits = curr * Long.SIZE + Long.numberOfTrailingZeros(window) - currentIndex++ - upperBitsStart;
			window &= window - 1;
			return upperBits;
		}

		@Override
		public int nextInt() {
			if (currentIndex >= outdegree) {
				last = END_OF_LIST;
				return -1;
			}
			return last = (int)(getNextUpperBits() << l | lowerBits.extract());
		}

		@Override
		public int skipTo(final int lowerBound) {
			if (lowerBound <= last) return last;
			final long zeroesToSkip = lowerBound >>> l;
			long delta = zeroesToSkip - ((last & (-1 >>> 1)) >>> l); // This catches last =
																			// Integer.MIN_VALUE and
																			// turns it into 0
			assert delta >= 0;

			if (delta < SKIPPING_THRESHOLD) {
				do
					nextInt();
				while (last < lowerBound);
				return last == n ? last = END_OF_LIST : last;
			}

			if (delta > quantum) {
				final long block = zeroesToSkip >>> log2Quantum;
				assert block > 0;
				assert block <= numberOfPointers(outdegree + 1, upperBound, log2Quantum);
				final long blockZeroes = block << log2Quantum;
				final long skip = skipPointers.extract(skipPointersStart + (block - 1) * pointerSize);
				assert skip != 0;
				position(upperBitsStart + skip);
				currentIndex = skip - blockZeroes;
				delta = zeroesToSkip - curr * Long.SIZE + currentIndex + upperBitsStart;
			}

			assert delta >= 0 : delta;

			for (int bitCount; (bitCount = Long.bitCount(~window)) < delta;) {
				window = graph.getLong(++curr);
				delta -= bitCount;
				currentIndex += Long.SIZE - bitCount;
			}

			/* Note that for delta == 1 the following code is a NOP, but the test for zero is so
			 * faster that it is not worth replacing with a > 1. Predecrementing won't work as delta
			 * might be zero. */
			if (delta-- != 0) {
				// Phase 1: sums by byte
				final long word = ~window;
				assert delta < Long.bitCount(word) : delta + " >= " + Long.bitCount(word);
				long byteSums = word - ((word & 0xa * ONES_STEP_4) >>> 1);
				byteSums = (byteSums & 3 * ONES_STEP_4) + ((byteSums >>> 2) & 3 * ONES_STEP_4);
				byteSums = (byteSums + (byteSums >>> 4)) & 0x0f * ONES_STEP_8;
				byteSums *= ONES_STEP_8;

				// Phase 2: compare each byte sum with delta to obtain the relevant byte
				final long rankStep8 = delta * ONES_STEP_8;
				final long byteOffset = (((((rankStep8 | MSBS_STEP_8) - byteSums) & MSBS_STEP_8) >>> 7) * ONES_STEP_8 >>> 53) & ~0x7;

				final int byteRank = (int)(delta - (((byteSums << 8) >>> byteOffset) & 0xFF));

				final int select = (int)(byteOffset + Fast.selectInByte[(int)(word >>> byteOffset & 0xFF) | byteRank << 8]);

				// We cancel up to, but not including, the target one.
				window &= -1L << select;
				currentIndex += select - delta;
			}

			final long lower = lowerBits.extract(lowerBitsStart + l * currentIndex);
			last = (int)(getNextUpperBits() << l | lower);

			for (;;) {
				if (last >= lowerBound) return last == n ? last = END_OF_LIST : last;
				nextInt();
			}
		}

		@Override
		public String toString() {
			return this.getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(this));
		}
	}

	@Override
	public LazyIntSkippableIterator successors(final int x) {
		return new EliasFanoSuccessorReader(n, upperBound, graph, outdegree(x), cachedPointer, log2Quantum);
	}

	@Override
	public EFGraph copy() {
		return new EFGraph(basename, n, m, upperBound, log2Quantum, graph instanceof ByteBufferLongBigList ? ((ByteBufferLongBigList)graph).copy() : graph, offsets);
	}

	public static void main(final String args[]) throws SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, JSAPException, ClassNotFoundException,
			InstantiationException {
		String source, dest;
		Class<?> graphClass;

		final SimpleJSAP jsap = new SimpleJSAP(
				BVGraph.class.getName(),
				"Compresses a graph using the Elias-Fano representation. Source and destination are basenames from which suitable filenames will be stemmed; alternatively, if the suitable option was specified, source is a spec (see below). For more information about the compression techniques, see the Javadoc documentation.",
				new Parameter[] {
						new FlaggedOption("graphClass", GraphClassParser.getParser(), null, JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java class for the source graph."),
						new Switch("spec", 's', "spec", "The source is not a basename but rather a specification of the form <ImmutableGraphImplementation>(arg,arg,...)."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval",
								"The minimum time interval between activity logs in milliseconds."),
						new FlaggedOption("log2Quantum", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_LOG_2_QUANTUM), JSAP.NOT_REQUIRED, 'q', "--log2-quantum",
								"The base-two logarithm of the indexing quantum."),
						new Switch("offline", 'o', "offline", "No-op for backward compatibility."),
						new Switch("once", '1', "once", "Use the read-once load method to read a graph from standard input."),
						new Switch("list", 'L', "list", "Precomputes an Elias-Fano list of offsets for the source graph."),
						new Switch("fixedWidthList", 'F', "fixed-width-list", "Precomputes a list of fixed-width offsets for the source graph."),
						new UnflaggedOption("sourceBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY,
								"The basename of the source graph, or a source spec if --spec was given; it is immaterial when --once is specified."),
						new UnflaggedOption("destBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY,
								"The basename of the destination graph; if omitted, no recompression is performed. This is useful in conjunction with --offsets and --list."),
				}
				);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean once = jsapResult.getBoolean("once");
		final boolean spec = jsapResult.getBoolean("spec");
		final boolean list = jsapResult.getBoolean("list");
		final boolean fixedWidthList = jsapResult.getBoolean("fixedWidthList");
		final int log2Quantum = jsapResult.getInt("log2Quantum");
		graphClass = jsapResult.getClass("graphClass");
		source = jsapResult.getString("sourceBasename");
		dest = jsapResult.getString("destBasename");

		final ImmutableGraph graph;
		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);

		if (graphClass != null) {
			if (spec) {
				System.err.println("Options --graph-class and --spec are incompatible");
				System.exit(1);
			}
			if (once) graph = (ImmutableGraph)graphClass.getMethod(LoadMethod.ONCE.toMethod(), InputStream.class).invoke(null, System.in);
			else graph = (ImmutableGraph)graphClass.getMethod(LoadMethod.OFFLINE.toMethod(), CharSequence.class).invoke(null, source);
		}
		else {
			if (!spec) graph = once ? ImmutableGraph.loadOnce(System.in) : ImmutableGraph.loadOffline(source, pl);
			else graph = ObjectParser.fromSpec(source, ImmutableGraph.class, GraphClassParser.PACKAGE);
		}

		if (dest != null) {
			if (list || fixedWidthList) throw new IllegalArgumentException("You cannot specify a destination graph with these options");
			EFGraph.store(graph, dest, log2Quantum, DEFAULT_CACHE_SIZE, ByteOrder.nativeOrder(), pl);
		}
		else {
			if (!(graph instanceof EFGraph)) throw new IllegalArgumentException("The source graph is not an EFGraph");
			final InputBitStream offsets = new InputBitStream(graph.basename() + OFFSETS_EXTENSION);
			final long sizeInBits = new File(graph.basename() + GRAPH_EXTENSION).length() * Byte.SIZE + 1;
			final OffsetsLongIterator offsetsIterator = new OffsetsLongIterator(offsets, graph.numNodes());
			if (list) {
				BinIO.storeObject(new EliasFanoMonotoneLongBigList(graph.numNodes() + 1, sizeInBits, offsetsIterator), graph.basename() + OFFSETS_BIG_LIST_EXTENSION);
			}
			else if (fixedWidthList) {
				final LongBigList t = LongArrayBitVector.getInstance().asLongBigList(Fast.length(sizeInBits));
				while (offsetsIterator.hasNext())
					t.add(offsetsIterator.nextLong());
				BinIO.storeObject(t, graph.basename() + OFFSETS_BIG_LIST_EXTENSION);
			}
			offsets.close();
		}
	}
}
