/*
 * Copyright (C) 2007-2021 Sebastiano Vigna
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

import java.util.NoSuchElementException;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;

/** A class providing static methods and objects that do useful
 * things with {@linkplain LazyIntIterator lazy integer iterators}. */

public class LazyIntIterators {

	protected LazyIntIterators() {}

	/** An empty lazy iterator. */
	public final static LazyIntIterator EMPTY_ITERATOR = new LazyIntIterator() {
		@Override
		public int nextInt() { return -1; }
		@Override
		public int skip(final int n) { return 0; }
	};

	/** Unwraps the elements returned by a lazy iterator into an array.
	 *
	 * @param lazyIntIterator a lazy integer iterator.
	 * @param array an array.
	 * @return the number of elements unwrapped into <code>array</code> starting from index 0.
	 */
	public static int unwrap(final LazyIntIterator lazyIntIterator, final int array[]) {
		int j, t;
		final int l = array.length;
		for(j = 0; j < l && (t = lazyIntIterator.nextInt()) != -1; j++) array[j] = t;
		return j;
	}

	/** Unwraps the elements returned by a lazy iterator into an array fragment.
	 *
	 * @param lazyIntIterator a lazy integer iterator.
	 * @param array an array.
	 * @param offset the index of the first element ot <code>array</code> to be used.
	 * @param length the maximum number of elements to be unwrapped.
	 * @return the number of elements unwrapped into <code>array</code> starting from index <code>offset</code>.
	 */
	public static int unwrap(final LazyIntIterator lazyIntIterator, final int array[], final int offset, final int length) {
		int j, t;
		final int l = Math.min(length, array.length - offset);
		for(j = 0; j < l && (t = lazyIntIterator.nextInt()) != -1; j++) array[offset + j] = t;
		return j;
	}

	/** Unwraps the elements returned by a lazy iterator into a new array.
	 *
	 * <p>If you need the resulting array to contain the
	 * elements returned by <code>lazyIntIterator</code>, but some more elements set to zero
	 * would cause no harm, consider using {@link #unwrapLoosely(LazyIntIterator)}, which
	 * usually avoids a final call to {@link IntArrays#trim(int[], int)}.
	 *
	 * @param lazyIntIterator a lazy integer iterator.
	 * @return an array containing the elements returned by <code>lazyIntIterator</code>.
	 * @see #unwrapLoosely(LazyIntIterator)
	 */
	public static int[] unwrap(final LazyIntIterator lazyIntIterator) {
		int array[] = new int[16];
		int j = 0, t;

		while((t = lazyIntIterator.nextInt()) != -1) {
			if (j == array.length) array = IntArrays.grow(array, j + 1);
			array[j++] = t;
		}

		return IntArrays.trim(array, j);
	}

	/** Unwraps the elements returned by a lazy iterator into a new array that can contain additional entries set to zero.
	 *
	 * <p>If you need the resulting array to contain <em>exactly</em> the
	 * elements returned by <code>lazyIntIterator</code>, consider using {@link #unwrap(LazyIntIterator)}, but this
	 * method avoids a final call to {@link IntArrays#trim(int[], int)}.
	 *
	 * @param lazyIntIterator a lazy integer iterator.
	 * @return an array containing the elements returned by <code>lazyIntIterator</code>; note
	 * that in general it might contains some final zeroes beyond the elements returned by <code>lazyIntIterator</code>,
	 * so the number of elements actually written into <code>array</code> must be known externally.
	 * @see #unwrap(LazyIntIterator)
	 */
	public static int[] unwrapLoosely(final LazyIntIterator lazyIntIterator) {
		int array[] = new int[16];
		int j = 0, t;

		while((t = lazyIntIterator.nextInt()) != -1) {
			if (j == array.length) array = IntArrays.grow(array, j + 1);
			array[j++] = t;
		}

		return array;
	}

	/** A lazy iterator returning the elements of a given array. */

	private static final class ArrayLazyIntIterator implements LazyIntIterator {
		/** The underlying array. */
		private final int[] a;
		/** The number of valid elements in {@link #a}, starting from 0. */
		private final int length;
		/** The next element of {@link #a} that will be returned. */
		private int pos;

		public ArrayLazyIntIterator(final int a[], final int length) {
			this.a = a;
			this.length = length;
		}

		@Override
		public int nextInt() {
			if (pos == length) return -1;
			return a[pos++];
		}

		@Override
		public int skip(final int n) {
			final int toSkip = Math.min(n, length - pos);
			pos += toSkip;
			return toSkip;
		}
	}

	/** Returns a lazy integer iterator enumerating the given number of elements of an array.
	 *
	 * @param array an array.
	 * @param length the number of elements to enumerate.
	 * @return a lazy integer iterator enumerating the first <code>length</code> elements of <code>array</code>.
	 */

	public static LazyIntIterator wrap(final int array[], final int length) {
		if (length == 0) return EMPTY_ITERATOR;
		return new ArrayLazyIntIterator(array, length);
	}

	/** Returns a lazy integer iterator enumerating the elements of an array.
	 *
	 * @param array an array.
	 * @return a lazy integer iterator enumerating the elements of <code>array</code>.
	 */

	public static LazyIntIterator wrap(final int array[]) {
		return wrap(array, array.length);
	}

	/** An adapter from lazy to eager iteration. */
	private static final class LazyToEagerIntIterator implements IntIterator {
		/** The underlying lazy iterator. */
		private final LazyIntIterator lazyIntIterator;
		/** Whether this iterator has been already advanced, that is, whether {@link #next} is valid. */
		private boolean advanced;
		/** The next value to be returned, if {@link #advanced} is true. */
		private int next;

		public LazyToEagerIntIterator(final LazyIntIterator lazyIntIterator) {
			this.lazyIntIterator = lazyIntIterator;
		}

		@Override
		public boolean hasNext() {
			if (! advanced) {
				advanced = true;
				next = lazyIntIterator.nextInt();
			}
			return next != -1;
		}

		@Override
		public int nextInt() {
			if (! hasNext()) throw new NoSuchElementException();
			advanced = false;
			return next;
		}

		@Override
		public int skip(final int n) {
			if (n == 0) return 0;
			final int increment = advanced ? 1 : 0;
			advanced = false;
			return lazyIntIterator.skip(n - increment) + increment;
		}
	}

	/** Returns an eager {@link IntIterator} enumerating the same elements of
	 * a given lazy integer iterator.
	 *
	 * @param lazyIntIterator a lazy integer iterator.
	 * @return an eager {@link IntIterator} enumerating the same elements of
	 * <code>lazyIntIterator</code>.
	 */

	public static IntIterator eager(final LazyIntIterator lazyIntIterator) {
		return new LazyToEagerIntIterator(lazyIntIterator);
	}


	private static final class EagerToLazyIntIterator implements LazyIntIterator {
		private final IntIterator underlying;


		public EagerToLazyIntIterator(final IntIterator underlying) {
			this.underlying = underlying;
		}

		@Override
		public int nextInt() {
			return underlying.hasNext() ? underlying.nextInt() : -1;
		}

		@Override
		public int skip(final int n) {
			return underlying.skip(n);
		}

	}

	/** Returns a {@link LazyIntIterator} enumerating the same elements of
	 * a given eager integer iterator.
	 *
	 * @param eagerIntIterator an eager integer iterator.
	 * @return a lazy integer iterator enumerating the same elements of
	 * <code>eagerIntIterator</code>.
	 */

	public static LazyIntIterator lazy(final IntIterator eagerIntIterator) {
		return new EagerToLazyIntIterator(eagerIntIterator);
	}
}
