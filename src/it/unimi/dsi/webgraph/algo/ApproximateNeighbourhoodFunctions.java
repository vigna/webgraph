/*
 * Copyright (C) 2011-2023 Paolo Boldi and Sebastiano Vigna
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

import java.math.BigDecimal;
import java.util.Arrays;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterators;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.stat.Jackknife;
import it.unimi.dsi.stat.Jackknife.Statistic;

/** Static methods and objects that manipulate approximate neighbourhood functions.
 *
 * <p>A number of {@linkplain Statistic statistics} that can be used with {@link Jackknife}, such as
 * {@link #CDF}, {@link #AVERAGE_DISTANCE}, {@link #HARMONIC_DIAMETER} and {@link #SPID} are available.
 */
public class ApproximateNeighbourhoodFunctions {

	private ApproximateNeighbourhoodFunctions() {}

	/** Combines several approximate neighbourhood functions for the same
	 * graph by averaging their values.
	 *
	 * <p>Note that the resulting approximate neighbourhood function has its standard
	 * deviation reduced by the square root of the number of samples (the standard error). However,
	 * if the cumulative distribution function has to be computed instead, calling this method and dividing
	 * all values by the last value is not the best approach, as it leads to a biased estimate.
	 * Rather, the samples should be combined using the {@linkplain Jackknife jackknife} and
	 * the {@link #CDF} statistic.
	 *
	 * <p>If you want to obtain estimates on the standard error of each data point, please consider using
	 * the {@linkplain Jackknife jackknife} with the {@linkplain Jackknife#IDENTITY identity} statistic instead of this method.
	 *
	 * @param anf an iterable object returning arrays of doubles representing approximate neighbourhood functions.
	 * @return a combined approximate neighbourhood functions.
	 */
	public static double[] combine(final Iterable<double[]> anf) {
		final Object[] t = ObjectIterators.unwrap(anf.iterator());
		final double a[][] = Arrays.copyOf(t, t.length, double[][].class);

		final int n = a.length;

		int length = 0;
		for(final double[] b : a) length = Math.max(length, b.length);
		final double[] result = new double[length];

		BigDecimal last = BigDecimal.ZERO, curr;

		for(int i = 0; i < length; i++) {
			curr = BigDecimal.ZERO;
			for(int j = 0; j < n; j++) curr  = curr.add(BigDecimal.valueOf(a[j][i < a[j].length ? i : a[j].length - 1]));
			if (curr.compareTo(last) < 0) curr = last;
			result[i] = curr.doubleValue() / n;
			last = curr;
		}

		return result;
	}

	/** Evens out several approximate neighbourhood functions for the same
	 * graph by extending them to the same length (by copying the last value). This is usually a
	 * preparatory step for the {@linkplain Jackknife jackknife}.
	 *
	 * @param anf an iterable object returning arrays of doubles representing approximate neighbourhood functions.
	 * @return a list containing the same approximate neighbourhood functions, extended to the same length.
	 */
	public static ObjectList<double[]> evenOut(final Iterable<double[]> anf) {
		final Object[] u = ObjectIterators.unwrap(anf.iterator());
		final double t[][] = Arrays.copyOf(u, u.length, double[][].class);
		final int n = t.length;
		int max = 0;
		for(final double[] a: t) max = Math.max(max, a.length);

		final ObjectArrayList<double[]> result = new ObjectArrayList<>(n);
		for(int i = 0; i < n; i++) {
			final double[] a = new double[max];
			System.arraycopy(t[i], 0, a, 0, t[i].length);
			for(int j = t[i].length; j < max; j++) a[j] = a[j - 1];
			result.add(a);
		}

		return result;
	}

	/** A statistic that computes the {@linkplain NeighbourhoodFunction#spid(double[]) spid}. */
	public static Jackknife.Statistic SPID = (sample, mc) -> {
		BigDecimal sumDistances = BigDecimal.ZERO;
		BigDecimal sumSquareDistances = BigDecimal.ZERO;
		for(int i = sample.length; i-- != 1;) {
			final BigDecimal delta = sample[i].subtract(sample[i - 1]);
			sumDistances = sumDistances.add(delta.multiply(BigDecimal.valueOf(i)));
			sumSquareDistances = sumSquareDistances.add(delta.multiply(BigDecimal.valueOf((long)i * i)));
		}
		return new BigDecimal[] { sumSquareDistances.divide(sumDistances, mc).subtract(sumDistances.divide(sample[sample.length - 1], mc)) };
	};

	/** A statistic that computes the {@linkplain NeighbourhoodFunction#averageDistance(double[]) average distance}. */
	public static Jackknife.Statistic AVERAGE_DISTANCE = (sample, mc) -> {
		BigDecimal mean = BigDecimal.ZERO;
		for(int i = sample.length; i-- != 1;) mean = mean.add(sample[i].subtract(sample[i - 1]).multiply(BigDecimal.valueOf(i)));
		return new BigDecimal[] { mean.divide(sample[sample.length - 1], mc) };
	};

	/** A statistic that computes the {@linkplain NeighbourhoodFunction#harmonicDiameter(int, double[]) harmonic diameter}. */
	public static Jackknife.Statistic HARMONIC_DIAMETER = (sample, mc) -> {
		BigDecimal sumInverseDistances = BigDecimal.ZERO;
		for(int i = sample.length; i-- != 1;) sumInverseDistances = sumInverseDistances.add(sample[i].subtract(sample[i - 1]).divide(BigDecimal.valueOf(i), mc));
		return new BigDecimal[] { sample[0].multiply(sample[0]).divide(sumInverseDistances, mc) };
	};

	/** A statistic that computes the {@linkplain NeighbourhoodFunction#effectiveDiameter(double[]) effective diameter}. */
	public static Jackknife.Statistic EFFECTIVE_DIAMETER = new Jackknife.AbstractStatistic() {
		@Override
		public double[] compute(final double[] sample) {
			return new double[] { NeighbourhoodFunction.effectiveDiameter(sample) };
		}
	};

	/** A statistic that divides all values of a sample (an approximate neighbourhood function)
	 * by the last value. Useful for moving from neighbourhood functions to cumulative distribution functions. */
	public static Jackknife.Statistic CDF = (sample, mc) -> {
		final BigDecimal[] result = new BigDecimal[sample.length];
		final BigDecimal norm = BigDecimal.ONE.divide(sample[sample.length - 1], mc);
		for(int i = result.length; i-- != 0;) result[i] = sample[i].multiply(norm);
		return result;
	};

	/** A statistic that computes differences between consecutive elements of a sample (an approximate neighbourhood function)
	 * and divide them by the last value. Useful for moving from neighbourhood functions or cumulative distribution functions
	 * to probability mass functions. */
	public static Jackknife.Statistic PMF = (sample, mc) -> {
		final BigDecimal[] result = new BigDecimal[sample.length];
		final BigDecimal norm = BigDecimal.ONE.divide(sample[sample.length - 1], mc);
		result[0] = sample[0].multiply(norm);
		for(int i = result.length - 1; i-- != 0;) result[i + 1] = sample[i + 1].subtract(sample[i]).multiply(norm);
		return result;
	};
}
