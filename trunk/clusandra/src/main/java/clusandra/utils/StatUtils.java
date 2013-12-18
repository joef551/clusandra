/*
 * COPYRIGHT(c) 2013 by Jose R. Fernandez
 * 
 *           joef551@gmail.com
 *
 * This file is part of CluSandra.
 *
 * CluSandra is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CluSandra is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CluSandra.  If not, see <http://www.gnu.org/licenses/>.
 *
 * $Date: $
 * $Revision: $
 * $Author: $
 * $Id: $
 */
package clusandra.utils;

import java.util.List;

import clusandra.clusterers.DataRecord;


/**
 * Some general stats related utils
 * 
 * @author jfernandez
 * 
 */
public class StatUtils {

	/**
	 * Get the euclidean distance between the two vectors
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static double getDistance(double[] a, double[] b) {
		double sum = 0;
		for (int i = 0; i < a.length; i++) {
			double diff = a[i] - b[i];
			sum += diff * diff;
		}
		return Math.sqrt(sum);
	}

	/**
	 * Optimized version of getDistance; it doesn't take the square root.
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static double getFastDistance(double[] a, double[] b) {
		double sum = 0;
		for (int i = 0; i < a.length; i++) {
			double diff = a[i] - b[i];
			sum += diff * diff;
		}
		return sum;
	}

	/**
	 * Depending on the fast boolean's setting, calls either getFastDistance or
	 * getDistance.
	 * 
	 * @param fast
	 * @param a
	 * @param b
	 * @return
	 */
	public static double getDistance(boolean fast, double[] a, double[] b) {
		return (fast == true) ? getFastDistance(a, b) : getDistance(a, b);
	}

	/**
	 * This method assumes that the given currentMin distance was gotten from
	 * the 'fast' method, so the currentMin distance was never subjected to
	 * being square rooted.
	 * 
	 * This method will return the given currentMin distance if, while
	 * calculating the new distance, the new distance is found to be greater
	 * than the currentMin distance. Another optimization.
	 * 
	 * @param currentMin
	 * @param a
	 * @param b
	 * @return
	 */
	public static double getMinDistance(double currentMin, double[] a,
			double[] b) {
		double sum = 0;
		for (int i = 0; i < a.length; i++) {
			double diff = a[i] - b[i];
			sum += diff * diff;
			if (sum >= currentMin) {
				return currentMin;
			}
		}
		return sum;
	}

	/**
	 * Depending on the fast boolean's setting, calls either getMinDistance or
	 * getDistance.
	 * 
	 * @param currentMin
	 * @param fast
	 * @param a
	 * @param b
	 * @return
	 */
	public static double getMinDistance(double currentMin, boolean fast,
			double[] a, double[] b) {
		return (fast == true) ? getMinDistance(currentMin, a, b) : getDistance(
				a, b);
	}
	
	/**
	 * Using the z-score, normalize the given points.
	 * 
	 * Since Euclidean distance is used, the clusters will be influenced
	 * strongly by the magnitudes of the variables, especially by outliers.
	 * Normalizing removes this bias. However, whether or not one desires this
	 * removal of bias depends on what one wants to find: sometimes if one would
	 * want a variable to influence the clusters more, one could manipulate the
	 * clusters precisely in this way, by increasing the relative magnitude of
	 * these fields.
	 * 
	 * @param points
	 */
	public static void normalizePoints(List<DataRecord> points) {

		if (points == null || points.isEmpty()) {
			return;
		}

		double N = points.size();

		// first find the mean
		double[] mean = new double[points.get(0).numValues()];
		for (DataRecord point : points) {
			sumArrays(mean, point.getValues());
		}
		for (int i = 0; i < mean.length; i++) {
			mean[i] /= N;
		}

		// now find the std dev of each of the attributes
		double[] stdev = new double[mean.length];
		for (DataRecord point : points) {
			double[] location = point.getValues();
			for (int i = 0; i < location.length; i++) {
				double diff = location[i] - mean[i];
				stdev[i] += (diff * diff);
			}
		}
		// the above figured out the variance, now take the square
		// root to get the std dev
		for (int i = 0; i < stdev.length; i++) {
			stdev[i] = Math.sqrt(stdev[i] / N);
		}

		// now that we have the mean and std dev, normalize all the points
		for (DataRecord point : points) {
			double[] location = point.getValues();
			for (int i = 0; i < location.length; i++) {
				location[i] = (location[i] - mean[i]) / stdev[i];
			}
		}
	}
	
	/**
	 * Adds array B to A
	 * 
	 * @param valsA
	 * @param valsB
	 * @return
	 */
	public static void sumArrays(double[] A, double[] B) {
		for (int i = 0; i < A.length; i++) {
			A[i] += B[i];
		}
	}

	/**
	 * Subtracts array B from A
	 * @param A
	 * @param B
	 */
	public static void subArrays(double[] A, double[] B) {
		for (int i = 0; i < A.length; i++) {
			A[i] -= B[i];
		}
	}


}
