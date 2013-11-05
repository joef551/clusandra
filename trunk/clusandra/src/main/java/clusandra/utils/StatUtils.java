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

}
