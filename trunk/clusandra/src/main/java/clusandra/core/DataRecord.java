/*
 * COPYRIGHT(c) 2011 by Jose R. Fernandez
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
 * $Date: 2011-08-23 15:06:55 -0400 (Tue, 23 Aug 2011) $
 * $Revision: 125 $
 * $Author: jose $
 * $Id: DataRecord.java 125 2011-08-23 19:06:55Z jose $
 */
package clusandra.core;

import java.io.Serializable;
import clusandra.clusterers.KmeansKernel;

/**
 * This is an object that is passed through the Clusandra framework, via the JMS
 * queues. It encapsulates a multi-dimensional vector whose components are
 * continuous numerical values represented as doubles. An instance of a
 * DataRecord represents an occurrence in the point space.
 * 
 * This object is typically injected into the Clusandra framework by a
 * StreamGenerator.
 * 
 */
public class DataRecord implements Serializable, Comparable<DataRecord> {

	private static final long serialVersionUID = 1955537382154981392L;

	// Record the creation time of this DataRecord. It may later be
	// overridden by the StreamGenerator with the raw data stream record's time
	// stamp (if any).
	private double timestamp = System.currentTimeMillis();

	// The data record's attribute values. */
	protected double[] m_AttValues;

	// these two variables are used by the kmeans clusterer
	private transient double distanceToCluster = 0.0;
	private transient KmeansKernel kmeansKernel = null;
	private transient boolean centroid;

	// a new instance of a DataRecord must use a copy or clone
	// of the given vector
	public DataRecord(double[] attValues) {
		m_AttValues = attValues.clone();
	}

	public DataRecord(int numAttributes) {
		m_AttValues = new double[numAttributes];
	}

	/**
	 * Set this DataRecord's timestamp.
	 * 
	 * @param timestamp
	 */
	public void setTimestamp(double timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Get this DataRecord's timestamp
	 * 
	 * @return
	 */
	public double getTimestamp() {
		return timestamp;
	}

	/**
	 * Returns the number of attributes or dimensions
	 * 
	 * @return the number of attributes as an integer
	 */
	public int numAttributes() {
		return m_AttValues.length;
	}

	/**
	 * Returns the number of values present. Always the same as numAttributes().
	 * 
	 * @return the number of values
	 */

	public int numValues() {
		return m_AttValues.length;
	}
	
	/**
	 * Returns the encapsulated vector. 
	 * 
	 * @return
	 */
	public double[] getValues(){
		return m_AttValues;
	}

	/**
	 * Returns a copy of the encapsulated vector.
	 * 
	 * @return an array containing all the instance attribute values
	 */
	public double[] toDoubleArray() {
		double[] newValues = new double[m_AttValues.length];
		System.arraycopy(m_AttValues, 0, newValues, 0, m_AttValues.length);
		return newValues;
	}

	/**
	 * Returns attribute value.
	 * 
	 * @param attIndex
	 *            the attribute's index
	 * @return the specified value as a double
	 */
	public double value(int attIndex) {
		return m_AttValues[attIndex];
	}

	public String printPayLoad() {
		double[] dRecordPayload = toDoubleArray();
		String s1 = "";
		for (int i = 0; i < dRecordPayload.length; i++) {
			s1 += (Double.toString(dRecordPayload[i]) + ",");
		}
		return s1;
	}

	/**
	 * Set the distance to this data record's closest or owning cluster. Used
	 * primarily by kmeans.
	 * 
	 * @param d
	 */
	public void setDistanceToCluster(Double d) {
		distanceToCluster = d;
	}

	/**
	 * Get the distance to this data record's owning cluster. Used primarily by
	 * kmeans.
	 * 
	 * @return
	 */
	public double getDistanceToCluster() {
		return distanceToCluster;
	}

	/**
	 * Assign this DataRecord (point) to a kmeans kernel (cluster).
	 * 
	 * @param kernel
	 */
	public void setKmeansKernel(KmeansKernel kernel) {
		this.kmeansKernel = kernel;
	}

	/**
	 * Get the Kmeans kernel (cluster) that this point is currently assigned to.
	 * 
	 * @return
	 */
	public KmeansKernel getKmeansKernel() {
		return this.kmeansKernel;
	}

	/**
	 * Has this point been marked as an initial centroid? Used by Kmeans.
	 * 
	 * @return
	 */
	public boolean isCentroid() {
		return centroid;
	}

	public void setCentroid(boolean b) {
		centroid = b;
	}

	public int compareTo(DataRecord record) {
		return Double.compare(timestamp, record.timestamp);
	}
}
