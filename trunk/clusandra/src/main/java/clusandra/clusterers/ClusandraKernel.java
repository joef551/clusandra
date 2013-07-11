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
 * $Date: 2011-11-15 17:05:43 -0500 (Tue, 15 Nov 2011) $
 * $Revision: 174 $
 * $Author: jose $
 * $Id: ClusandraKernel.java 174 2011-11-15 22:05:43Z jose $
 */
package clusandra.clusterers;

import weka.core.Instance;
import clusandra.core.DataRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.Collections;
import java.io.Serializable;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Formatter;

/**
 * This class represents a CluSandra cluster. If the IDLIST is empty, then it is
 * a Microcluster, else it is a Superor Macro Cluster. All clusters are stored
 * in Cassandra. The term, 'Kernel' is used, instead of 'Cluster', to remain
 * consistent with its superclass.
 * 
 * 
 * Based on micro cluster, as defined by Aggarwal et al, On Clustering Massive
 * Data Streams: A Summarization Praradigm in the book Data streams : models and
 * algorithms, by Charu C Aggarwal
 * 
 * @article{ title = {Data Streams: Models and Algorithms}, author = {Aggarwal,
 *           Charu C.}, year = {2007}, publisher = {Springer Science+Business
 *           Media, LLC}, url = {http://ebooks.ulb.tu-darmstadt.de/11157/},
 *           institution = {eBooks [http://ebooks.ulb.tu-darmstadt.de/perl/oai2]
 *           (Germany)}, }
 * 
 */
public class ClusandraKernel implements Comparable<ClusandraKernel>,
		Serializable {

	private static final Log LOG = LogFactory.getLog(ClusandraKernel.class);

	private static final long serialVersionUID = 4245730038375465162L;
	private static final double EPSILON = 0.00005;
	private static final double MIN_VARIANCE = 1e-50;

	// these represent the elements in a CluSandra cluster feature.
	public static final String N_STR = "N";
	public static final byte[] N_BARRAY = N_STR.getBytes();
	public static final String LS_STR = "LS";
	public static final byte[] LS_BARRAY = LS_STR.getBytes();
	public static final String SS_STR = "SS";
	public static final byte[] SS_BARRAY = SS_STR.getBytes();
	public static final String LST_STR = "LST";
	public static final byte[] LST_BARRAY = LST_STR.getBytes();
	public static final String SST_STR = "SST";
	public static final byte[] SST_BARRAY = SST_STR.getBytes();
	public static final String CT_STR = "CT";
	public static final byte[] CT_BARRAY = CT_STR.getBytes();
	public static final String LAT_STR = "LAT";
	public static final byte[] LAT_BARRAY = LAT_STR.getBytes();
	public static final String LISTID_STR = "LISTID";
	public static final byte[] LISTID_BARRAY = LISTID_STR.getBytes();
	public static final String MAXRADIUS_STR = "MAXRADIUS";
	public static final byte[] MAXRADIUS_BARRAY = MAXRADIUS_STR.getBytes();

	// Number of points in the cluster.
	private double N;
	// Linear sum of all the points added to the cluster.
	private double[] LS;
	// Squared sum of all the points added to the cluster.
	private double[] SS;
	// creation time for this cluster
	private double CT;
	// the timestamp associated with the last DataRecord that this cluster
	// absorbed.
	private double LAT;
	// linear sum of timestamps
	private double LST;
	// Squared sum of the timestamps
	private double SST;
	// this cluster's id, which is also its Cassandra row key
	private String ID = UUID.randomUUID().toString();
	// the factor multiplied with the standard deviation to arrive at the radius
	private double radiusFactor = 1.5;
	// the maximum radius, which is relative to the data set.
	private double maxRadius = 200.0;
	// the list of cluster ids comprising the supercluster.
	private ArrayList<byte[]> IDLIST = new ArrayList<byte[]>();
	// this flag is used to determine if the cluster needs to be written out to
	// the data store
	private boolean saved = false;
	// used by cql for projecting
	private boolean project = true;

	/**
	 * The SortField is used by CQL to sort a list of clusters.
	 * 
	 */
	public static enum SortField {
		ID, RADIUS, N, CT, LAT;
	}

	/**
	 * Used for specifying the order of the sort.
	 * 
	 */
	public static enum SortOrder {
		ASCENDING, DESCENDING;
	}

	private SortField sortField = SortField.ID;
	private SortOrder sortOrder = SortOrder.ASCENDING;

	/**
	 * Create a cluster based on specified DataRecord.
	 * 
	 * @param record
	 *            initial DataRecord for this cluster
	 */
	public ClusandraKernel(DataRecord record) {
		this(record, record.numAttributes(), record.getTimestamp());
	}

	/**
	 * Create a cluster with specified DataRecord and max radius. This is the
	 * constructor used by the Clusandra clusterer.
	 * 
	 * @param record
	 *            initial DataRecord for this cluster
	 */
	public ClusandraKernel(DataRecord record, double maxRadius) {
		this(record, record.numAttributes(), record.getTimestamp());
		this.maxRadius = maxRadius;
	}

	public ClusandraKernel(DataRecord record, int dimensions, double timestamp) {
		this(record.toDoubleArray(), dimensions);
		CT = timestamp;
		LAT = timestamp;
		LST = timestamp;
		SST = timestamp * timestamp;
	}

	/**
	 * Create cluster based on given cluster. We're essentially cloning a
	 * cluster. Except for the ID, all fields are copied.
	 * 
	 * @param cluster
	 */
	public ClusandraKernel(ClusandraKernel cluster) {
		N = cluster.N;
		LS = Arrays.copyOf(cluster.LS, cluster.LS.length);
		SS = Arrays.copyOf(cluster.SS, cluster.SS.length);
		CT = cluster.CT;
		LAT = cluster.LAT;
		LST = cluster.LST;
		SST = cluster.SST;
		Collections.copy(IDLIST, cluster.IDLIST);
		radiusFactor = cluster.radiusFactor;
		maxRadius = cluster.getMaxRadius();
	}

	/**
	 * Create an empty cluster. This is the constructor used by Kmeans.
	 * 
	 * @param dimensions
	 */
	public ClusandraKernel(int dimensions) {
		LS = new double[dimensions];
		SS = new double[dimensions];
	}

	public ClusandraKernel(double[] center, int dimensions) {
		N = 1;
		LS = center.clone();
		SS = new double[dimensions];
		for (int i = 0; i < SS.length; i++) {
			SS[i] = LS[i] * LS[i];
		}
	}

	/**
	 * Specify how this cluster should be sorted. That is, the field that is
	 * used for sorting.
	 * 
	 * @param sortType
	 */
	public void setSortField(SortField sortType) {
		sortField = sortType;
	}

	/**
	 * Return how this cluster should be sorted. That is, the field that is used
	 * for sorting.
	 * 
	 * @return
	 */
	public SortField getSortField() {
		return sortField;
	}

	/**
	 * Specify the order of the sorting.
	 * 
	 * @param sortType
	 */
	public void setSortOrder(SortOrder sortOrder) {
		this.sortOrder = sortOrder;
	}

	/**
	 * Return the order of the sorting.
	 * 
	 * @return
	 */
	public SortOrder getSortOrder() {
		return sortOrder;
	}

	/**
	 * Return true if this cluster is a super cluster, else false.
	 * 
	 * @return
	 */
	public boolean isSuper() {
		return !getIDLIST().isEmpty();
	}

	/**
	 * The maximum radius for this cluster.
	 * 
	 * @return
	 */
	public double getMaxRadius() {
		return maxRadius;
	}

	/**
	 * Get the maximum radius for this cluster.
	 * 
	 * @param maxRadius
	 */
	public void setMaxRadius(double maxRadius) {
		this.maxRadius = maxRadius;
	}

	/**
	 * Returns whether or not this cluster needs to be saved to the data store.
	 * 
	 * @return
	 */
	public boolean isSaved() {
		return saved;
	}

	/**
	 * Called by CassandraDao after successfully writing cluster to Cassandra.
	 */
	public void saved() {
		saved = true;
	}

	/**
	 * Called to signify that the microcluster has been updated and needs
	 * saving.
	 */
	public void touch() {
		saved = false;
	}

	/**
	 * Get the creation time
	 * 
	 * @return
	 */
	public double getCT() {
		return CT;
	}

	/**
	 * Set the creation time
	 * 
	 * @param CT
	 */
	public void setCT(double CT) {
		this.CT = CT;
		touch();
	}

	/**
	 * Get the last absorption time
	 * 
	 * @return
	 */
	public double getLAT() {
		return LAT;
	}

	/**
	 * Set the last absorption time, but only if the given LAT is greater than
	 * the current LAT
	 * 
	 * @param LAT
	 */
	public void setLAT(double LAT) {
		if (LAT > this.LAT) {
			this.LAT = LAT;
			touch();
		}
	}

	/**
	 * Get the cluster's ID
	 * 
	 * @return
	 */
	public String getID() {
		return ID;
	}

	/**
	 * Used to override the auto-generated id. Invoked by template when
	 * retrieving existing clusters from Cassandra.
	 */
	public void setID(String ID) {
		this.ID = ID;
		touch();
	}

	/**
	 * Get the list of IDs comprising this cluster.
	 * 
	 * @return
	 */
	public ArrayList<byte[]> getIDLIST() {
		return IDLIST;
	}

	/**
	 * Make this cluster a super cluster
	 * 
	 * @param IDLIST
	 */
	public void setIDLIST(ArrayList<byte[]> IDLIST) {
		this.IDLIST = IDLIST;
		touch();
	}

	/**
	 * Set the total count of absorbed DataRecords
	 * 
	 * @param N
	 */
	public void setN(double N) {
		this.N = N;
		touch();
	}

	/**
	 * Get the total count of absorbed DataRecords
	 * 
	 * @return
	 */
	public double getN() {
		return N;
	}

	/**
	 * Set the linear sum vector
	 * 
	 * @param LS
	 */
	public void setLS(double[] LS) {
		this.LS = LS;
		touch();
	}

	/**
	 * Get the linear sum vector
	 * 
	 * @return
	 */
	public double[] getLS() {
		return LS;
	}

	/**
	 * Set the sum of the squares vector
	 * 
	 * @param SS
	 */
	public void setSS(double[] SS) {
		this.SS = SS;
		touch();
	}

	/**
	 * Get sum of the squares vector
	 * 
	 * @return
	 */
	public double[] getSS() {
		return SS;
	}

	/**
	 * Get the square sum of times
	 * 
	 * @param SST
	 */
	public void setSST(double SST) {
		this.SST = SST;
		touch();
	}

	/**
	 * Get the sum square of times
	 * 
	 * @return
	 */
	public double getSST() {
		return SST;
	}

	/**
	 * Set the linear sum of times
	 * 
	 * @param LST
	 */
	public void setLST(double LST) {
		this.LST = LST;
		touch();
	}

	/**
	 * Get the linear sum of time
	 * 
	 * @return
	 */
	public double getLST() {
		return LST;
	}

	/**
	 * Absorb the provided DataRecord
	 * 
	 * @param record
	 */
	public synchronized void absorb(DataRecord record) {
		N++;
		LST += record.getTimestamp();
		SST += record.getTimestamp() * record.getTimestamp();
		for (int i = 0; i < record.numValues(); i++) {
			LS[i] += record.value(i);
			SS[i] += record.value(i) * record.value(i);
		}
		LAT = record.getTimestamp();
		touch();
	}

	/**
	 * Add provided microcluster to create supercluster.
	 * 
	 * @param cluster
	 */
	public synchronized void add(ClusandraKernel cluster) throws Exception {
		if (cluster.isSuper()) {
			throw new Exception("cannot add supercluster");
		}
		IDLIST.add(cluster.getID().getBytes());
		merge(cluster);
	}

	/**
	 * Merge the specified cluster with this cluster
	 * 
	 * @param cluster
	 */
	public synchronized void merge(ClusandraKernel cluster) {
		if (cluster.CT < CT) {
			CT = cluster.CT;
		}
		if (cluster.LAT > LAT) {
			LAT = cluster.LAT;
		}
		LST += cluster.LST;
		SST += cluster.SST;
		this.N += cluster.N;
		addVectors(LS, cluster.LS);
		addVectors(SS, cluster.SS);
		touch();
	}

	/**
	 * Get this cluster's centroid (mean)
	 * 
	 * @return this cluster's centroid
	 */
	public double[] getCenter() {
		int len = LS.length;
		double res[] = new double[len];
		for (int i = 0; i < len; i++) {
			res[i] = LS[i] / N;
		}
		return res;
	}

	/**
	 * The radius is the cluster's standard deviation (average distance of all
	 * absorbed DataRecords from mean).
	 * 
	 * Design note: the radius should have a maximum boundary. The max boundary
	 * defines what is to be considered 'similar' with respect to the given data
	 * set.
	 * 
	 * @return radius or root mean squared deviation (RMSD)
	 */
	public synchronized double getRadius() {
		double radius = getTrueRadius();
		return (radius == 0) ? getMaxRadius() : radius;
	}

	/**
	 * Get the true radius for this cluster.
	 * 
	 * radius factor
	 * 
	 * @return radius or root mean squared deviation (RMSD)
	 */
	public synchronized double getTrueRadius() {
		return (N == 1) ? 0.0 : getDeviation();
	}

	/**
	 * Returns true if the passed in cluster is the same, ID-wise, as this
	 * cluster. False otherwise.
	 * 
	 * @param cluster
	 * @return
	 */
	public boolean same(ClusandraKernel cluster) {
		return getID().equals(cluster.getID());

	}

	/**
	 * Set the factor by which to multiply the standard deviation to arrive at
	 * the radius.
	 * 
	 * @param factor
	 */
	public void setRadiusFactor(double factor) {
		radiusFactor = factor;
	}

	/**
	 * Get the factor by which to multiply the standard deviation to arrive at
	 * the radius.
	 * 
	 * @return
	 */
	public double getRadiusFactor() {
		return radiusFactor;
	}

	public synchronized void subtract(ClusandraKernel cluster) {
		// TBD
	}

	/**
	 * Calculate the Euclidean distance from this cluster's center to the point
	 * provided.
	 * 
	 * @param point
	 *            point to take the distance from
	 * @return distance from center to point
	 */
	public synchronized double getDistance(double[] point) {
		double[] center = getCenter();
		if (point == null || point.length != center.length) {
			return Double.MAX_VALUE;
		}
		double res = 0.0;
		for (int i = 0; i < center.length; i++) {
			res += Math.pow((center[i] - point[i]), 2);
		}
		return Math.sqrt(res);

	}	

	/**
	 * Get the mean time of all the absorbed DataRecords
	 * 
	 * @return
	 */
	public double getMuTime() {
		return LST / N;
	}

	public double getSigmaTime() {
		return Math.sqrt((SST / N) - Math.pow((LST / N), 2));
	}

	// Get the root mean squared deviation (standard deviation)
	private double getDeviation() {
		double[] variance = getVarianceVector();
		double sumOfDeviation = 0.0;
		for (int i = 0; i < variance.length; i++) {
			sumOfDeviation += Math.sqrt(variance[i]);
		}
		return sumOfDeviation;
	}

	// Get the variance used to calculate the standard deviation
	private double[] getVarianceVector() {
		double[] res = new double[LS.length];
		for (int i = 0; i < LS.length; i++) {
			double lsDivNSquared = Math.pow(LS[i], 2) / N;
			res[i] = ((SS[i]) - lsDivNSquared) / (N - 1);
			if (res[i] <= 0.0 && res[i] > -EPSILON) {
				res[i] = MIN_VARIANCE;
			}
		}
		return res;
	}

	/**
	 * See interface <code>Cluster</code>
	 * 
	 * @param point
	 * @return
	 */
	public double getInclusionProbability(Instance instance) {
		// not used within clusandra
		return 0.0d;
	}

	/**
	 * Check if this cluster and the other cluster lived within their same
	 * active time horizons. That is, determine if the two clusters reside
	 * within the same active time horizon
	 * 
	 * @param other
	 * @param expireTime
	 * @return
	 */
	public boolean withinTimeHorizon(ClusandraKernel other, double expireTime) {

		// first determine clusters that has lived closest to and farthest from
		// present time.
		ClusandraKernel closest;
		ClusandraKernel farthest;

		if (other.getLAT() >= getLAT()) {
			closest = other;
			farthest = this;
		} else {
			closest = this;
			farthest = other;
		}

		// if the one that lived the farthest back in time also lived within the
		// closest's time horizon?
		if (farthest.getLAT() >= closest.getCT()) {
			return true;
		}
		// ok, then check if the one that lived the farthest back is within the
		// expire time of the one that lived the closest
		if (farthest.getLAT() >= (closest.getCT() - expireTime)) {
			return true;
		}
		return false;
	}

	/**
	 * Compare this cluster to the other cluster provided.
	 * 
	 * @param other
	 * @return
	 */
	public boolean compare(ClusandraKernel other) {

		if (getCT() != other.getCT()) {
			return false;
		} else if (getLAT() != other.getLAT()) {
			return false;
		} else if (getLST() != other.getLST()) {
			return false;
		} else if (getSST() != other.getSST()) {
			return false;
		} else if (getRadius() != other.getRadius()) {
			return false;
		} else if (getMaxRadius() != other.getMaxRadius()) {
			return false;
		} else if (IDLIST.size() != other.IDLIST.size()) {
			return false;
		} else if (IDLIST.size() > 0 && !IDLIST.containsAll(other.getIDLIST())) {
			return false;
		} else if (!Arrays.equals(LS, other.getLS())) {
			return false;
		} else if (!Arrays.equals(SS, other.getSS())) {
			return false;
		} else if (!Arrays.equals(getCenter(), other.getCenter())) {
			return false;
		}
		return true;
	}

	/**
	 * Set whether this cluster can be projected or not.
	 * 
	 * @param b
	 */
	public void setProject(boolean b) {
		project = b;
	}

	/**
	 * Returns whether this cluster can be projected or not
	 * 
	 * @return
	 */
	public boolean getProject() {
		return project;
	}

	/**
	 * Return the String representation of this cluster.
	 */
	public String toString() {
		return new Formatter().format(
				"cluster id=[%s] N=[%8.0f] radius=[%8.2f]", getID(), getN(),
				getRadius()).toString()
				+ printLS() + printSS();
	}

	/**
	 * This method is used by the CQL to sort a list of ClusandraKernel objects.
	 */
	public int compareTo(ClusandraKernel other) {
		// the order of the sorting can be ascending or descending. the default
		// is ascending
		switch (sortField) {
		case ID:
			int ret = getID().compareTo(other.getID());
			if (ret == 0 || getSortOrder() == SortOrder.ASCENDING) {
				return ret;
			}
			return (ret > 0 ? -1 : 1);
		case RADIUS:
			if (getRadius() == other.getRadius()) {
				return 0;
			} else if (getRadius() > other.getRadius()) {
				// return 1;
				return ((getSortOrder() == SortOrder.ASCENDING) ? 1 : -1);
			}
			// return -1;
			return ((getSortOrder() == SortOrder.ASCENDING) ? -1 : 1);
		case N:
			if (getSortOrder() == SortOrder.ASCENDING) {
				return (int) ((int) getN() - (int) other.getN());
			}
			return (int) ((int) other.getN() - (int) getN());

		case CT:
			if (getSortOrder() == SortOrder.ASCENDING) {
				return (int) (getCT() - other.getCT());
			}
			return (int) (other.getCT() - getCT());
		case LAT:
			if (getSortOrder() == SortOrder.ASCENDING) {
				return (int) (getLAT() - other.getLAT());
			}
			return (int) (other.getLAT() - getLAT());
		}
		return 0;
	}

	/**
	 * Adds the second array to the first array element by element. The arrays
	 * must have the same length.
	 * 
	 * @param a1
	 *            Vector to which the second vector is added.
	 * @param a2
	 *            Vector to be added. This vector does not change.
	 */
	public static void addVectors(double[] a1, double[] a2) {
		assert (a1 != null);
		assert (a2 != null);
		assert (a1.length == a2.length) : "Adding two arrays of different "
				+ "length";

		for (int i = 0; i < a1.length; i++) {
			a1[i] += a2[i];
		}
	}

	/**
	 * Returns the average distance between all the clusters in the given list.
	 * This is essentially telling us the density of the clusters within their
	 * time horizon.
	 * 
	 * @param clusters
	 * @return
	 */
	public static double getDensity(List<ClusandraKernel> clusters) {
		double distance = 0.0;
		double distanceCounter = 0;

		if (clusters == null || clusters.size() < 2) {
			return distance;
		}
		for (int i = 0; i < clusters.size(); i++) {
			ClusandraKernel source = clusters.get(i);
			for (int j = i + 1; j < clusters.size(); j++) {
				ClusandraKernel target = clusters.get(j);
				distance += ClusandraClusterer.getDistance(source.getCenter(),
						target.getCenter());
				distanceCounter++;
			}
		}
		return (distance / distanceCounter);
	}

	public double getCenterDistance(Instance instance) {
		double distance = 0.0;
		// get the center through getCenter so subclass have a chance
		double[] center = getCenter();
		for (int i = 0; i < center.length; i++) {
			double d = center[i] - instance.value(i);
			distance += d * d;
		}
		return Math.sqrt(distance);
	}

	/**
	 * Does this cluster temporally overlap the given cluster?
	 * 
	 * @param target
	 * @return true if the two clusters temporally overlap
	 */
	public boolean temporalOverlap(ClusandraKernel target) {
		if ((getCT() >= target.getCT() && getCT() <= target.getCT())
				|| (getLAT() >= target.getCT() && getLAT() <= target.getLAT())) {
			return true;
		}
		return false;
	}

	/**
	 * Does this cluster spatially overlap the given cluster?
	 * 
	 * @param target
	 * @param overlapFactor
	 * @return true if this cluster overlaps the given cluster
	 */
	public boolean spatialOverlap(ClusandraKernel target, double overlapFactor) {
		double sR = overlapFactor * getRadius();
		double tR = overlapFactor * target.getRadius();
		double dist = getDistance(target.getCenter());
		return (((sR + tR) - dist) > 0);
	}
	
	public static double getDistance(double[] a, double[] b) {
		if (a.length != b.length) {
			throw new RuntimeException(
					"Attempting to compare two clusterables of different dimensions");
		}
		double sum = 0;
		for (int i = 0; i < a.length; i++) {
			double diff = a[i] - b[i];
			sum += diff * diff;
		}
		return Math.sqrt(sum);
	}

	/**
	 * * This method is passed a list of supercolumns that represent a cluster
	 * (i.e.,ClusandraKernel). From the supercolumns, it creates and returns a
	 * ClusandraKernel.
	 * 
	 * @param cluster
	 * @param clusterID
	 *            This id of the cluster to be generated. This is also its row
	 *            key
	 * @return
	 */
	public static ClusandraKernel getClusandraKernel(List<SuperColumn> cluster,
			String clusterID) {

		// The elements of a cluster
		double N = 0;
		double[] LS = null;
		double[] SS = null;
		double LST = 0;
		double SST = 0;
		double CT = 0;
		double LAT = 0;
		double maxRadius = 0;
		ArrayList<byte[]> IDLIST = new ArrayList<byte[]>();

		List<Column> scSubCols = null;
		byte[] scName = null;
		int index = 0;

		// Iterate through the list of provided supercolumns that comprise a
		// cluster
		for (SuperColumn sc : cluster) {
			// get the name of the supercolumn
			scName = sc.getName();
			if (Arrays.equals(scName, N_BARRAY)) {
				N = Double
						.valueOf(new String(sc.getColumns().get(0).getValue()));
			} else if (Arrays.equals(scName, LS_BARRAY)) {
				scSubCols = sc.getColumns();
				LS = new double[scSubCols.size()];
				for (int i = 0; i < LS.length; i++) {
					index = Integer.valueOf(new String(scSubCols.get(i)
							.getName()));
					LS[index] = Double.valueOf(new String(scSubCols.get(i)
							.getValue()));
				}
			} else if (Arrays.equals(scName, SS_BARRAY)) {
				scSubCols = sc.getColumns();
				SS = new double[scSubCols.size()];
				for (int i = 0; i < SS.length; i++) {
					index = Integer.valueOf(new String(scSubCols.get(i)
							.getName()));
					SS[index] = Double.valueOf(new String(scSubCols.get(i)
							.getValue()));
				}
			} else if (Arrays.equals(scName, SST_BARRAY)) {
				SST = Double.valueOf(new String(sc.getColumns().get(0)
						.getValue()));
			} else if (Arrays.equals(scName, LST_BARRAY)) {
				LST = Double.valueOf(new String(sc.getColumns().get(0)
						.getValue()));
			} else if (Arrays.equals(scName, CT_BARRAY)) {
				CT = Double.valueOf(new String(sc.getColumns().get(0)
						.getValue()));
			} else if (Arrays.equals(scName, LAT_BARRAY)) {
				LAT = Double.valueOf(new String(sc.getColumns().get(0)
						.getValue()));
			} else if (Arrays.equals(scName, MAXRADIUS_BARRAY)) {
				maxRadius = Double.valueOf(new String(sc.getColumns().get(0)
						.getValue()));
			} else if (Arrays.equals(scName, LISTID_BARRAY)) {
				// Get the IDLIST, which may be empty
				scSubCols = sc.getColumns();
				if (!scSubCols.isEmpty()) {
					IDLIST = new ArrayList<byte[]>(scSubCols.size());
					for (int i = 0; i < scSubCols.size(); i++) {
						IDLIST.add(scSubCols.get(i).getValue());
					}
				}
			}
		}

		// now we can create a cluster
		ClusandraKernel clusKernel = new ClusandraKernel(new DataRecord(LS));
		clusKernel.setID(clusterID);
		clusKernel.setN(N);
		clusKernel.setSS(SS);
		clusKernel.setLST(LST);
		clusKernel.setSST(SST);
		clusKernel.setCT(CT);
		clusKernel.setLAT(LAT);
		clusKernel.setIDLIST(IDLIST);
		clusKernel.setMaxRadius(maxRadius);
		return clusKernel;
	}

	private String printCenter() {
		String s1 = new String(" \ncenter=[");
		double[] center = getCenter();
		for (int i = 0; i < center.length; i++) {
			if (i == center.length - 1) {
				s1 += (new Formatter().format("%10.3f", center[i]).toString())
						+ "";
			} else {
				s1 += (new Formatter().format("%10.3f", center[i]).toString())
						+ ",";
			}
		}
		return s1 += "]";
	}

	private String printLS() {
		String s1 = new String(" \nLS=[");
		double[] LS = getLS();
		for (int i = 0; i < LS.length; i++) {
			if (i == LS.length - 1) {
				s1 += (new Formatter().format("%10.3f", LS[i]).toString()) + "";
			} else {
				s1 += (new Formatter().format("%10.3f", LS[i]).toString())
						+ ",";
			}
		}
		return s1 += "]";
	}

	private String printSS() {
		String s1 = new String(" \nSS=[");
		double[] SS = getSS();
		for (int i = 0; i < SS.length; i++) {
			if (i == SS.length - 1) {
				s1 += (new Formatter().format("%10.3f", SS[i]).toString()) + "";
			} else {
				s1 += (new Formatter().format("%10.3f", SS[i]).toString())
						+ ",";
			}
		}
		return s1 += "]";
	}

	// For test purposes and to illustrate how relatively simple it is to write
	// and read a cluster to and from Cassandra.
	public static void main(String[] args) throws Exception {

		// Create some byte arrays that will mimic DataRecords
		double[] d1 = new double[5];
		d1[0] = 0;
		d1[1] = 0;
		d1[2] = 0;
		// d1[3] = 1;
		// d1[4] = 1;
		double[] d2 = new double[5];
		d2[0] = 0;
		d2[1] = 0;
		d2[2] = 5;
		// d2[3] = 1;
		// d2[4] = 120;
		double[] d3 = new double[5];
		d3[0] = 0;
		d3[1] = 0;
		d3[2] = 10;
		// d3[3] = 1;
		// d3[4] = 1;
		double[] d4 = new double[5];
		d4[0] = 2;
		d4[1] = 1;
		// d4[2] = 0;
		// d4[3] = 1;
		// d3[4] = 8;

		// Create a cluster from d1, then have it absorb the others
		ClusandraKernel c1 = new ClusandraKernel(new DataRecord(d1));
		ClusandraKernel c2 = new ClusandraKernel(new DataRecord(d2));
		ClusandraKernel c3 = new ClusandraKernel(c1);
		c3.add(c2);
		System.out.println("isSuper() returns " + c3.isSuper());

	}
}
