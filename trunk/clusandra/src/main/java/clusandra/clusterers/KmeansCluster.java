package clusandra.clusterers;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Cluster type used by the Kmeans clusterer.
 * 
 * @author jfernandez
 * 
 */
public class KmeansCluster {

	private static final long serialVersionUID = -6927185445156513561L;

	private transient double[] currentLocation;
	private transient double[] meanLocation;
	private transient double[] runningTotalForMean;

	// this cluster's nearest neighbor
	private transient KmeansCluster NN = null;
	// the distance to this cluster's nearest neighbor
	private transient double distNN = 0;

	// if a point is added or removed from this
	// cluster, then its mean will have to be
	// recalculated
	private transient boolean updateMean;

	// the set of points (DataRecords) assigned to this cluster
	private transient List<DataRecord> points = new LinkedList<DataRecord>();

	// how far has the cluster drifted from its prior spot
	private transient double driftDistance = Double.MAX_VALUE;

	private transient double radius;

	/**
	 * Create a cluster based on given DataRecord. Note that the
	 * ClusandraCluster super class is empty until this Kmeans cluster is
	 * flushed.
	 * 
	 * @param record
	 *            initial DataRecord for this cluster
	 */
	public KmeansCluster(DataRecord record) {
		currentLocation = record.toDoubleArray();
		meanLocation = new double[currentLocation.length];
		runningTotalForMean = new double[currentLocation.length];
	}

	/**
	 * Merges or absorbs the specified cluster with this cluster.
	 * 
	 * @param cluster
	 * @return
	 */
	public boolean merge(KmeansCluster cluster) {
		if (cluster == null || cluster.getPoints() == null
				|| cluster.getPoints().size() == 0) {
			return false;
		}
		// adopt the cluster's points
		for (DataRecord point : cluster.getPoints()) {
			addPoint(point);
		}
		// update the mean and location for this cluster.
		currentLocation = getMean();
		return true;
	}

	/**
	 * Return a copy of the cluster's current location.
	 */
	public double[] getLocation() {
		return currentLocation.clone();
	}

	/**
	 * Give this cluster a new location
	 * 
	 * @param location
	 */
	public void setLocation(double[] location) {
		System.arraycopy(location, 0, currentLocation, 0,
				currentLocation.length);
	}

	/**
	 * Get the cluster's center of gravity (mean)
	 * 
	 * @return
	 */
	public double[] getMean() {
		// if a point has neither been added nor removed
		// from this cluster, then don't spend time
		// recalculating the mean
		if (updateMean) {
			double numItems = (double) points.size();
			Arrays.fill(meanLocation, 0.0F);
			for (int i = 0; i < runningTotalForMean.length; i++) {
				meanLocation[i] = runningTotalForMean[i] / numItems;
			}
			updateMean = false;
		}
		return meanLocation;
	}

	/**
	 * Return the distance from this cluster to the given point.
	 * 
	 * @param point
	 * @return
	 */
	public double getDistance(DataRecord point) {
		return MicroCluster.getDistance(getLocation(), point.toDoubleArray());
	}

	public double getDriftDistance() {
		return driftDistance;
	}

	public void setDriftDistance(double d) {
		driftDistance = d;
	}

	public void removePoint(DataRecord point) {
		if (points.remove(point)) {
			KmeansClusterer.subArrays(runningTotalForMean,
					point.toDoubleArray());
			updateMean = true;
		}
	}

	public void addPoint(DataRecord point) {
		points.add(point);
		point.setKmeansKernel(this);
		// update the running total used to
		// return the mean
		KmeansClusterer.sumArrays(runningTotalForMean, point.toDoubleArray());
		updateMean = true;
	}

	public List<DataRecord> getPoints() {
		return points;
	}

	public String toStringLocation() {
		String str = "";
		for (int i = 0; i < currentLocation.length; i++) {
			str += currentLocation[i];
			if (i < currentLocation.length - 1) {
				str += "  ";
			}
		}
		return str;
	}

	/**
	 * Set this cluster's nearest neighbor
	 * 
	 * @param NN
	 */
	public void setNN(KmeansCluster NN) {
		this.NN = NN;
	}

	public KmeansCluster getNN() {
		return NN;
	}

	/**
	 * Set the distance to this cluster's nearest neighbor
	 * 
	 * @param dist
	 */
	public void setDistNN(double dist) {
		distNN = dist;
	}

	public double getDistNN() {
		return distNN;
	}

	public void reset() {
		NN = null;
		distNN = 0;
		currentLocation = null;
		meanLocation = null;
		runningTotalForMean = null;
		points = new LinkedList<DataRecord>();
		driftDistance = Double.MAX_VALUE;
	}

	public double getRadius() {
		return radius;
	}

	public void setRadius(double tmpRadius) {
		this.radius = tmpRadius;
	}

}
