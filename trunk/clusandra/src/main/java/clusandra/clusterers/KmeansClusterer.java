/*
 * COPYRIGHT(c) 2013 by Jose R. Fernandez
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
 * $Date: 2011-11-14 21:31:23 -0500 (Mon, 14 Nov 2011) $
 * $Revision: 170 $
 * $Author: jose $
 * $Id: ClusandraClusterer.java 170 2011-11-15 02:31:23Z jose $
 */
package clusandra.clusterers;

import java.util.ListIterator;
import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import clusandra.cassandra.ClusandraDao;
import clusandra.core.DataRecord;
import clusandra.core.QueueAgent;
import clusandra.core.Processor;
import clusandra.core.CluMessage;

/**
 * This class implements the Kmeans clustering algorithm.
 * 
 * <pre>
 * 
 * 1) Calculate the initial set of clusters using the kmeans++ seeding
 * algorithm.
 * 
 * 2) Assign all points to their closest cluster 
 * 
 * 3) Re-calculate cluster's mean and move each cluster centroid to mean 
 *    location
 * 
 * 4) If clusters have converged (i.e., none moved), we're done, else go back 
 *    to #2 and do it again.
 * 
 * </pre>
 * 
 * This Clusterer runs the Kmeans against a list of data stream records
 * (occurrences in point space) to produce micro clusters. Note its a list and
 * not a set because it is conceivable that duplicate points may exist. It sends
 * the resulting micro clusters to the specified message queue. The micro
 * clusters sent to the queue are then clustered into a BTree by one central
 * BTreeClusterer. The idea being that you can have many instances of the
 * KmeansClusterer working in parallel to harness the data stream's records,
 * while the single instance of the BTreeClusterer acts as a reducer of sorts.
 * The BTreeClusterer persists micro-clusters to the Cassandra DB.
 * 
 * The density portion of this work is based, in part, on the following paper.
 * 
 * Citation: Yixin Chen, Li Tu: Density-Based Clustering for Real-Time Stream
 * Data. KDD '07
 * 
 * The above paper describes an approach for managing the temporal density of
 * micro-clusters or data points without having to visit a micro-cluster each
 * and every time period; as described in:
 * 
 * Citation: Feng Cao, Martin Ester, Weining Qian, Aoying Zhou: Density-Based
 * Clustering over an Evolving Data Stream with Noise. SDM 2006
 * 
 * @author jfernandez
 * 
 */
public class KmeansClusterer implements Processor {

	private static final Log LOG = LogFactory.getLog(KmeansClusterer.class);

	private static final int MAX_ITERATIONS = 1000;

	private static final double DRIFT_TOLERANCE = 0.005d;

	private static final double OVERLAP_FACTOR = 1.00d;

	// the set of microclusters that this instance of the Clusterer maintains.
	private List<ClusandraKernel> microClusters = new ArrayList<ClusandraKernel>();

	// this clusterer's CassandraDao
	private ClusandraDao clusandraDao = null;

	// The overlap factor controls the merging of clusters. If the factor
	// is set to 1.0, then the two clusters will merge iff their radii
	// overlap. If the factor is set to 0.5, then the two will merge iff
	// one-half their radii overlap. So in the latter case, the clusters
	// must be much closer to one another.
	private double overlapFactor = OVERLAP_FACTOR;

	// Lambda is the forgetfulness factor. It dictates how quickly a set of
	// DataRecords becomes temporally irrelevant. The lower the value for
	// lambda, the quicker the set becomes irrelevant. When a set becomes
	// temporally irrelevant it is clustered and the resulting micro-clusters
	// are sent on to the CTreeClusterer.
	private double lambda = 0.5d;

	// the density, as a factor of maximum density, that a set of DataRecords is
	// considered irrelevant. So if the factor is set to 0.25, then the set
	// becomes temporally irrelevant if its density falls below 25% of its
	// maximum density.
	private double sparseFactor = 0.25d;

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// ~~~~~~~~~~~~~ This Algorithm's Configurable Properties ~~~~~~~~~~~~
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// the amount of time a microcluster is allowed to remain active without
	// absorbing a Datarecord
	private static final String overlapFactorKey = "overlapFactor";
	private static final String sparseFactorKey = "sparseFactor";
	private static final String lambdaKey = "lambda";
	private static final String driftToleranceKey = "driftTolerance";

	// default maximum number of iterations before giving up
	// on convergence
	private int maxIterations = MAX_ITERATIONS;

	private double driftTolerance = DRIFT_TOLERANCE;

	// this clusterers queue agent. this clusterer will both read and send
	// messages from and to a queue, respectively
	private QueueAgent queueAgent;

	private double currentDensity;

	// this list is used for grouping DataRecords that are temporally relevant
	// to one another
	List<DataRecord> recordsToCluster = new ArrayList<DataRecord>();

	public KmeansClusterer() {
		super();
	}

	/**
	 * Invoked by Spring to set the Map that contains configuration parameters
	 * for this Clusterer.
	 * 
	 * @param map
	 */
	public void setConfig(Map<String, String> map) throws Exception {
		for (String key : map.keySet()) {
			if (overlapFactorKey.equals(key)) {
				setOverlapFactor(map.get(key));
				LOG.trace("setConfig:cluster overlapFactor = "
						+ getOverlapFactor());
			} else if (sparseFactorKey.equals(key)) {
				setSparseFactor(map.get(key));
				LOG.trace("setConfig:cluster sparseFactor = "
						+ getSparseFactor());
			} else if (lambdaKey.equals(key)) {
				setLambda(map.get(key));
				LOG.trace("lambda = " + getLambda());
			} else if (driftToleranceKey.equals(key)) {
				setDriftTolerance(map.get(key));
				LOG.trace("driftToleranceKey = " + getDriftTolerance());
			}
		}
	}

	/**
	 * Invoked by Spring to set the QueueAgent for this Processor.
	 * 
	 * @param map
	 */
	public void setQueueAgent(QueueAgent queueAgent) {
		this.queueAgent = queueAgent;

	}

	/**
	 * Returns the QueueAgent that is wired to this Processor.
	 * 
	 * @param map
	 */
	public QueueAgent getQueueAgent() {
		return queueAgent;
	}

	/**
	 * Called by setConfig() to set the cluster overlap factor.
	 * 
	 * @param overlapFactor
	 * @throws Exception
	 */
	public void setOverlapFactor(String overlapFactor) throws Exception {
		setOverlapFactor(Double.parseDouble(overlapFactor));
	}

	public void setOverlapFactor(double overlapFactor)
			throws IllegalArgumentException {

		if (overlapFactor < 0.0) {
			throw new IllegalArgumentException(
					"invalid overlapFactor specified; value must be > 0.0");
		}
		this.overlapFactor = overlapFactor;
	}

	/**
	 * Returns the cluster overlap factor being used.
	 * 
	 * @return
	 */
	public double getOverlapFactor() {
		return overlapFactor;
	}

	/**
	 * Called by setConfig() to set the cluster sparse factor.
	 * 
	 * @param sparseFactor
	 * @throws Exception
	 */
	public void setSparseFactor(String sparseFactor) throws Exception {
		setSparseFactor(Double.parseDouble(sparseFactor));
	}

	public void setSparseFactor(double sparseFactor)
			throws IllegalArgumentException {

		if (sparseFactor <= 0.0 || sparseFactor >= 1.0) {
			throw new IllegalArgumentException(
					"invalid sparseFactor specified; value must be > 0.0 and < 1.0");
		}
		this.sparseFactor = sparseFactor;
	}

	/**
	 * Returns the cluster sparse factor being used.
	 * 
	 * @return
	 */
	public double getSparseFactor() {
		return sparseFactor;
	}

	/**
	 * Called by setConfig() to set the cluster lambda value for density
	 * calculations.
	 * 
	 * @param lambda
	 * @throws Exception
	 */
	public void setLambda(String lambda) throws Exception {
		setLambda(Double.parseDouble(lambda));
	}

	public void setLambda(double lambda) throws IllegalArgumentException {

		if (lambda <= 0.0d || lambda >= 1.0d) {
			throw new IllegalArgumentException(
					"invalid lambda specified; value must be > 0.0 and < 1.0");
		}
		this.lambda = lambda;
	}

	/**
	 * Returns the lambda value for density calculations.
	 * 
	 * @return
	 */
	public double getLambda() {
		return lambda;
	}

	/**
	 * Called by setConfig() to set the drift tolerance.
	 * 
	 * @param driftTolerance
	 * @throws Exception
	 */
	public void setDriftTolerance(String driftTolerance) throws Exception {
		setDriftTolerance(Double.parseDouble(driftTolerance));
	}

	public void setDriftTolerance(double driftTolerance)
			throws IllegalArgumentException {

		if (driftTolerance <= 0.0d) {
			throw new IllegalArgumentException(
					"invalid driftTolerance specified; value must be > 0.0");
		}
		this.driftTolerance = driftTolerance;
	}

	/**
	 * Returns the cluster drift tolerance.
	 * 
	 * @return
	 */
	public double getDriftTolerance() {
		return driftTolerance;
	}

	/**
	 * 
	 * @return The maximum density, which is 1/(1-lambda).
	 */
	private double getMaximumDensity() {
		return (1.0d / (1.0d - getLambda()));
	}

	private double getDensityRange() {
		return getMaximumDensity() - 1.0d;
	}

	private void setCurrentDensity(double density) {
		this.currentDensity = density;
	}

	private double getCurrentDensity() {
		return this.currentDensity;
	}

	private void resetCurrentDensity() {
		setCurrentDensity(getMaximumDensity());
	}

	/**
	 * Called by Spring to wire this clusterer to its ClusandraDao object
	 */
	public void setClusandraDao(ClusandraDao clusandraDao) {
		this.clusandraDao = clusandraDao;
	}

	/**
	 * Get this clusterer's ClusandraDao. Not used by this processor.
	 * 
	 * @return
	 */
	public ClusandraDao getClusandraDao() {
		return clusandraDao;
	}

	/**
	 * Called by QueueAgent to initialize this Clusterer.
	 */
	public boolean initialize() throws Exception {
		if (getQueueAgent() == null) {
			throw new Exception(
					"this clusterer has not been wired to a QueueAgent");
		} else if (getQueueAgent().getJmsWriteDestination() == null) {
			throw new Exception(
					"this clusterer has not been wired to a JmsWriteDestination");
		} else if (getQueueAgent().getJmsWriteTemplate() == null) {
			throw new Exception(
					"this clusterer has not been wired to a JmsWriteTemplate");
		}
		return true;
	}

	/**
	 * Called by the QueueAgent to give the Clusterer a collection of
	 * DataRecords to process. The collection is read from the QueueAgent's JMS
	 * queue. DataRecords are then clustered in groups, where all members of a
	 * particular group are temporally relevant.
	 * 
	 * Design note: This method can be called by the QueueAgent within the
	 * context of a local JMS transaction. So not until this method returns,
	 * without throwing an Exception, would the QueueAgent commit the
	 * transaction. The transaction can be rolled back if an exception is
	 * thrown; thus preserving the DataRecords at the JMS queue. The general
	 * idea is to ensure that the messages remain in the queue until this
	 * processing phase has successfully completed its work.
	 * 
	 * @param dataRecords
	 * @throws Exception
	 */
	public void processDataRecords(List<DataRecord> dataRecords)
			throws Exception {

		LOG.debug("processDataRecords: entered");

		if (dataRecords == null || dataRecords.size() == 0) {
			LOG.debug("processDataRecords: dataRecords is null or empty");
			return;
		}

		LOG.debug("processDataRecords: number of DataRecords received = "
				+ dataRecords.size());

		// first sort the list of data records in ascending timestamp order
		Collections.sort(dataRecords);

		// reset the current temporal density to the maximum density
		resetCurrentDensity();

		// cluster the given data records in groups where all records in a group
		// are temporally relevant to one another
		for (ListIterator<DataRecord> li = dataRecords.listIterator(); li
				.hasNext();) {
			DataRecord record = li.next();
			if (!recordsToCluster.isEmpty()) {
				// grab the time stamp of the last record in the current
				// grouping and run a density check between that time
				// and the new record's time.
				double density = 0.0d;
				// the density algorithm is based on seconds, so convert from
				// ms to seconds
				double lastTime = recordsToCluster.get(
						recordsToCluster.size() - 1).getTimestamp() / 1000;
				double newTime = record.getTimestamp() / 1000;
				double delta = Math.round((newTime / 1000) - (lastTime / 1000));
				if (delta <= 0.0d) {
					density = getCurrentDensity();
				} else {
					density = Math.pow(getLambda(), delta)
							* getCurrentDensity() + 1.0d;
				}
				// if this new record is temporally relevant with the group,
				// save the density as the current density
				if (((density - 1.0d) / getDensityRange()) >= getSparseFactor()) {
					setCurrentDensity(density);
				} else {
					// the new record is not temporally relevant with the
					// current group, so cluster the existing group and
					// start a new one
					clusterDataRecords(recordsToCluster);
					recordsToCluster.clear();
					resetCurrentDensity();
				}
			}
			recordsToCluster.add(record);
			li.remove();
		}
		clusterDataRecords(recordsToCluster);
		recordsToCluster.clear();
		LOG.debug("processDataRecords: exit");
	}

	/**
	 * Cluster a group of temporally relevant occurrences in the point space.
	 * The result is a set of micro-clusters that is sent on to the
	 * BTreeClusterer.
	 * 
	 * @param dataRecords
	 * @throws Exception
	 */
	private void clusterDataRecords(List<DataRecord> dataRecords)
			throws Exception {

		LOG.debug("clusterDataRecords: entered");

		// do a little validation
		if (dataRecords == null || dataRecords.isEmpty()) {
			LOG.warn("clusterDataRecords: entered with empty DataRecord list");
			return;
		} else if (dataRecords.size() < 4) {
			throw new Exception(
					"clusterDataRecords: entered with not enough DataRecords");
		}

		LOG.debug("clusterDataRecords: number of DataRecords received = "
				+ dataRecords.size());

		int numClusters = (int) Math.sqrt(dataRecords.size());

		LOG.debug("clusterDataRecords: initial number of clusters = "
				+ numClusters);

		// using kmeans++, create the initial set of clusters from the set
		// of points provided.
		KmeansKernel[] clusters = kmeansPlusPlus(dataRecords, numClusters);

		// begin the kmeans loop
		int numIterations = 0;
		long startTime = System.currentTimeMillis();
		for (; numIterations < maxIterations; numIterations++) {

			// [re]assign the clusters to their nearest neighbor
			assignClusters(clusters);

			// now assign all points to their nearest cluster centroid
			for (DataRecord point : dataRecords) {

				double minDist = Double.MAX_VALUE;
				KmeansKernel minCluster = point.getKmeansKernel();

				// if the point is assigned to a cluster and the
				// distance from this point to the cluster is less
				// than 1/2 the distance from that cluster to its
				// nearest neighbor, then by the triangle inequality,
				// the point can be ignored; i.e., it can remain
				// assigned to its present cluster. this usually
				// works best when the number of clusters is greater
				// than 20
				if (minCluster != null) {
					minDist = ClusandraClusterer.getDistance(
							point.toDoubleArray(), minCluster.getLocation());
					if (minDist <= (0.5 * minCluster.getDistNN())) {
						continue;
					}
				}

				// find this points nearest cluster centroid
				minDist = Double.MAX_VALUE;
				minCluster = null;
				for (KmeansKernel cluster : clusters) {
					double distance = ClusandraClusterer.getDistance(
							cluster.getLocation(), point.toDoubleArray());
					if (distance < minDist) {
						minDist = distance;
						minCluster = cluster;
					}
				}
				// move this point from its current cluster to its new
				// closest cluster
				if (point.getKmeansKernel() != null) {
					point.getKmeansKernel().removePoint(point);
				}
				// assign point to its new cluster
				minCluster.addPoint(point);
			}

			boolean converged = true;
			// [re]calculate the amount of drift for each and every
			// cluster. important to do it for all clusters even if
			// they have not converged.
			for (KmeansKernel cluster : clusters) {
				if (cluster.getPoints().size() > 0) {
					double driftDistance = ClusandraClusterer.getDistance(
							cluster.getMean(), cluster.getLocation());
					if (driftDistance > driftTolerance) {
						converged = false;
						cluster.setDriftDistance(driftDistance);

					} else {
						cluster.setDriftDistance(0.0);
					}
					// move the cluster to its new location - even if
					// it is a minute amount
					cluster.setLocation(cluster.getMean());
				}
			}

			// we're done if all clusters have converged
			if (converged || numIterations == (maxIterations - 1)) {
				break;
			}

		}
		long endTime = System.currentTimeMillis();
		LOG.debug("clusterDataRecords: number of iterations = " + numIterations);
		LOG.debug("clusterDataRecords: number of clusters prior to merge = "
				+ clusters.length);
		LOG.debug("clusterDataRecords: number of ms per iteration = "
				+ ((endTime - startTime) / numIterations));

		// merge the clusters
		clusters = mergeClusters(clusters);

		LOG.debug("clusterDataRecords: number of clusters after merge = "
				+ clusters.length);

		// now transform the kmeans clusters into a micro-cluster and
		// send it off to the queue
		for (KmeansKernel cluster : clusters) {
			cluster.initMicro();
			getQueueAgent().sendMessage(new CluMessage(cluster));
		}
		// send out any stragglers
		getQueueAgent().flush();

		LOG.debug("clusterDataRecords: exit");

	}

	public void processCluMessages(List<CluMessage> cluMessages)
			throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
	 * Return the working set of microclusters for this clusterer
	 * 
	 * @return
	 */
	public List<ClusandraKernel> getMicroClusters() {
		return microClusters;
	}

	/**
	 * Used for seeding the algorithm using the k-means++.
	 * 
	 * The idea behind k-means++ is to spread out the initial clusters as much
	 * as possible and thus avoid settling on a local optimum; outliers should
	 * also be avoided.
	 * 
	 * @param points
	 *            represents the points in the point space
	 * @param numClusters
	 *            requested number of clusters
	 * @return
	 */
	private KmeansKernel[] kmeansPlusPlus(List<DataRecord> points,
			int numClusters) {

		LOG.debug("kmeansPlusPlus: entered");
		LOG.debug("kmeansPlusPlus: number of points = " + points.size());
		LOG.debug("kmeansPlusPlus: number of clusters = " + numClusters);
		LOG.debug("kmeansPlusPlus: overlap factor = " + getOverlapFactor());

		// always use a different seed! if not, you always end up with the
		// same initial cluster pattern
		Random randomGen = new Random();

		// this will hold the set of clusters that will be returned to the
		// caller
		KmeansKernel[] clusters = new KmeansKernel[numClusters];

		// keeps track of number of clusters created
		int clusCount = 0;

		// randomly find our first cluster centroid from the given points
		// and mark it as being selected as a centroid
		int randomIndex = randomGen.nextInt(points.size());

		clusters[clusCount] = new KmeansKernel(points.get(randomIndex));

		// mark the point as already being chosen as a cluster centroid
		points.get(randomIndex).setCentroid(true);

		clusCount++;

		// we could have stuck the above in the loop below, but doing so
		// would have added extra overhead to the loop

		// main loop that creates the rest of the clusters
		for (; clusCount < numClusters; clusCount++) {

			// this will hold the sum of all the distances from each point to
			// its nearest cluster.
			double sumDistance = 0.0;

			// assign all points to their nearest cluster and record
			// their distances to that cluster
			for (DataRecord point : points) {

				// skip those points that have already been
				// selected as centroids.
				if (point.isCentroid()) {
					continue;
				}

				// find this point's closest cluster
				double dxb = 0.0D;
				double minDistance = Double.MAX_VALUE;
				for (int i = 0; i < clusCount; i++) {
					dxb = ClusandraClusterer.getDistance(
							clusters[i].getLocation(), point.toDoubleArray());
					if (dxb < minDistance) {
						minDistance = dxb;
					}
				}
				sumDistance += (minDistance * minDistance);
				point.setDistanceToCluster(minDistance);
			}

			// find the overall probability factor
			double probability = (1 / sumDistance);
			double maxProbability = 0.0;
			DataRecord maxPoint = null;
			// find the point with the highest ratio, which will be our next
			// center
			for (DataRecord point : points) {
				if (point.isCentroid()) {
					continue;
				}
				double d2 = point.getDistanceToCluster();
				d2 = (d2 * d2) * probability;
				if (d2 > maxProbability) {
					maxProbability = d2;
					maxPoint = point;
				}
			}
			clusters[clusCount] = new KmeansKernel(maxPoint);
		}

		LOG.debug("kmeansPlusPlus: exit");

		return clusters;
	}

	/**
	 * Merge clusters that overlap. The overlap is based on the distance between
	 * the two clusters and their radii. A cluster that absorbs another cluster
	 * will evolve into a larger cluster and its mean and location must be
	 * updated. As a cluster evolves into a bigger cluster, it may absorb other
	 * clusters
	 * 
	 * @param clusters
	 * @return
	 */
	private KmeansKernel[] mergeClusters(KmeansKernel[] clusters) {

		if (clusters == null || clusters.length == 0) {
			return clusters;
		}

		int mergeCount = 0;
		boolean merged = false;

		do {
			merged = false;
			for (int i = 0; i < clusters.length; i++) {
				// continue if this cluster had been previously
				// absorbed by another cluster
				if (clusters[i] == null) {
					continue;
				}
				// scale the 1st radius with the given overlap factor
				double r1 = getOverlapFactor() * getStdDev(clusters[i]);
				if (r1 == 0.0) {
					continue;
				}
				for (int j = 0; j < clusters.length; j++) {

					// continue if this cluster had been previously
					// absorbed by another cluster
					if (i == j || clusters[j] == null) {
						continue;
					}

					// scale the 2nd radius with the given overlap factor
					double r2 = getOverlapFactor() * getStdDev(clusters[j]);

					// if the sum of the two scaled radii is larger than the
					// distance between the two clusters, then the two
					// overlap and will be merged or one absorbs the other
					double dist = ClusandraClusterer.getDistance(
							clusters[i].getLocation(),
							clusters[j].getLocation());
					// merge the two clusters if there is enough of an overlap
					if (((r1 + r2) - dist) > 0) {
						clusters[i].merge(clusters[j]);
						// GC the absorbed cluster.
						clusters[j] = null;
						++mergeCount;
						merged = true;
					}
				}
			}
			// continue merging until clusters no longer merge
			// as a cluster merges or absorbs another, its radius will
			// grow and thus may overlap with new clusters.
		} while (merged);

		// if clusters never merged, then return
		if (mergeCount == 0) {
			return clusters;
		}

		// the number of clusters has been reduced to original number - number
		// absorbed or merged
		KmeansKernel[] newClusters = new KmeansKernel[clusters.length
				- mergeCount];

		// pick up the merged clusters
		mergeCount = 0;
		for (int i = 0; i < clusters.length; i++) {
			if (clusters[i] != null) {
				newClusters[mergeCount++] = clusters[i];
			}
		}

		return newClusters;
	}

	/**
	 * Get the standard deviation of all the cluster's points relative to the
	 * cluster's center of gravity.
	 * 
	 * @param cluster
	 * @return
	 */
	public double getStdDev(KmeansKernel cluster) {
		if (cluster == null || cluster.getPoints().size() <= 1) {
			return 0.0;
		}
		return Math.sqrt(getVariance(cluster));
	}

	/**
	 * Get the variance for the cluster.
	 * 
	 * @param cluster
	 * @return
	 */
	public double getVariance(KmeansKernel cluster) {
		double N = cluster.getPoints().size();
		if (N <= 1) {
			return 0.0;
		}
		double mean[] = cluster.getMean();
		double sum = 0.0D;
		for (DataRecord point : cluster.getPoints()) {
			double dist = ClusandraClusterer.getDistance(mean,
					point.toDoubleArray());
			sum += (dist * dist);
		}
		return sum / N;
	}

	/**
	 * Assign each cluster to its nearest neighbor
	 * 
	 * @param clusters
	 */
	public void assignClusters(KmeansKernel[] clusters) {
		for (int i = 0; i < clusters.length; i++) {
			double minDist = Double.MAX_VALUE;
			KmeansKernel minCluster = null;
			for (int j = 0; j < clusters.length; j++) {
				if (i != j) {
					double distance = ClusandraClusterer.getDistance(
							clusters[i].getLocation(),
							clusters[j].getLocation());
					if (distance < minDist) {
						minCluster = clusters[j];
						minDist = distance;
					}
				}
			}
			clusters[i].setNN(minCluster);
			clusters[i].setDistNN(minDist);
		}
	}

}
