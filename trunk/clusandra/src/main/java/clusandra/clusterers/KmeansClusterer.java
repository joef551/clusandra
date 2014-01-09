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

import java.util.Arrays;
import java.util.ListIterator;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Random;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import clusandra.core.AbstractProcessor;
import clusandra.core.CluMessage;
import clusandra.utils.StatUtils;

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
 * (occurrences in point space) to produce microclusters. Note its a list and
 * not a set because it is conceivable that duplicate points may exist. It sends
 * the resulting microclusters to the specified message queue. The micro
 * clusters sent to the queue are then clustered into a BTree by one central
 * BTreeClusterer. The idea being that you can have many instances of the
 * KmeansClusterer working in parallel to harness the data stream's records,
 * while the single instance of the BTreeClusterer acts as a reducer of sorts.
 * The BTreeClusterer persists micro-clusters to the Cassandra DB.
 * 
 * The temporalDensity portion of this work is based, in part, on the following paper.
 * 
 * Citation: Yixin Chen, Li Tu: Density-Based Clustering for Real-Time Stream
 * Data. KDD '07
 * 
 * The above paper describes an approach for managing the temporal temporalDensity of
 * microclusters or data points without having to visit a microcluster each and
 * every time period; as described in:
 * 
 * Citation: Feng Cao, Martin Ester, Weining Qian, Aoying Zhou: Density-Based
 * Clustering over an Evolving Data Stream with Noise. SDM 2006
 * 
 * @author jfernandez
 * 
 */
public class KmeansClusterer extends AbstractProcessor {

	private static final Log LOG = LogFactory.getLog(KmeansClusterer.class);

	private static final int MAX_ITERATIONS = 1000;

	private static final double DRIFT_TOLERANCE = 0.005d;

	private static final double OVERLAP_FACTOR = 1.00d;

	/*
	 * The overlap factor controls the merging of clusters. If the factor is set
	 * to 1.0, then the two clusters will merge iff their radii overlap. In
	 * other words, iff they are within one standard deviation of another. If
	 * the factor is set to 0.5, then the two will merge iff one-half their
	 * radii overlap. So in the latter case, the clusters must be much closer to
	 * one another.
	 */
	private double overlapFactor = OVERLAP_FACTOR;
	/*
	 * Lambda is the forgetfulness factor. It dictates how quickly a set of
	 * DataRecords becomes temporally irrelevant. The lower the value for
	 * lambda, the quicker the set becomes irrelevant. When a set becomes
	 * temporally irrelevant it is clustered and the resulting microclusters are
	 * sent on to the BTreeClusterer.
	 */
	private double lambda = 0.5d;

	/*
	 * the temporalDensity, as a factor of maximum temporalDensity, that a set of DataRecords is
	 * considered irrelevant. So if the factor is set to 0.25, then the set
	 * becomes temporally irrelevant if its temporalDensity falls below 25% of its
	 * maximum temporalDensity.
	 */
	private double sparseFactor = 0.25d;

	// when set to true, invokes the optimized euclidean distance algorithm
	private boolean fastDistance;

	// default maximum number of iterations before giving up
	// on convergence
	private int maxIterations = MAX_ITERATIONS;

	// the drift tolerance for kmeans
	private double driftTolerance = DRIFT_TOLERANCE;

	// used for temporal temporalDensity calculation
	private double currentDensity;

	// the choke is used in the iterative reduction phase; the selected value,
	// which is relatively high, corresponds with an initial clustering that is
	// very cluster'able. for example, the clusters in the set are
	// well-separated and have low variance. the choke is decreased if it is
	// determined that the clustering is not as cluster'able
	private static final double DEFAULT_CHOKE = 0.35D;
	private double choke = DEFAULT_CHOKE;

	// these three variables are used for incremental reduction of clusters
	private double baseVariance = 0.01D;
	private double varianceInterval = 0.0065D;
	private double reductionRate = 0.16D;

	// these are labels used to assign a cluster'ability level to the various
	// statistics associated with an initial cluster set.
	private enum Measure {
		VERY_LOW, LOW, MEDIUM, HIGH, VERY_HIGH;
	}

	// used to specify how hard the overall algorithm should work to find the
	// optimal solution
	private int numRetries = 2;

	// this list is used for grouping DataRecords (points) that are temporally
	// relevant to one another
	List<DataRecord> pointsToCluster = new ArrayList<DataRecord>();

	public KmeansClusterer() {
		super();
	}

	public double getChoke() {
		return choke;
	}

	public void setChoke(double choke) {
		this.choke = choke;
	}

	public int getNumRetries() {
		return numRetries;
	}

	public void setNumRetries(int numRetries) {
		this.numRetries = numRetries;
	}

	public double getReductionRate() {
		return reductionRate;
	}

	public void setReductionRate(double reductionRate) {
		this.reductionRate = reductionRate;
	}

	public double getBaseVariance() {
		return baseVariance;
	}

	public void setBaseVariance(double baseVariance) {
		this.baseVariance = baseVariance;
	}

	public double getVarianceInterval() {
		return varianceInterval;
	}

	public void setVarianceInterval(double varianceInterval) {
		this.varianceInterval = varianceInterval;
	}

	public void setOverlapFactor(double overlapFactor)
			throws IllegalArgumentException {

		if (overlapFactor < 0.0) {
			throw new IllegalArgumentException(
					"invalid overlapFactor specified; value must be > 0.0");
		}
		this.overlapFactor = overlapFactor;
	}

	public double getOverlapFactor() {
		return overlapFactor;
	}

	public void setSparseFactor(double sparseFactor)
			throws IllegalArgumentException {

		if (sparseFactor <= 0.0 || sparseFactor >= 1.0) {
			throw new IllegalArgumentException(
					"invalid sparseFactor specified; value must be > 0.0 and < 1.0");
		}
		this.sparseFactor = sparseFactor;
	}

	public double getSparseFactor() {
		return sparseFactor;
	}

	public void setLambda(double lambda) throws IllegalArgumentException {

		if (lambda <= 0.0d || lambda >= 1.0d) {
			throw new IllegalArgumentException(
					"invalid lambda specified; value must be > 0.0 and < 1.0");
		}
		this.lambda = lambda;
	}

	public double getLambda() {
		return lambda;
	}

	public void setDriftTolerance(double driftTolerance)
			throws IllegalArgumentException {

		if (driftTolerance <= 0.0d) {
			throw new IllegalArgumentException(
					"invalid driftTolerance specified; value must be > 0.0");
		}
		this.driftTolerance = driftTolerance;
	}

	public double getDriftTolerance() {
		return driftTolerance;
	}

	public void setFastDistance(boolean fastDistance) {
		this.fastDistance = fastDistance;
	}

	public boolean getFastDistance() {
		return fastDistance;
	}

	/**
	 * 
	 * @return The maximum temporalDensity, which is 1/(1-lambda).
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
	 * Called by the QueueAgent to give the Clusterer a collection of
	 * DataRecords to process. The collection is read from the QueueAgent's JMS
	 * queue. DataRecords are then clustered in groups, where all members of a
	 * particular group are temporally relevant.
	 * 
	 * The DataRecords are treated as points in a point-space.
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
	public void processCluMessages(List<CluMessage> cluMessages)
			throws Exception {

		LOG.debug("processCluMessages: entered");

		if (cluMessages == null || cluMessages.size() == 0) {
			LOG.debug("processCluMessages: cluMessage list is null or empty");
			return;
		}

		List<DataRecord> dataPoints = new ArrayList<DataRecord>();
		for (CluMessage cMsg : cluMessages) {
			Serializable obj = cMsg.getBody();
			if (obj instanceof DataRecord) {
				dataPoints.add((DataRecord) obj);
			} else {
				throw new Exception(
						"ERROR, cluMessage does not contain a DataRecord");
			}
		}

		LOG.debug("processCluMessages: number of DataRecords received = "
				+ dataPoints.size());

		// ensure that all data points passed in have the same
		// dimension!
		int dims = dataPoints.get(0).getLocation().length;
		LOG.debug("processCluMessages: number of dimensions = " + dims);
		for (DataRecord point : dataPoints) {
			if (point.getLocation().length != dims) {
				throw new Exception(
						"ERROR: all points do not have the same dimensions");
			}
		}

		// normalize the points in their point space
		normalizePoints(dataPoints);

		// sort the list of points in ascending timestamp order
		Collections.sort(dataPoints);

		// reset the current temporal temporalDensity to the maximum temporalDensity
		resetCurrentDensity();

		/*
		 * cluster the given points into groups where all points in a group are
		 * temporally relevant to one another; spatial clustering is next
		 * 
		 * The temporalDensity of the group is calculated as follows:
		 * 
		 * D(g,tn) = (lambda^tn-tl) * D(g,tl) + 1;
		 * 
		 * Where D(g,tn) is the denisty with the new data point and D(g,tl) is
		 * the denisty with the last added data point. tn and tl are the
		 * timestamps of the new and last data points, respectively.
		 */
		for (ListIterator<DataRecord> li = dataPoints.listIterator(); li
				.hasNext();) {
			DataRecord point = li.next();
			if (!pointsToCluster.isEmpty()) {
				// grab the time stamp of the last record in the current
				// grouping and run a temporalDensity check between that time
				// and the new record's time.
				double density = 0.0d;
				// the temporalDensity algorithm is based on seconds, so convert from
				// ms to seconds
				double lastTime = pointsToCluster.get(
						pointsToCluster.size() - 1).getTimestamp();
				double newTime = point.getTimestamp();
				double delta = (newTime - lastTime) / 1000;
				if (delta <= 0.0d) {
					density = getCurrentDensity();
				} else {
					density = Math.pow(getLambda(), delta)
							* getCurrentDensity() + 1.0d;
				}
				// if this new record is temporally relevant with the group,
				// add the new data point and save the temporalDensity as the current
				// temporalDensity
				if (((density - 1.0d) / getDensityRange()) >= getSparseFactor()) {
					setCurrentDensity(density);
				} else {
					// the new record is not temporally relevant with the
					// current group, so cluster the existing group and
					// start a new one
					clusterDataPoints(pointsToCluster);
					pointsToCluster.clear();
					resetCurrentDensity();
				}
			}
			pointsToCluster.add(point);
			li.remove();
		}
		clusterDataPoints(pointsToCluster);
		pointsToCluster.clear();
		LOG.debug("processCluMessages: exit");
	}

	/**
	 * Cluster the temporally related points into spatially similar points.
	 * 
	 * @param dataPoints
	 * @throws Exception
	 */
	private void clusterDataPoints(List<DataRecord> dataPoints)
			throws Exception {

		// get the initial number of clusters, which is the square root of the
		// number of points
		int numClusters = (int) Math.sqrt(dataPoints.size());

		LOG.debug("clusterDataPoints: initial cluster count = " + numClusters);

		// generate the initial set of clusters
		KmeansCluster[] clusters = runKmeans(dataPoints, numClusters);

		// begin merge-reduction process

		// first, begin by merging those clusters whose radius (standard
		// deviation of 1.0) overlap. This forms the base set from which to
		// start the iterative reduction process
		clusters = mergeClusters(clusters);

		numClusters = clusters.length;

		LOG.debug("clusterDataPoints: Cluster count after merge = "
				+ numClusters);

		Measure separation;

		// now we gather some descriptive statics for the clusters,
		// this allows us to make decisions and/or conclusions
		// regarding the structure of the clusters; e.g., the
		// stats may indicate that the structure associated with
		// this initial merged set of clusters is not conducive
		// for further reduction via the iterative approach

		// get the average variance of the cluster set
		double avgVariance = getAvgVariance(clusters);
		LOG.debug("clusterDataPoints: Avg variance = " + avgVariance);

		// get the standard deviation of the new clusters' overall
		// standard deviations. this will be used to measure the
		// increased or decreased variance, across all the standard
		// deviations, as a result of reducing the cluster count
		// by one.
		double stdDev = getStdDevOfClusters(clusters);
		double baseAvgStdDev = stdDev;
		LOG.debug("clusterDataPoints: Initial baseAvgStdDev = " + baseAvgStdDev);

		// get the average distance between the clusters' 1.0 radii
		double avgRadiiDist = getAvgRadiusDistance(clusters);
		LOG.debug("clusterDataPoints: Avg distance between clusters' radii = "
				+ avgRadiiDist);

		double tmpChoke = getChoke();

		// the more variance there is across the cluster set, the more
		// subtle will be the increase in the variance across that cluster
		// set as you reduce the set.

		// if the choke has not been specified and the average
		// variance is greater than the minimum (baseVariance), then
		// the choke will need to be reduced. the amount of the reduction is
		// dictated by the amount of variance across the cluster set. the
		// more variance the more the choke will need to be reduced.
		if (tmpChoke == DEFAULT_CHOKE && avgVariance > baseVariance) {
			/*
			 * subtract the baseVariance from the avgVariance, then divide by
			 * the step interval. this in turn gives us the number of steps to
			 * reduce, where each step has a reduction rate. e.g., suppose the
			 * average variance for the cluster set is 0.0164, the baseVarance
			 * is set at 0.01, the varianceInterval is set at 0.0065, the
			 * default choke is set at 0.35, and the reductionRate is 0.14. The
			 * difference between the base and average variance is then 0.0065,
			 * which also happens to be the varianceInterval; therefore the
			 * number of steps to reduce by is 1. So multiply 1 times the
			 * reductionRate which gives you 0.14, then subtract that from 1 and
			 * multiply it times the choke.
			 */
			tmpChoke = (1 - (((avgVariance - baseVariance) / varianceInterval) * reductionRate))
					* tmpChoke;
			LOG.debug("clusterDataPoints: calculated choke  = " + tmpChoke);
		}

		LOG.debug("clusterDataPoints: final choke  = " + tmpChoke);

		/*
		 * if the final choke is less than 0, then this temporal grouping of
		 * data points is not clusterable. the grouping represents stragglers
		 * that pertain to some other main grouping or all or some subset can be
		 * outliers. if this is the case, pass each data point as a microcluster
		 * onto the btree clusterer (i.e., reducer).
		 */
		if (tmpChoke <= 0) {
			for (DataRecord dataPoint : dataPoints) {
				dataPoint.restoreAttValues();
				// create microcluster
				MicroCluster mc = new MicroCluster(
						dataPoint.getLocation().length);
				// now have the microcluster absorb the data point, and
				// enqueue the microcluster
				mc.absorb(dataPoint);
				getQueueAgent().sendMessage(mc);
			}
			getQueueAgent().flush();
			LOG.debug("clusterDataRecords: exiting after sending on stragglers");
			return;
		}

		// label the average distance between the clusters' radii
		if (avgRadiiDist > 1.8) {
			separation = Measure.VERY_HIGH;
		} else if (avgRadiiDist >= 1.5) {
			separation = Measure.HIGH;
		} else if (avgRadiiDist >= 1.4) {
			separation = Measure.MEDIUM;
		} else {
			separation = Measure.LOW;
		}

		LOG.debug("clusterDataPoints: separation  = " + separation);

		double runningBaseStdDev = stdDev;
		double baseCount = 1.0;
		double deltaStdDev = 0.0D;
		int retry = numRetries;
		KmeansCluster[] newClusters = null;
		double newStdDev = Double.MAX_VALUE;

		while (retry > 0 && numClusters > 1) {

			// reset the points so that they can be reused
			for (DataRecord point : dataPoints) {
				point.reset();
			}

			// perform kmeans on the new reduced cluster count
			newClusters = runKmeans(dataPoints, --numClusters);

			// get the new standard deviation of the new clusters'
			// overall standard deviations.
			newStdDev = getStdDevOfClusters(newClusters);

			// calculate the percentage increase or decrease from
			// the base average standard deviation. if the change was
			// too high, then ignore this set and try it again if
			// there are enough retries left.
			deltaStdDev = (newStdDev - baseAvgStdDev) / baseAvgStdDev;

			// LOG.trace("clusterDataPoints: New/prev stdDev = " + newStdDev +
			// " / " + stdDev);
			// LOG.trace("clusterDataPoints: deltaStdDev = " + deltaStdDev);

			// if the increase is very high and the separation is also
			// very high, then stop the reduction process
			if (deltaStdDev >= 1.0 && separation == Measure.VERY_HIGH) {
				LOG.trace("clusterDataPoints: premature exit");
				retry = 0;
			}

			// if the percentage change was acceptable, then keep the
			// new reduced set of clusters.
			else if (deltaStdDev < tmpChoke) {
				clusters = newClusters;
				stdDev = newStdDev;
				runningBaseStdDev += stdDev;
				baseAvgStdDev = runningBaseStdDev / (++baseCount);
				retry = numRetries;

				LOG.trace("clusterDataPoints: New cluster count = "
						+ numClusters);
				LOG.trace("clusterDataPoints: New running baseAvgStdDev = "
						+ baseAvgStdDev);

			} else {
				--retry;
				++numClusters;
			}
		}
		LOG.debug("clusterDataPoints: cluster count after cost reduction = "
				+ clusters.length);

		// one final attempt at a merge
		if (clusters.length > 1) {
			clusters = mergeClusters(clusters);
		}

		LOG.debug("clusterDataPoints: Cluster count after final merge = "
				+ clusters.length);
		LOG.trace("clusterDataPoints: Final avg distance between clusters' radii = "
				+ getAvgRadiusDistance(clusters));
		LOG.trace("clusterDataPoints: Final avg variance = "
				+ getAvgVariance(clusters));
		LOG.trace("clusterDataPoints: Final cluster standard deviation = "
				+ getStdDevOfClusters(clusters));

		// lastly, transform the final cluster set into a set of microclusters
		// and enqueue the microclusters for the btree clusterer
		for (KmeansCluster cluster : clusters) {
			// first sort the list of the cluster's data records in ascending
			// timestamp order
			Collections.sort(cluster.getPoints());
			// create microcluster
			MicroCluster mc = new MicroCluster(cluster.getLocation().length);
			// now have the microcluster absorb all of this kmeans cluster's
			// data points; the first (oldest) data point will be the
			// microcluster's creation time
			for (DataRecord point : cluster.getPoints()) {
				point.restoreAttValues();
				mc.absorb(point);
			}
			getQueueAgent().sendMessage(mc);
		}
		// send out any stragglers
		getQueueAgent().flush();

		LOG.debug("clusterDataRecords: exit");
	}

	/**
	 * Cluster a group of temporally related occurrences in the point space. The
	 * result is a set of micro-clusters that is sent on to the BTreeClusterer.
	 * 
	 * @param dataPoints
	 * @throws Exception
	 */
	private KmeansCluster[] runKmeans(List<DataRecord> dataPoints,
			int numClusters) throws Exception {

		if (dataPoints == null || dataPoints.size() == 0 || numClusters == 0) {
			throw new IllegalArgumentException();
		}

		// we should try and do something to prevent this!
		// if it is less than 4, but greater than 0, then perhaps we should
		// treat each point as a microcluster!!
		if (dataPoints.size() < 4) {
			throw new Exception(
					"runKmeans: entered with not enough DataRecords");
		}

		LOG.debug("runKmeans: number of DataRecords received = "
				+ dataPoints.size());

		LOG.debug("runKmeans: number of clusters = " + numClusters);

		// using kmeans++, seed kmeans
		KmeansCluster[] clusters = kmeansPlusPlus(dataPoints, numClusters);

		// begin the kmeans loop
		int numIterations = 0;
		long startTime = System.currentTimeMillis();
		for (; numIterations < maxIterations; numIterations++) {

			// [re]assign the clusters to their nearest neighbor
			assignClusters(clusters);

			// now assign all points to their nearest cluster centroid
			for (DataRecord point : dataPoints) {

				double minDist = Double.MAX_VALUE;
				// get the cluster that this point id currently assigned to (if
				// any)
				KmeansCluster minCluster = point.getKmeansKernel();

				// if the point is assigned to a cluster and the
				// distance from this point to the cluster is less
				// than 1/2 the distance from that cluster to its
				// nearest neighbor, then by the triangle inequality,
				// the point can be ignored; i.e., it can remain
				// assigned to its present cluster. this usually
				// works best when the number of clusters is greater
				// than 20
				if (minCluster != null) {
					minDist = getDistance(point.toDoubleArray(),
							minCluster.getLocation());
					if (minDist <= (0.5 * minCluster.getDistNN())) {
						continue;
					}
				}

				// find this point's nearest cluster centroid
				minDist = Double.MAX_VALUE;
				minCluster = null;
				for (KmeansCluster cluster : clusters) {
					double distance = getMinDistance(minDist,
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
			for (KmeansCluster cluster : clusters) {
				if (cluster.getPoints().size() > 0) {
					double driftDistance = getDistance(cluster.getMean(),
							cluster.getLocation());
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
		LOG.debug("runKmeans: number of iterations = " + numIterations);
		LOG.debug("runKmeans: number of ms per iteration = "
				+ ((endTime - startTime) / numIterations));

		return clusters;
	}

	/**
	 * Used for seeding the algorithm using a k-means++ deviant.
	 * 
	 * The idea behind k-means++ is to spread out the initial clusters as much
	 * as possible and thus avoid settling on a local optimum; outliers should
	 * also be avoided.
	 * 
	 * Note that this implementation deviates somewhat from the true kmeans++
	 * (see next method below); however, considerable empirical evidence has
	 * shown that this hybrid version provides superior results.
	 * 
	 * @param points
	 *            represents the points in the point space
	 * @param numClusters
	 *            requested number of clusters
	 * @return
	 */
	private KmeansCluster[] kmeansPlusPlus(List<DataRecord> points,
			int numClusters) {

		LOG.trace("kmeansPlusPlus: entered");
		LOG.trace("kmeansPlusPlus: number of points = " + points.size());
		LOG.trace("kmeansPlusPlus: number of clusters = " + numClusters);
		LOG.trace("kmeansPlusPlus: overlap factor = " + getOverlapFactor());

		// always use a different seed! if not, you always end up with the
		// same initial cluster pattern
		Random randomGen = new Random();

		// this will hold the set of clusters that will be returned to the
		// caller
		KmeansCluster[] clusters = new KmeansCluster[numClusters];

		// keeps track of number of clusters created
		int clusCount = 0;

		// randomly find our first cluster centroid from the given points
		int randomIndex = randomGen.nextInt(points.size());

		clusters[clusCount++] = new KmeansCluster(points.get(randomIndex));

		// main loop that creates the rest of the clusters
		for (; clusCount < numClusters; clusCount++) {

			// this will hold the sum of all the distances from each point to
			// its nearest cluster.
			double sumDistance = 0.0;

			// assign all points to their nearest cluster and record
			// their distances to that cluster
			for (DataRecord point : points) {

				// find this point's closest cluster
				double dxb = 0.0D;
				double minDistance = Double.MAX_VALUE;
				for (int i = 0; i < clusCount; i++) {
					dxb = getDistance(clusters[i].getLocation(),
							point.toDoubleArray());
					if (dxb < minDistance) {
						minDistance = dxb;
					}
				}
				sumDistance += minDistance;
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
				d2 = d2 * probability;
				if (d2 > maxProbability) {
					maxProbability = d2;
					maxPoint = point;
				}
			}
			clusters[clusCount] = new KmeansCluster(maxPoint);
		}

		return clusters;
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
		double[] mean = new double[points.get(0).getLocation().length];
		Arrays.fill(mean, 0.0F);
		for (DataRecord point : points) {
			sumArrays(mean, point.getLocation());
		}
		for (int i = 0; i < mean.length; i++) {
			mean[i] /= N;
		}

		// now find the std dev of each of the attributes
		double[] stdev = new double[mean.length];
		for (DataRecord point : points) {
			double[] location = point.getLocation();
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
			point.backupAttValues();
			double[] location = point.getLocation();
			for (int i = 0; i < location.length; i++) {
				location[i] = (location[i] - mean[i]) / (double) stdev[i];
			}
		}
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
	private KmeansCluster[] mergeClusters(KmeansCluster[] clusters) {

		if (clusters == null || clusters.length == 0) {
			return clusters;
		}

		int mergeCount = 0;
		boolean merged = false;
		double r1 = 0D;
		double r2 = 0D;
		for (KmeansCluster cluster : clusters) {
			cluster.setRadius(-1.0D);
		}

		do {
			merged = false;
			for (int i = 0; i < clusters.length; i++) {
				// continue if this cluster had been previously
				// absorbed by another cluster
				if (clusters[i] == null) {
					continue;
				}
				if ((r1 = clusters[i].getRadius()) < 0) {
					// scale the 1st radius with the given overlap factor
					r1 = getOverlapFactor() * getStdDev(clusters[i]);
					clusters[i].setRadius(r1);
				}
				if (r1 == 0.0) {
					continue;
				}
				for (int j = 0; j < clusters.length; j++) {

					// continue if this cluster had been previously
					// absorbed by another cluster
					if (i == j || clusters[j] == null) {
						continue;
					}
					if ((r2 = clusters[j].getRadius()) < 0) {
						// scale the 1st radius with the given overlap factor
						r2 = getOverlapFactor() * getStdDev(clusters[j]);
						clusters[j].setRadius(r2);
					}
					// if the sum of the two scaled radii is larger than the
					// distance between the two clusters, then the two
					// overlap and will be merged or one absorbs the other
					double dist = getDistance(clusters[i].getLocation(),
							clusters[j].getLocation());
					// merge the two clusters if there is enough of an overlap
					if (((r1 + r2) - dist) > 0) {
						// merge cluster j into cluster i, then update cluster
						// i's new tmp radius
						clusters[i].merge(clusters[j]);
						// update the new temp radius
						clusters[i].setRadius(getOverlapFactor()
								* getStdDev(clusters[i]));
						r1 = clusters[i].getRadius();
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
		KmeansCluster[] newClusters = new KmeansCluster[clusters.length
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

	private double getDistance(double[] a, double[] b) {
		return StatUtils.getDistance(getFastDistance(), a, b);
	}

	/**
	 * If fastDistance is enabled, then this method aborts prematurely and
	 * returns currentMin if, while calculating the distance between a and b, it
	 * detects that the distance between a and b has gotten larger than
	 * currentMin.
	 * 
	 * @param currentMin
	 * @param a
	 * @param b
	 * @return
	 */
	private double getMinDistance(double currentMin, double[] a, double[] b) {
		return StatUtils.getMinDistance(currentMin, getFastDistance(), a, b);
	}

	/**
	 * Get the standard deviation of all the cluster's points relative to the
	 * cluster's center of gravity.
	 * 
	 * @param cluster
	 * @return
	 */
	private double getStdDev(KmeansCluster cluster) {
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
	private double getVariance(KmeansCluster cluster) {
		double N = cluster.getPoints().size();
		if (N <= 1) {
			return 0.0;
		}
		double mean[] = cluster.getMean();
		double sum = 0.0D;
		for (DataRecord point : cluster.getPoints()) {
			double dist = getDistance(mean, point.toDoubleArray());
			sum += (dist * dist);
		}
		return sum / N;
	}

	/**
	 * Get the mean of the standard deviation across all clusters. Each
	 * cluster's standard deviation is placed in the stdvs array; this is done
	 * so that the calling method doesn't have to recalculate the stdvs.
	 * 
	 */
	public double getCost(KmeansCluster[] clusters, double[] stdvs) {

		if (clusters.length != stdvs.length) {
			return 0;
		}
		int N = clusters.length;
		double sum = 0.0;
		for (int i = 0; i < N; i++) {
			stdvs[i] = getStdDev(clusters[i]);
			sum += stdvs[i];
		}
		return (sum / (double) N);
	}

	/**
	 * This method is used to measure the variance or standard deviation of all
	 * the clusters' standard deviations. This is much more sensitive than
	 * taking the mean of the standard deviations.
	 * 
	 * The idea is that if the variance from one set of clusters to one with one
	 * less cluster increases, rather dramatically, then you have too few
	 * clusters for the one with one less cluster. For example, suppose you run
	 * kmeans against a point space and tell kmeans to generate 10 clusters.
	 * Then run kmeans again and have it generate 9 clusters against the same
	 * point space. If there is a very large variance across the two sets of
	 * clusters, then stick with the set having 10 clusters, else use the one
	 * with 9 clusters.
	 * 
	 * @param clusters
	 * @return
	 */
	public double getStdDevOfClusters(KmeansCluster[] clusters) {
		double[] stdvs = new double[clusters.length];
		double mean = getCost(clusters, stdvs);
		double variance = 0;
		double N = clusters.length;
		double diff = 0;
		for (int i = 0; i < N; i++) {
			diff = stdvs[i] - mean;
			variance += diff * diff;
		}
		return Math.sqrt(variance / N);
	}

	/**
	 * Return the average distance between the clusters' radii
	 * 
	 * @param clusters
	 * @param radiusSize
	 * @return
	 */
	public double getAvgRadiusDistance(KmeansCluster[] clusters) {
		double count = 0D;
		double sum = 0D;
		double r1 = 0D;
		double r2 = 0D;
		for (KmeansCluster cluster : clusters) {
			cluster.setRadius(-1.0D);
		}
		for (int i = 0; i < clusters.length; i++) {
			if ((r1 = clusters[i].getRadius()) < 0) {
				r1 = getStdDev(clusters[i]);
				clusters[i].setRadius(r1);
			}
			for (int j = i + 1; j < clusters.length; j++) {
				if ((r2 = clusters[j].getRadius()) < 0) {
					r2 = getStdDev(clusters[j]);
					clusters[j].setRadius(r2);
				}
				double dist = getDistance(clusters[i].getLocation(),
						clusters[j].getLocation());
				sum += (dist - (r1 + r2));
				++count;
			}
		}
		return sum / count;
	}

	public double getAvgVariance(KmeansCluster[] clusters) {
		double sum = 0.0D;
		for (KmeansCluster cluster : clusters) {
			sum += getVariance(cluster);
		}
		return sum / (double) clusters.length;
	}

	public static void sumArrays(double[] valsA, double[] valsB) {
		for (int i = 0; i < valsA.length; i++) {
			valsA[i] += valsB[i];
		}
	}

	public static void subArrays(double[] valsA, double[] valsB) {
		for (int i = 0; i < valsA.length; i++) {
			valsA[i] -= valsB[i];
		}
	}

	/**
	 * Assign each cluster to its nearest neighbor
	 * 
	 * @param clusters
	 */
	private void assignClusters(KmeansCluster[] clusters) {
		for (int i = 0; i < clusters.length; i++) {
			double minDist = Double.MAX_VALUE;
			KmeansCluster minCluster = null;
			for (int j = 0; j < clusters.length; j++) {
				if (i != j) {
					double distance = getDistance(clusters[i].getLocation(),
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
