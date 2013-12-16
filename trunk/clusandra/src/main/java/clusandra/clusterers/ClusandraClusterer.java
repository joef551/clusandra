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
 * $Date: 2011-11-14 21:31:23 -0500 (Mon, 14 Nov 2011) $
 * $Revision: 170 $
 * $Author: jose $
 * $Id: ClusandraClusterer.java 170 2011-11-15 02:31:23Z jose $
 */
package clusandra.clusterers;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import clusandra.cassandra.ClusandraDao;
import clusandra.core.DataRecord;
import clusandra.utils.DateUtils;
import clusandra.core.CluMessage;
import static clusandra.utils.StatUtils.*;
import clusandra.core.AbstractProcessor;

/**
 * This class implements the Clusandra clustering algorithm. This class is
 * supposed to be Spring-wired to a CassandraDao class.
 * 
 * @author jfernandez
 * 
 */
public class ClusandraClusterer extends AbstractProcessor {

	private static final Log LOG = LogFactory.getLog(ClusandraClusterer.class);

	// the set of microclusters that this instance of the Clusterer maintains.
	private List<ClusandraKernel> microClusters = new ArrayList<ClusandraKernel>();

	// this clusterer's CassandraDao
	private ClusandraDao clusandraDao = null;

	// keeps a running total of all the DataRecords processed by this instance
	private int dataRecordRunningCount = 0;

	// maximum radius for the clusters produced by this instance of the
	// clusterer
	private double maxRadius = 0.0;

	// the portion of the timeline to aggregate when aggregating
	private String startTime = null;
	private String endTime = null;

	private double overlapFactor = 0.0;

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// ~~~~~~~~~~~~~ This Algorithm's Configurable Properties ~~~~~~~~~~~~
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// the amount of time a microcluster is allowed to remain active without
	// absorbing a Datarecord
	private static final String clusterExpireTimeKey = "clusterExpireTime";
	private static final String clusterDistanceFactorKey = "clusterDistanceFactor";
	private static final String maxRadiusKey = "maxRadius";
	private static final String functionKey = "function";
	private static final String startTimeKey = "startTime";
	private static final String endTimeKey = "endTime";
	private static final String overlapFactorKey = "overlapFactor";

	private static final String AGGREGATE_STR = "aggregate";
	private static final String CLUSTER_STR = "cluster";

	// The clusterer's functions. By default, it performs clustering.
	private static enum ClustererFunction {
		AGGREGATE, CLUSTER;
		static ClustererFunction getFunction(String function)
				throws IllegalArgumentException {
			if (function == null || function.length() == 0) {
				throw new IllegalArgumentException();
			} else if (AGGREGATE_STR.equals(function)) {
				return AGGREGATE;
			} else if (CLUSTER_STR.equals(function)) {
				return CLUSTER;
			}
			throw new IllegalArgumentException();
		}
	}

	// Default is to cluster.
	private ClustererFunction myFunction = ClustererFunction.CLUSTER;

	/**
	 * The default expire time (milliseconds) for microclusters
	 */
	private long clusterExpireTime = 10000L;
	/**
	 * The default distance factor. NOTE: Currently not used.
	 */
	private double clusterDistanceFactor = 1.3;

	public ClusandraClusterer() {
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
			if (clusterExpireTimeKey.equals(key)) {
				setClusterExpireTime(map.get(key));
				LOG.trace("setConfig:clusterExpireTime = "
						+ getClusterExpireTime());
			} else if (clusterDistanceFactorKey.equals(key)) {
				setClusterDistanceFactor(map.get(key));
				LOG.trace("setConfig:clusterDistanceFactor = "
						+ getClusterDistanceFactor());
			} else if (maxRadiusKey.equals(key)) {
				setMaxRadius(map.get(key));
				LOG.trace("maxRadius = " + getMaxRadius());
			} else if (functionKey.equals(key)) {
				setFunction(map.get(key));
				LOG.trace("function = " + getFunction());
			} else if (startTimeKey.equals(key)) {
				setStartTime(map.get(key));
				LOG.trace("startTime = " + getStartTime());
			} else if (endTimeKey.equals(key)) {
				setEndTime(map.get(key));
				LOG.trace("endTime = " + getEndTime());
			} else if (overlapFactorKey.equals(key)) {
				setOverlapFactor(map.get(key));
				LOG.trace("overlapFactor = " + getOverlapFactor());
			}
		}
	}

	/**
	 * Called by setConfig() to set the cluster overlap factor.
	 * 
	 * @param clusterDistanceFactor
	 * @throws Exception
	 */
	public void setClusterDistanceFactor(String clusterDistanceFactor)
			throws Exception {
		this.setClusterDistanceFactor(Double.parseDouble(clusterDistanceFactor));
	}

	public void setClusterDistanceFactor(double clusterDistanceFactor)
			throws IllegalArgumentException {

		if (clusterDistanceFactor <= 1.0 || clusterDistanceFactor >= 2.0) {
			throw new IllegalArgumentException(
					"invalid clusterDistanceFactor specified; value must be > 1.0 and < 2.0");
		}
		this.clusterDistanceFactor = clusterDistanceFactor;
	}

	/**
	 * Called by setConfig() to set the cluster distance factor.
	 * 
	 * @param clusterDistanceFactor
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
	 * The maximum radius set for this cluster.
	 * 
	 * @return
	 */
	public double getMaxRadius() {
		return maxRadius;
	}

	/**
	 * Set the maximum radius for this cluster.
	 * 
	 * @param maxRadius
	 */
	public void setMaxRadius(double maxRadius) {
		this.maxRadius = maxRadius;
	}

	/**
	 * Called by setConfig() to set the max radius for the clusters produced by
	 * this clusterer.
	 * 
	 * @param maxRadius
	 */
	public void setMaxRadius(String maxRadius) throws Exception {
		this.maxRadius = Double.parseDouble(maxRadius);
	}

	/**
	 * Get the cluster distance factor for this clusterer.
	 * 
	 * @return
	 */
	public double getClusterDistanceFactor() {
		return clusterDistanceFactor;
	}

	/**
	 * Called by setConfig() to set the expire time for microclusters.
	 * 
	 * @param clusterExpireTime
	 * @throws Exception
	 */
	public void setClusterExpireTime(String clusterExpireTime) throws Exception {
		this.clusterExpireTime = Integer.parseInt(clusterExpireTime) * 1000;
	}

	/**
	 * Set the expire time for microclusters.
	 * 
	 * @param clusterExpireTime
	 * @throws Exception
	 */
	public void setClusterExpireTime(long clusterExpireTime) {
		this.clusterExpireTime = clusterExpireTime * 1000;
	}

	/**
	 * Return the cluster expire time.
	 * 
	 * @return
	 */
	public long getClusterExpireTime() {
		return clusterExpireTime;
	}

	/**
	 * Set the function to perform. Only 'cluster' and 'aggregate' are allowed.
	 * 
	 * @param function
	 * @throws IllegalArgumentException
	 */
	public void setFunction(String function) throws IllegalArgumentException {
		this.myFunction = ClustererFunction.getFunction(function);
	}

	/**
	 * Return the function being performed by this clusterer.
	 * 
	 * @return
	 */
	public ClustererFunction getFunction() {
		return myFunction;
	}

	/**
	 * Called by Spring to wire this clusterer to its ClusandraDao object
	 */
	public void setClusandraDao(ClusandraDao clusandraDao) {
		this.clusandraDao = clusandraDao;
	}

	/**
	 * Get this clusterer's ClusandraDao.
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
		if (getClusandraDao() == null) {
			throw new Exception(
					"this clusterer has not been wired to a CassandraDao");
		}
		if (getFunction() == ClustererFunction.AGGREGATE) {
			runAggregator();
			return false;
		}
		return true;
	}

	/**
	 * Called by the QueueAgent to give the Clusterer a collection of
	 * DataRecords to process. The collection is read from the QueueAgent's JMS
	 * queue.
	 * 
	 * Design note: This method can be called by the QueueAgent within the
	 * context of a local JMS transaction. So not until this method returns,
	 * without throwing an Exception, would the QueueAgent commit the
	 * transaction. The transaction can be rolled back if an exception is
	 * thrown; thus preserving the DataRecords at the JMS queue.
	 * 
	 * @param dataRecords
	 * @throws Exception
	 */
	public void processDataRecords(List<DataRecord> dataRecords)
			throws Exception {

		// do a little validation
		if (dataRecords == null || dataRecords.isEmpty()) {
			LOG.warn("processDataRecords: entered with empty DataRecord list");
			return;
		} else if (getClusandraDao() == null) {
			throw new Exception(
					"processDataRecords: this clusterer has not been wired to a CassandraDao");
		}

		// if not already set via Spring, sample the data records to get the max
		// radius
		if (getMaxRadius() == 0.0) {
			double radius = getAverageDistance(dataRecords);
			if (radius == 0.0) {
				throw new Exception(
						"processDataRecords: maxRadius was not set and could not acquire it");
			}
			setMaxRadius(radius);
			LOG.info("processDataRecords: max radius = " + getMaxRadius());
		}

		LOG.debug("processDataRecords: number of DataRecords received = "
				+ dataRecords.size());

		// Find the time horizon of the collection of data records that have
		// been passed in. The time horizon is supposed to be a temporal
		// *forward sliding* window of the data stream. This is extremely
		// important. This sliding window cannot slide back!
		double minTime = Double.MAX_VALUE;
		double maxTime = 0.0;
		for (DataRecord dRecord : dataRecords) {
			minTime = Math.min(minTime, dRecord.getTimestamp());
			maxTime = Math.max(maxTime, dRecord.getTimestamp());
		}

		// this should not happen.
		if (minTime <= 0 || minTime > maxTime) {
			throw new Exception("processDataRecords: invalid min time = "
					+ minTime);
		}

		// extend the minimum back by the expire time. we want to include
		// microclusters that have not expired and are thus still active
		minTime -= getClusterExpireTime();

		LOG.trace("processDataRecords: time horizon = " + minTime + " - "
				+ maxTime);

		dataRecordRunningCount += dataRecords.size();

		LOG.info("processDataRecords: running total of DataRecords received = "
				+ dataRecordRunningCount);

		// remove all microclusters, from the current working set, that do not
		// fall within this new sliding window. any microclusters that are
		// removed here, will have been previously saved out to Cassandra.
		// removed microclusters are considered inactive and no longer capable
		// of absorbing DataRecords.
		boolean startOver;
		do {
			startOver = false;
			for (int i = 0; i < getMicroClusters().size() && !startOver; i++) {
				if (getMicroClusters().get(i).getLAT() < minTime) {
					getMicroClusters().remove(i);
					startOver = true;
				}
			}
		} while (startOver && getMicroClusters().size() > 1);

		// Now we should have an ACTIVE working set of microclusters for all the
		// DataRecords in the collection. That is, the set is ACTIVE relative
		// to the sliding window. Now we need to find an active subset, within
		// the active set, for each DataRecord and use that subset to find
		// microcluster candidates to absorb the DataRecord
		List<ClusandraKernel> subset = new ArrayList<ClusandraKernel>();
		for (DataRecord dRecord : dataRecords) {
			subset.clear();
			for (ClusandraKernel cluster : getMicroClusters()) {
				if ((dRecord.getTimestamp() >= cluster.getCT() && dRecord
						.getTimestamp() <= cluster.getLAT())
						|| (cluster.getLAT() < dRecord.getTimestamp() && cluster
								.getLAT() >= (dRecord.getTimestamp() - getClusterExpireTime()))) {
					subset.add(cluster);
				}
			}
			// note: this method will update the working micro-cluster set
			absorbDataRecordFromList(dRecord, subset);
		}

		LOG.trace("processDataRecords: Number of clusters in working set = "
				+ getMicroClusters().size());

		// use this clusterer's CassandraDao to write the working
		// set of microcluster out to Cassandra. The Dao will write out only the
		// ones that have been touched.
		double drClusterCount = 0;
		for (ClusandraKernel cluster : getMicroClusters()) {
			drClusterCount += cluster.getN();
			getClusandraDao().writeClusandraKernel(cluster);
		}
		LOG.trace("processDataRecords: data records in working set = "
				+ drClusterCount);

		// we're done, return control to the QueueAgent and await the next
		// collection of DataRecords to process
	}

	/**
	 * Return the working set of microclusters for this clusterer
	 * 
	 * @return
	 */
	public List<ClusandraKernel> getMicroClusters() {
		return microClusters;
	}

	public void processCluMessages(List<CluMessage> cluMessages)
			throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
	 * Set the end time for this aggregator.
	 * 
	 * @param startTime
	 */
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	/**
	 * Return the start time for this aggregator
	 * 
	 * @return
	 */
	public String getStartTime() {
		return startTime;
	}

	/**
	 * Set the end time for this aggregator
	 * 
	 * @param endTime
	 */
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	/**
	 * Return's the end time for this aggregator
	 * 
	 * @return
	 */
	public String getEndTime() {
		return endTime;
	}

	// Method is used to look for a cluster in the provided list of
	// clusters that can absorb the provided DataRecord. the absorption will
	// take place in this method
	private void absorbDataRecordFromList(DataRecord dataRecord,
			List<ClusandraKernel> microClusters) {

		if (microClusters == null || microClusters.isEmpty()) {
			// there are probably no clusters in the subset.
			// create a microcluster for this DataRecord and place in the
			// working set
			LOG.trace("absorbDataRecordFromList: got empty list, creating new microcluster");
			getMicroClusters().add(
					new ClusandraKernel(dataRecord, getMaxRadius()));
			return;
		}
		LOG.trace("absorbDataRecordFromList: number of clusters in list = "
				+ microClusters.size());

		double closestDistance = Double.MAX_VALUE;
		int closestIndex = 0;
		double[] dRecordPayload = dataRecord.toDoubleArray();

		// find the microcluster in the provided list that
		// is the closest to the DataRecord
		for (int i = 0; i < microClusters.size(); i++) {
			double distance = microClusters.get(i).getDistance(dRecordPayload);
			if (distance < closestDistance) {
				closestDistance = distance;
				closestIndex = i;
			}
		}
		// LOG.trace("absorbDataRecordFromList: closest distance = "
		// + closestDistance);

		// see if the closest cluster can absorb the data record
		if (absorbDataRecord(dataRecord, microClusters.get(closestIndex))) {
			return;
		}
		// if (closestDistance <= getMaxRadius()
		// && absorbDataRecord(dataRecord, microClusters.get(closestIndex))) {
		// return;
		// }

		LOG.trace("absorbDataRecord: creating microcluster, could not find cluster from list of closest");
		getMicroClusters().add(new ClusandraKernel(dataRecord, getMaxRadius()));
	}

	private boolean absorbDataRecord(DataRecord dataRecord,
			ClusandraKernel cluster) {

		if (dataRecord == null || cluster == null) {
			LOG.warn("absorbDataRecordX: dataRecord and/or cluster == null");
			return false;
		}

		ClusandraKernel tmpCluster = new ClusandraKernel(dataRecord);
		tmpCluster.merge(cluster);

		if (tmpCluster.getRadius() > getMaxRadius()) {
			return false;
		}
		cluster.absorb(dataRecord);
		return true;
	}

	/**
	 * This method is invoked to aggregate clusters in the data store.
	 */
	public void runAggregator() throws Exception {

		LOG.info("ClusandraAggregator has started");

		if (getClusandraDao() == null) {
			LOG.error("ERROR: ClusandraDao has not been set");
			throw new Exception("ClusandraDao has not been set");
		} else if (getStartTime() == null || getEndTime() == null) {
			LOG.error("ERROR: get and/or start times has not been set");
			throw new Exception(
					"ERROR: get and/or start times has not been set");
		}

		// get the time horizon over which to sweep the data stream timeline
		long fromMills = DateUtils.validateDate(getStartTime());
		long toMills = DateUtils.validateDate(getEndTime());

		// validate the time horizon
		if (fromMills > toMills) {
			throw new IllegalArgumentException(
					"from date is greater than to (end) date");
		}

		// extend the minimum back by the expire time and extend the maximum by
		// the expire time. we want to include all microclusters that have not
		// expired and are thus still active relative to the specified time
		// horizon
		long startMills = fromMills - getClusterExpireTime();
		long endMills = toMills + getClusterExpireTime();

		if (LOG.isTraceEnabled()) {
			Date fDate = new Date(fromMills);
			Date tDate = new Date(toMills);
			LOG.trace("the from and to dates = [" + fDate.toString() + "]["
					+ tDate.toString() + "]");
			fDate = new Date(startMills);
			tDate = new Date(endMills);
			LOG.trace("start and to dates = [" + fDate.toString() + "]["
					+ tDate.toString() + "]");
		}

		// the calendars that are used to walk the timeline
		Calendar startCal = Calendar.getInstance();
		startCal.setTimeInMillis(startMills);
		Calendar endCal = Calendar.getInstance();
		endCal.setTimeInMillis(endMills);

		// the row keys used to index the cluster index table
		long startRowKey = ClusandraDao.getIndexRowKey(startCal);
		long endRowKey = ClusandraDao.getIndexRowKey(endCal);

		LOG.debug("Start and end row keys = " + startRowKey + " " + endRowKey);

		// advance to the first valid row in the given time horizon. that is,
		// advance to the first row that has entries. advancement is done from
		// the start of the specified time horizon
		boolean timeHorizonShortened = false;
		while (startRowKey < endRowKey
				&& getClusandraDao().getIndexColumnCount(startRowKey) == 0) {
			LOG.debug("Skipping this non-existent startRowKey = " + startRowKey);
			startCal.add(Calendar.DATE, 1);
			startRowKey = ClusandraDao.getIndexRowKey(startCal);
			timeHorizonShortened = true;
		}
		// if row advanced, then start from the very beginning of the row
		if (timeHorizonShortened) {
			startCal.setTimeInMillis(startRowKey);
			timeHorizonShortened = false;
		}

		// now work back, if required, to the last existing row in the index. we
		// start from the end of the specified time horizon
		while (endRowKey > startRowKey
				&& getClusandraDao().getIndexColumnCount(endRowKey) == 0) {
			LOG.debug("Skipping this non-existent endRowKey = " + endRowKey);
			endCal.add(Calendar.DATE, -1);
			endRowKey = ClusandraDao.getIndexRowKey(endCal);
			timeHorizonShortened = true;
		}
		// if row brought in, then end at the very end of the row
		if (timeHorizonShortened) {
			endCal.setTimeInMillis(endRowKey);
			endCal.add(Calendar.SECOND, (int) ClusandraDao.FINAL_SECOND_IN_DAY);
		}

		LOG.debug("Final start and end row keys = " + startRowKey + " "
				+ endRowKey);
		LOG.debug("Final start and end millis for cals = "
				+ startCal.getTimeInMillis() + " " + endCal.getTimeInMillis());

		LOG.info("Please wait...");

		// start walking the time line
		while (startCal.getTimeInMillis() < endCal.getTimeInMillis()) {

			// Get all the microclusters that are active relative to one another
			// from this particular second in the time line. We're essentially
			// walking the cluster index table, which represents the time line.
			List<ClusandraKernel> clusters = getClusandraDao().getClusters(
					startCal.getTimeInMillis(),
					startCal.getTimeInMillis() + getClusterExpireTime());

			if (clusters != null && clusters.size() > 1) {

				// store the clusters that will be removed in this list
				ArrayList<ClusandraKernel> clustersToRemove = new ArrayList<ClusandraKernel>();

				LOG.info("Retrieved " + clusters.size() + " clusters at "
						+ (new Date(startCal.getTimeInMillis()).toString()));

				// Filter out any superclusters
				boolean startOver;
				do {
					startOver = false;
					for (int i = 0; i < clusters.size() && !startOver; i++) {
						if (clusters.get(i).isSuper()) {
							clusters.remove(i);
							startOver = true;
						}
					}
				} while (startOver);

				LOG.info("Number of micro-clusters retrieved = "
						+ clusters.size());

				// sort the clusters by N in ascending order
				for (ClusandraKernel cluster : clusters) {
					cluster.setSortField(ClusandraKernel.SortField.N);
				}

				// merge those overlapping microclusters that meet the overlap
				// factor/criteria
				do {
					startOver = false;
					for (int i = 0; i < clusters.size() && !startOver; i++) {
						ClusandraKernel sourceCluster = clusters.get(i);
						double sourceRadius = sourceCluster.getRadius();
						for (int j = i + 1; j < clusters.size() && !startOver; j++) {
							ClusandraKernel targetCluster = clusters.get(j);
							double targetRadius = targetCluster.getRadius();
							double sumRadius = targetRadius + sourceRadius;
							double distance = getDistance(
									sourceCluster.getCenter(),
									targetCluster.getCenter());
							// if the two clusters overlap, then merge them
							if (sumRadius > distance) {
								// if the overlap factor is set, then use it as
								// further criteria for merging
								if (getOverlapFactor() > 0.0) {
									double overlap = (sumRadius - distance)
											/ distance;
									LOG.trace("overlap [" + overlap
											+ "] overlap factor ["
											+ getOverlapFactor() + "]");
									// if the overlap is less than the overlap
									// factor then skip the merging
									if (overlap > getOverlapFactor()) {
										LOG.trace("merging source and target radii = "
												+ sourceRadius
												+ " "
												+ targetRadius);
										LOG.trace("distance = " + distance);
										startOver = true;
										clusters.remove(j);
										clusters.remove(i);
										if (sourceCluster.getN() >= targetCluster
												.getN()) {
											sourceCluster.merge(targetCluster);
											// put it at the end
											clusters.add(sourceCluster);
											clustersToRemove.add(targetCluster);
										} else {
											targetCluster.merge(sourceCluster);
											clusters.add(targetCluster);
											clustersToRemove.add(sourceCluster);
										}
									}
								}

							}
						}
					}
				} while (startOver);
				LOG.info("Number of micro-clusters after merge = "
						+ clusters.size());
				LOG.info("Number of micro-clusters to be removed = "
						+ clustersToRemove.size());
				// removed clusters that were merged (if any)
				getClusandraDao().delClusters(clustersToRemove);
				// and write back out the clusters that remain
				getClusandraDao().writeClusandraKernels(clusters);
			} // if (clusters != null && clusters.size() > 0)

			// skip to the next second in the time line
			startCal.add(Calendar.SECOND, 1);
		}
		LOG.info("done");
	}

	/**
	 * This method is used to acquire the average distance between all the
	 * DataRecords in the list. The average distance is a measure of the density
	 * of the DataRecords.
	 * 
	 * @param payloads
	 * @return
	 */
	public static double getAverageDistance(List<DataRecord> records) {
		double distanceTotal = 0.0;
		double distance = 0.0;
		double distanceCnt = 0.0;
		double[] v1 = null;
		double[] v2 = null;
		int payLoadCnt = records.size();
		for (int i = 0; i < payLoadCnt; i++) {
			v1 = records.get(i).toDoubleArray();
			for (int j = i + 1; j < payLoadCnt; j++) {
				v2 = records.get(j).toDoubleArray();
				if (v1.length != v2.length) {
					return 0.0;
				}
				distance = 0.0;
				for (int k = 0; k < v1.length; k++) {
					distance += Math.pow((v1[k] - v2[k]), 2);
				}
				distanceTotal += Math.sqrt(distance);
				distanceCnt++;
			}
		}
		return distanceTotal / distanceCnt;
	}

}
