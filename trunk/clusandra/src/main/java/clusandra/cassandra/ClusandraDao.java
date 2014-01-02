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
 * $Date: 2011-11-14 13:53:50 -0500 (Mon, 14 Nov 2011) $
 * $Revision: 166 $
 * $Author: jose $
 * $Id: ClusandraDao.java 166 2011-11-14 18:53:50Z jose $
 */
package clusandra.cassandra;

import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.exceptions.PelopsException;
import org.scale7.cassandra.pelops.exceptions.NotFoundException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.RowDeletor;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.KeyRange;
import org.springframework.beans.factory.InitializingBean;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Calendar;
import java.util.Map;
import java.util.HashMap;
import clusandra.utils.DateUtils;
import clusandra.clusterers.MicroCluster;
import static clusandra.clusterers.MicroCluster.getClusandraKernel;

/**
 * This is a Clusandra-specific, Cassandra Data Access Object (DAO). It is not
 * meant to be a general purpose Cassandra DAO. It is used by the Clusandra
 * clusterer (clustering algorithm) to read and write clusters to and from the
 * Cassandra data store. It is also used by CQL to read clusters form the data
 * store.
 * 
 * @author jfernandez
 * 
 */
public class ClusandraDao extends CassandraDao implements InitializingBean {

	private static final Log LOG = LogFactory.getLog(ClusandraDao.class);
	// This is the default name of the ColumnFamily for the cluster index table
	private String clusterIndexTable = "clusterIndexTable";
	// This is the default name of the ColumnFamily for the cluster table
	private String clusterTable = "clusterTable";
	// This is the default name for the ColumnFamily that records the ID for all
	// clusters. This ColumnFamily is used primarily as a convenience mechanism
	// for the CQL.
	private String clusterRecorderTable = "clusterRecorderTable";
	// the row key for the clusterRecorderTable; there is only one row in this
	// table.
	private static final String recorderTableRowKey = "recorderTableRowKey";
	// used for indexing to the final slot in a column index table
	public static final long FINAL_SECOND_IN_DAY = 86400L - 1L;

	public ClusandraDao() {
		super();
	}

	/**
	 * Set the name of the timeline cluster index table, which is a
	 * SuperColumnFamily.
	 * 
	 * @param clusterIndexTable
	 */
	public void setClusterIndexTable(String clusterIndexTable) {
		this.clusterIndexTable = clusterIndexTable;
	}

	/**
	 * Get the name of the timeline cluster index table.
	 */
	public String getClusterIndexTable() {
		return clusterIndexTable;
	}

	/**
	 * Set the name of the cluster table, which is a SuperColumnFamily. This is
	 * the table that holds all the micro and super clusters.
	 * 
	 * @param clusterIndexTable
	 */
	public void setClusterTable(String clusterIndexTable) {
		this.clusterTable = clusterIndexTable;
	}

	/**
	 * Get the name of the cluster table.
	 */
	public String getClusterTable() {
		return clusterTable;
	}

	/**
	 * Set the name of the cluster recorder table, which is a ColumnFamily. This
	 * is the table that holds the IDs for all the micro and super clusters. It
	 * is primarily a convenience structure for the CQL.
	 * 
	 * @param clusterIndexTable
	 */
	public void setclusterRecorderTable(String clusterRecorderTable) {
		this.clusterRecorderTable = clusterRecorderTable;
	}

	/**
	 * Get the name of the cluster recorder table.
	 */
	public String getClusterRecorderTable() {
		return clusterRecorderTable;
	}

	/**
	 * Return a list of Clusandra micro or super clusters that were born on the
	 * specified date.
	 * 
	 * @param cal
	 * @return
	 * @throws Exception
	 */
	public List<MicroCluster> getClustersBornOn(Calendar cal)
			throws Exception {
		// this list will house the clusters to return
		List<MicroCluster> clusters2Ret = new ArrayList<MicroCluster>();
		if (cal != null) {
			// find the name of the target supercolumn in the index table
			long superCol = getSuperColumnIndexName(cal);
			// now that we have the index supercolumn, get a list of clusters
			// that are or were active on this given date
			List<MicroCluster> clusters = getClusters(cal, cal);
			if (clusters != null && !clusters.isEmpty()) {
				// Return only those clusters whose CT (creation time) is equal
				// to the given date
				Calendar cal1 = Calendar.getInstance();
				for (MicroCluster cluster : clusters) {
					cal1.setTimeInMillis((long) cluster.getCT());
					// if the CT matches the given date, then the cluster was
					// born on this date.
					if (superCol == getSuperColumnIndexName(cal1)) {
						clusters2Ret.add(cluster);
					}
				}
			}
		}
		return clusters2Ret;
	}

	/**
	 * Delete the cluster with the specified id from the data store.
	 * 
	 * @param clusterID
	 * @return The deleted cluster or null if cluster could not be found
	 * @throws Exception
	 */
	public void delCluster(String clusterID) throws Exception {

		LOG.trace("delCluster: entered with this id [" + clusterID + "]");

		if (clusterID == null || clusterID.length() == 0) {
			return;
		}
		RowDeletor deletor = new RowDeletor(getThriftPool());
		// remove the cluster from the cluster table, then the
		// recorder table
		try {
			deletor.deleteRow(getClusterTable(), clusterID,
					getConsistencyLevel());
		} catch (PelopsException exc) {
			LOG.error("delCluster got this exception [" + exc.getMessage()
					+ "] when deleting this cluster [" + clusterID
					+ "] from the cluster table");
		}
		try {
			getMutator().deleteColumn(getClusterRecorderTable(),
					recorderTableRowKey, clusterID);
		} catch (PelopsException exc) {
			LOG.error("delCluster got this exception [" + exc.getMessage()
					+ "] when deleting this cluster [" + clusterID
					+ "] from the " + "recorder table");
		}
	}
	
	/**
	 * Delete a cluster from the cluster data store
	 * @param cluster
	 * @throws Exception
	 */
	public void delCluster(MicroCluster cluster) throws Exception{
		if(cluster == null){
			return;
		}
		delCluster(cluster.getID());
	}

	/**
	 * Delete all the clusters in the list.
	 * 
	 * @param clusterID
	 * @return The deleted cluster or null if cluster could not be found
	 * @throws Exception
	 */
	public void delClusters(ArrayList<MicroCluster> clusters)
			throws Exception {
		if (clusters != null && clusters.size() > 0) {
			for (MicroCluster cluster : clusters) {
				delCluster(cluster.getID());
			}
		}
	}

	/**
	 * Returns all the clusters in the cluster table.
	 * 
	 * TODO: Pagination!
	 * 
	 * @return
	 */
	public List<MicroCluster> getClusters() {
		List<Column> clusterKeys;
		List<MicroCluster> clusters = new ArrayList<MicroCluster>();
		try {
			clusterKeys = getSelector().getColumnsFromRow(
					getClusterRecorderTable(), recorderTableRowKey, false,
					getConsistencyLevel());
			if (clusterKeys != null && !clusterKeys.isEmpty()) {
				for (Column column : clusterKeys) {
					MicroCluster cluster = getCluster(new String(
							column.getName()));
					if (cluster != null) {
						clusters.add(cluster);
					}
				}
			}
		} catch (PelopsException e) {
			LOG.error("ERROR:getClusters got this PelopsException ["
					+ e.getMessage()
					+ "] when calling getSuperColumnsFromRow on single row");
		} catch (Exception e) {
			LOG.error("ERROR:getClusters got this Exception [" + e.getMessage()
					+ "] when calling getSuperColumnsFromRow on single row");
		}
		return clusters;
	}

	/**
	 * Return the cluster identified by the given ID.
	 * 
	 * @param clusterID
	 * @return MicroCluster
	 * @throws Exception
	 */
	public MicroCluster getCluster(String clusterID) throws Exception {

		LOG.trace("getCluster: entered with this id [" + clusterID + "]");

		if (clusterID == null || clusterID.length() == 0) {
			return null;
		}
		List<SuperColumn> clusterSuperColumns = null;
		MicroCluster clusandraKernel = null;
		try {
			// Get the supercolumn that represents the cluster.
			clusterSuperColumns = getSelector().getSuperColumnsFromRow(
					getClusterTable(), clusterID, false, getConsistencyLevel());
			if (clusterSuperColumns == null || clusterSuperColumns.isEmpty()) {
				LOG.trace("getCluster: cluster not found");
			} else {
				clusandraKernel = getClusandraKernel(clusterSuperColumns,
						clusterID);
			}
		} catch (Exception e) {
			LOG.error("getCluster: ERROR, encounterd this exception while trying to retrieve cluster = "
					+ e.getMessage());
			e.printStackTrace(System.out);
		}
		return clusandraKernel;
	}
	
	/**
	 * Return the cluster with the given clusterID. 
	 * @param clusterID
	 * @return MicroCluster
	 * @throws Exception
	 */
	public MicroCluster getCluster(byte[] clusterID) throws Exception {
		if(clusterID == null || clusterID.length == 0){
			return null;
		}
		return getCluster(new String(clusterID));
	}

	/**
	 * Return a list of clusters whose LAT falls within (inclusive) the
	 * specified date (i.e., seconds of the date). The date String value must be
	 * formatted as follows:<br>
	 * 
	 * year:month:day-of-month:hour-of-day:minute-of-hour:second-of-minute <br>
	 * For example: <br>
	 * 2011:07:05:13:05:55 <br>
	 * 
	 * All fields are required and hours must adhere to the 24-hour clock
	 * format!
	 * 
	 * 
	 * @param fromCal
	 * @param toCal
	 * @return
	 * @throws Exception
	 */
	public List<MicroCluster> getClusters(String fromDate) throws Exception {
		LOG.trace("getClusters: entered with this string [" + fromDate + "]");

		long fromMills = DateUtils.validateDate(fromDate);
		if (LOG.isTraceEnabled()) {
			Date fDate = new Date(fromMills);
			LOG.trace("getClusters: final Calendar date = [" + fDate.toString()
					+ "]");
		}
		return getClusters(fromMills, fromMills);
	}

	/**
	 * Return a list of clusters whose LAT falls within (inclusive) the
	 * specified from-to time horizon. The fromDate and toDate String values
	 * must be formatted as follows:<br>
	 * 
	 * year:month:day-of-month:hour-of-day:minute-of-hour:second-of-minute <br>
	 * For example: <br>
	 * 2011:07:05:13:05:55 <br>
	 * 
	 * All fields are required and hours must adhere to the 24-hour clock
	 * format!
	 * 
	 * 
	 * @param fromCal
	 * @param toCal
	 * @return
	 * @throws Exception
	 */
	public List<MicroCluster> getClusters(String fromDate, String toDate)
			throws Exception {

		LOG.trace("getClusters: entered with these strings [" + fromDate + "]["
				+ toDate + "]");

		long fromMills = DateUtils.validateDate(fromDate);
		long toMills = DateUtils.validateDate(toDate);

		if (LOG.isTraceEnabled()) {
			Date fDate = new Date(fromMills);
			Date tDate = new Date(toMills);
			LOG.trace("getClusters: final from and to (end) Calendar dates = ["
					+ fDate.toString() + "][" + tDate.toString() + "]");
		}
		if (fromMills > toMills) {
			throw new IllegalArgumentException(
					"from date is greater than to (end) date");
		}
		return getClusters(fromMills, toMills);
	}

	/**
	 * Return a list of clusters whose LAT falls within the specified calendar
	 * date, down to the second.
	 * 
	 * @param cal
	 * @return
	 * @throws Exception
	 */
	public List<MicroCluster> getClusters(Calendar cal) throws Exception {
		return getClusters(cal, cal);
	}

	/**
	 * Return a list of clusters whose LAT falls within (inclusive) the
	 * specified from-to time horizon.
	 * 
	 * @param fromCal
	 * @param toCal
	 * @return
	 * @throws Exception
	 */
	public List<MicroCluster> getClusters(Calendar fromCal, Calendar toCal)
			throws Exception {

		// validate the specified time values
		if ((fromCal == null || toCal == null)) {
			LOG.warn("getClusters: entered with illegal arguments [" + fromCal
					+ "] [" + toCal + "]");
			throw new IllegalArgumentException(
					"getClusters: entered with illegal arguments [" + fromCal
							+ "] [" + toCal + "]");
		}
		return getClusters(fromCal.getTimeInMillis(), toCal.getTimeInMillis());
	}

	/**
	 * Return a list of clusters whose LAT falls within (inclusive) the
	 * specified from-to time horizon.
	 * 
	 * @param from
	 *            the start of the time horizon
	 * @param to
	 *            the end of the time horizon
	 * @return
	 */
	public List<MicroCluster> getClusters(long fromMills, long toMills)
			throws Exception {

		LOG.trace("getClusters: entered with fromMills and toMills = ["
				+ fromMills + "] [" + toMills + "]");

		// validate the specified time values
		if ((fromMills <= 0L || toMills <= 0L) || (toMills < fromMills)) {
			LOG.warn("getClusters: entered with illegal arguments ["
					+ fromMills + "] [" + toMills + "]");
			throw new IllegalArgumentException(
					"getClusters: entered with illegal arguments [" + fromMills
							+ "] [" + toMills + "]");
		}

		// Create an empty array list for the clusters that will be fetched from
		// Cassandra
		ArrayList<MicroCluster> clusters = new ArrayList<MicroCluster>();

		// dayMills is used to hold the mills for the day's zero hour. it is
		// used as the row key into the cluster index table (CIT)
		long dayMills = 0L;
		// startMills and endMills are used primarily for creating slice
		// predicates. in other words, the range of supercolumns to
		// retrieve within a row in the CIT
		long startMills = 0L;
		long endMills = 0L;

		// calendar used for calculating millis
		Calendar cal = Calendar.getInstance();

		// calculate the start and end 'days' of the time horizon to determine
		// if we're working within one day or across multiple days
		cal.setTimeInMillis(fromMills);
		startMills = getIndexRowKey(cal);

		cal.setTimeInMillis(toMills);
		endMills = getIndexRowKey(cal);

		// does the time horizon fall on the same day or across days?
		if (startMills == endMills) {
			// same day
			LOG.trace("getClusters: working within same day");

			// calculate the start of day (row key) and time horizon for the
			// slice predicate
			cal.setTimeInMillis(fromMills);
			// dayMills contains day's zero hour (start of day), which is also
			// used as the row key for cluster index table
			dayMills = getIndexRowKey(cal);
			// getElapsedSeconds returns the number of seconds that have elapsed
			// since start of day relative to the specified calendar
			startMills = getSuperColumnIndexName(cal);
			cal.setTimeInMillis(toMills);
			endMills = getSuperColumnIndexName(cal);

			LOG.trace("dayMills, startMills, endMills = [" + dayMills + "]"
					+ "[" + startMills + "][" + endMills + "]");

			// get all supercolumns from the CIT within this day (row key)
			List<SuperColumn> superCols = getSuperColumnsFromIndex(dayMills,
					getSlicePredicate(startMills, endMills));

			// get clusters
			if (!superCols.isEmpty()) {
				getClusandraKernels(superCols, clusters);
			}

		} else {

			LOG.trace("getClusters: working across days/rows");

			// the specified time horizon spans multiple days. hopefully, just
			// one day :)

			// set a from and to calendar to control the loop
			Calendar calFrom = Calendar.getInstance();
			calFrom.setTimeInMillis(fromMills);
			Calendar calTo = Calendar.getInstance();
			calTo.setTimeInMillis(toMills);

			dayMills = getIndexRowKey(calFrom);
			endMills = dayMills + FINAL_SECOND_IN_DAY;
			startMills = getSuperColumnIndexName(calFrom);

			LOG.trace("dayMills, startMills, endMills = [" + dayMills + "]"
					+ "[" + startMills + "][" + endMills + "]");

			// grab all clusters in this first day from the start point
			// going all the way to the last supercolumn
			List<SuperColumn> superCols = getSuperColumnsFromIndex(dayMills,
					getSlicePredicate(startMills, endMills));

			if (!superCols.isEmpty()) {
				getClusandraKernels(superCols, clusters);
			}

			// go to the next day
			calFrom.add(Calendar.DAY_OF_YEAR, 1);
			while (!DateUtils.isSameDay(calFrom, calTo)) {

				// grab all clusters indexed by this CIT row (day).
				dayMills = getIndexRowKey(calFrom);
				startMills = dayMills;
				endMills = dayMills + FINAL_SECOND_IN_DAY;

				LOG.trace("dayMills, startMills, endMills = [" + dayMills + "]"
						+ "[" + startMills + "][" + endMills + "]");

				superCols = getSuperColumnsFromIndex(dayMills,
						getSlicePredicate(startMills, endMills));

				if (!superCols.isEmpty()) {
					getClusandraKernels(superCols, clusters);
				}

				// go to the next day
				calFrom.add(Calendar.DAY_OF_YEAR, 1);
			}

			// grab all clusters in this final day starting at the first
			// supercolumn until the 'fromSecond' supercolumn
			dayMills = getIndexRowKey(calTo);
			startMills = dayMills;
			endMills = getSuperColumnIndexName(calTo);

			LOG.trace("dayMills, startMills, endMills = [" + dayMills + "]"
					+ "[" + startMills + "][" + endMills + "]");

			superCols = getSuperColumnsFromIndex(dayMills,
					getSlicePredicate(startMills, endMills));

			if (!superCols.isEmpty()) {
				getClusandraKernels(superCols, clusters);
			}

		}
		return clusters;
	}

	/**
	 * Write out a list of clusters to the data store.
	 * 
	 * @param cluKernels
	 */
	public void writeClusandraKernels(List<MicroCluster> cluKernels)
			throws Exception {
		if (cluKernels != null && !cluKernels.isEmpty()) {
			for (MicroCluster cluKernel : cluKernels) {
				writeClusandraKernel(cluKernel);
			}
		}
	}

	/**
	 * Return the number of supercolumns from the specified row in the index
	 * 
	 * @param rowKey
	 * @return
	 */
	public int getIndexColumnCount(long rowKey) {
		int count = 0;
		try {
			count = getSelector().getColumnCount(getClusterIndexTable(),
					Long.toString(rowKey), getConsistencyLevel());
		} catch (Exception exc) {
			LOG.error("getIndexColumnCount: getColumnCount returns this exception - "
					+ exc.getMessage());
		}
		return count;
	}

	/**
	 * Write the cluster (MicroCluster) out to the data store. The cluster's
	 * LAT parameter is used to nail the microcluster to the cluster index
	 * table.
	 * 
	 * This must be the only method to write out a cluster to the data store!
	 * 
	 * @param cluKernels
	 */
	public void writeClusandraKernel(MicroCluster cluKernel)
			throws Exception {

		// no need saving this one if it has already been saved.
		if (cluKernel.isSaved()) {
			return;
		}

		Mutator mutator = getMutator();

		// Get the rowKey for this cluster
		String rowKey = cluKernel.getID();

		// write the scalars, followed by the vectors
		mutator.writeSubColumn(
				getClusterTable(),
				rowKey,
				MicroCluster.N_STR,
				mutator.newColumn(MicroCluster.N_STR,
						Double.toString(cluKernel.getN())));

		mutator.writeSubColumn(
				getClusterTable(),
				rowKey,
				MicroCluster.LST_STR,
				mutator.newColumn(MicroCluster.LST_STR,
						Double.toString(cluKernel.getLST())));

		mutator.writeSubColumn(
				getClusterTable(),
				rowKey,
				MicroCluster.SST_STR,
				mutator.newColumn(MicroCluster.SST_STR,
						Double.toString(cluKernel.getSST())));

		mutator.writeSubColumn(
				getClusterTable(),
				rowKey,
				MicroCluster.CT_STR,
				mutator.newColumn(MicroCluster.CT_STR,
						Double.toString(cluKernel.getCT())));

		mutator.writeSubColumn(
				getClusterTable(),
				rowKey,
				MicroCluster.LAT_STR,
				mutator.newColumn(MicroCluster.LAT_STR,
						Double.toString(cluKernel.getLAT())));

		mutator.writeSubColumn(
				getClusterTable(),
				rowKey,
				MicroCluster.MAXRADIUS_STR,
				mutator.newColumn(MicroCluster.MAXRADIUS_STR,
						Double.toString(cluKernel.getMaxRadius())));

		// I am assuming the list cannot be reused until after the execute.

		// first, the LS vector, followed by the SS vector
		List<Column> LSColumns = new ArrayList<Column>();
		for (int i = 0; i < cluKernel.getLS().length; i++) {
			LSColumns.add(mutator.newColumn(Integer.toString(i),
					Double.toString(cluKernel.getLS()[i])));
		}
		mutator.writeSubColumns(getClusterTable(), rowKey,
				MicroCluster.LS_STR, LSColumns);

		List<Column> SSColumns = new ArrayList<Column>();
		for (int i = 0; i < cluKernel.getSS().length; i++) {
			SSColumns.add(mutator.newColumn(Integer.toString(i),
					Double.toString(cluKernel.getSS()[i])));
		}
		mutator.writeSubColumns(getClusterTable(), rowKey,
				MicroCluster.SS_STR, SSColumns);

		// This list of IDs (if any)
		List<Column> IDLISTColumns = new ArrayList<Column>();
		for (int i = 0; i < cluKernel.getIDLIST().size(); i++) {
			IDLISTColumns.add(mutator.newColumn(Integer.toString(i),
					Bytes.fromByteArray(cluKernel.getIDLIST().get(i))));
		}
		mutator.writeSubColumns(getClusterTable(), rowKey,
				MicroCluster.LISTID_STR, IDLISTColumns);

		// write the cluster to the data store
		mutator.execute(getConsistencyLevel());

		// mark it as being saved
		cluKernel.saved();

		// next, nail the cluster to the cluster index table (i.e., the data
		// stream timeline).
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis((long) cluKernel.getLAT());
		long dayRow = DateUtils.truncate(cal, Calendar.DATE).getTimeInMillis();
		long superCol = dayRow + DateUtils.getElapsedSeconds(cal);

		mutator.writeSubColumn(getClusterIndexTable(), Long.toString(dayRow),
				Bytes.fromByteBuffer(ByteBufferUtil.bytes(superCol)),
				mutator.newColumn(cluKernel.getID(), cluKernel.getID()));

		mutator.execute(getConsistencyLevel());

		// and finally, record the cluster's ID
		mutator.writeColumn(getClusterRecorderTable(), recorderTableRowKey,
				mutator.newColumn(cluKernel.getID(), cluKernel.getID()));

		mutator.execute(getConsistencyLevel());
	}

	/**
	 * Get a slice predicate based on the time horizon specified by the start
	 * and end times.
	 * 
	 * @param startMills
	 * @param endMills
	 * @return
	 */
	@SuppressWarnings("static-access")
	public SlicePredicate getSlicePredicate(long startMills, long endMills)
			throws Exception {
		return getSelector().newColumnsPredicate(
				Bytes.fromByteBuffer(ByteBufferUtil.bytes(startMills)),
				Bytes.fromByteBuffer(ByteBufferUtil.bytes(endMills)), false,
				Integer.MAX_VALUE);
	}

	/**
	 * Returns the row key, for the cluster index table, that corresponds to the
	 * specified Calendar
	 * 
	 * @param cal
	 * @return
	 */
	public static long getIndexRowKey(Calendar cal) {
		return DateUtils.truncate(cal, Calendar.DATE).getTimeInMillis();
	}

	// ////////////// Private Methods /////////////////////

	// Get supercolumns from the cluster index table within the specified range
	// (slice predicate). The subcolumns of the supercolumns, are indices or row
	// keys into the cluster table.
	private List<SuperColumn> getSuperColumnsFromIndex(long rowKey,
			SlicePredicate slicePredicate) throws Exception {

		LOG.trace("getSuperColumnsFromIndex: entered with row key = [" + rowKey
				+ "]");

		List<SuperColumn> superCols = new ArrayList<SuperColumn>();

		// apply the predicate to get the supercolumns from the row
		try {
			// if the predicate's start and finish are the same, then don't
			// bother with the predicate. Hhmmm? Is this really necessary?
			if (Arrays.equals(slicePredicate.getSlice_range().getStart(),
					slicePredicate.getSlice_range().getFinish())) {
				LOG.trace("getSuperColumnsFromIndex: predicate start and finish are the same");
				SuperColumn superCol = getSelector().getSuperColumnFromRow(
						getClusterIndexTable(),
						Long.toString(rowKey),
						Bytes.fromByteArray(slicePredicate.getSlice_range()
								.getStart()), getConsistencyLevel());
				if (superCol != null) {
					superCols.add(superCol);
				}
			} else {
				LOG.trace("getSuperColumnsFromIndex: using predicate with rowKey of "
						+ rowKey);
				LOG.trace("getSuperColumnsFromIndex: slice predicate = "
						+ slicePredicate.toString());
				superCols = getSelector().getSuperColumnsFromRow(
						getClusterIndexTable(), Long.toString(rowKey),
						slicePredicate, getConsistencyLevel());
			}

		} catch (NotFoundException ignore) {

		} catch (PelopsException exc) {
			LOG.error("ERROR:getSuperColumnsFromIndex got this exception ["
					+ exc.getMessage()
					+ "] when calling getSuperColumnsFromRow on single row");
			// return empty array list
		}
		LOG.trace("getSuperColumnsFromIndex: returning superCols of size = "
				+ superCols.size());
		return superCols;
	}

	// superCols is a list of supercolumns from the CIT, where each supercolumn
	// in the list has subcolumns that are row keys into the cluster table.
	// this method uses those row keys to create a collection of clusters.
	private void getClusandraKernels(List<SuperColumn> superCols,
			List<MicroCluster> cluKernels) throws Exception {

		if (superCols == null || superCols.isEmpty() || cluKernels == null) {
			return;
		}

		// the kernelTracker is used to prevent duplicates.
		Map<String, String> kernelTracker = new HashMap<String, String>(500);
		// Iterate through the list of supercolumns
		for (SuperColumn sc : superCols) {
			// Iterate through all the subcolumns in this super column
			for (Column col : sc.getColumns()) {
				// the name of a subcolumn in this supercolumn is a row key
				// (index) into the cluster table. so we use each subcolumn to
				// index into the cluster table and fetch the corresponding
				// list of supercolumns that represents a cluster and from
				// that we create a cluster (MicroCluster). We also use
				// the kernel tracker to eliminate duplicates.
				String clusterID = new String(col.getName());
				if (!kernelTracker.containsKey(clusterID)) {
					kernelTracker.put(clusterID, clusterID);
					List<SuperColumn> clusterSuperColumns = getSelector()
							.getSuperColumnsFromRow(getClusterTable(),
									clusterID, false, getConsistencyLevel());
					// from the list of supercolumns, create a cluster
					// (MicroCluster) and add it to the list of clusters
					// that we're going to return
					if (clusterSuperColumns != null
							&& !clusterSuperColumns.isEmpty()) {
						cluKernels.add(getClusandraKernel(clusterSuperColumns,
								clusterID));
					} else {
						LOG.trace("getClusandraKernels "
								+ "encountered an empty row in the CIT; most "
								+ "probably a zombie index");
						// REMOVE SUBCOLUMN FROM CIT!
					}
				}
			}
		}
		if (cluKernels.isEmpty()) {
			LOG.debug("getClusandraKernels - no clusters were found.");
		}
		return;
	}

	// Returns the name of the supercolumn, for the cluster index table, that
	// corresponds to the specified Calendar
	private long getSuperColumnIndexName(Calendar cal) {
		return getIndexRowKey(cal) + DateUtils.getElapsedSeconds(cal);
	}
}
