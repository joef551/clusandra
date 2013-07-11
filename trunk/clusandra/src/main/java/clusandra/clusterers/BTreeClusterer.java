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

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import clusandra.cassandra.ClusandraDao;
import clusandra.core.DataRecord;
import clusandra.core.QueueAgent;
import clusandra.core.Processor;
import clusandra.core.CluMessage;
import clusandra.utils.BTree;
import static clusandra.utils.BTree.MAX_ENTRIES;

/**
 * 
 * The BTreeClusterer accepts micro-clusters from many KmeansClusterers and
 * inserts the micro-clusters into a BTree from where they are persisted to the
 * Cassandra DB.
 * 
 * @author jfernandez
 * 
 */
public class BTreeClusterer implements Processor {

	private static final Log LOG = LogFactory.getLog(BTreeClusterer.class);

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

	// the maximum number of entries (children) that can occupy a tree node.
	private int maxEntries = MAX_ENTRIES;

	// the BTree used by this clusterer
	private BTree bTree;

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// ~~~~~~~~~~~~~ This Algorithm's Configurable Properties ~~~~~~~~~~~~
	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// the amount of time a microcluster is allowed to remain active without
	// absorbing a Datarecord
	private static final String overlapFactorKey = "overlapFactor";
	private static final String sparseFactorKey = "sparseFactor";
	private static final String lambdaKey = "lambda";
	private static final String maxEntriesKey = "maxEntries";

	// this clusterers queue agent. this clusterer will only read
	// messages from a queue.
	private QueueAgent queueAgent;

	public BTreeClusterer() {
		super();
	}

	public BTreeClusterer(BTree bTree) {
		super();
		this.bTree = bTree;
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
			} else if (maxEntriesKey.equals(key)) {
				setMaxEntries(map.get(key));
				LOG.trace("maxEntries = " + getMaxEntries());
			}
		}
	}

	/**
	 * Set the BTree for this clusterer
	 * 
	 * @param bTree
	 */
	public void setBTree(BTree bTree) {
		this.bTree = bTree;
	}

	/**
	 * Set the BTree for this clusterer
	 * 
	 * @param bTree
	 */
	public BTree getBTree() {
		return bTree;
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
	 * Called by setConfig() to set the max number of entries in each tree node.
	 * 
	 * @param overlapFactor
	 * @throws Exception
	 */
	public void setMaxEntries(String maxEntries) throws Exception {
		setMaxEntries(Integer.parseInt(maxEntries));
	}

	public void setMaxEntries(int maxEntries) throws IllegalArgumentException {
		this.maxEntries = maxEntries;
	}

	/**
	 * Returns the max number of entries per tree node.
	 * 
	 * @return
	 */
	public int getMaxEntries() {
		return maxEntries;
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
		} else if (getQueueAgent().getJmsReadDestination() == null) {
			throw new Exception(
					"this clusterer has not been wired to a JmsReadDestination");
		} else if (getQueueAgent().getJmsReadTemplate() == null) {
			throw new Exception(
					"this clusterer has not been wired to a JmsReadTemplate");
		}
		return true;
	}

	/**
	 * Called by the QueueAgent to give the Clusterer a collection of
	 * micro-clusters to insert into the BTree.
	 * 
	 * @param cluMessages
	 * @throws Exception
	 */
	public void processCluMessages(List<CluMessage> cluMessages)
			throws Exception {

		LOG.debug("processCluMessages: entered");

		if (cluMessages == null || cluMessages.isEmpty()) {
			LOG.warn("processCluMessages: entered with no messages");
			return;
		}

		if (!(cluMessages.get(0).getBody() instanceof ClusandraKernel)) {
			LOG.error("processCluMessages: object is not of type ClusandraKernel");
			throw new Exception(
					"processCluMessages: object is not of type ClusandraKernel");
		}
		ClusandraKernel cluster = (ClusandraKernel) cluMessages.get(0)
				.getBody();
		if (getBTree() != null
				&& cluster.getCenter().length != getBTree().getMaxEntries()) {
			LOG.error("processCluMessages: cluster dimension does not match tree's given dimension");
			throw new Exception(
					"processCluMessages: cluster dimension does not match tree's given dimension");
		}

		if (getBTree() == null) {
			BTree tree = new BTree(this.getMaxEntries(),cluster.getCenter().length);
			tree.setLambda(getLambda());
			tree.setOverlapFactor(getOverlapFactor());
			setBTree(tree);
		}

		// insert the clusters into the BTree
		for (CluMessage cluMessage : cluMessages) {
			cluster = (ClusandraKernel) cluMessage.getBody();
			getBTree().insert(cluster);
		}

		LOG.debug(getBTree().printStats());
		LOG.debug("processCluMessages: exit");
	}

	public void processDataRecords(List<DataRecord> dataRecords)
			throws Exception {
		throw new UnsupportedOperationException();
	}

}
