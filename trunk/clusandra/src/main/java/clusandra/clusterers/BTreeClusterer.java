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

import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import clusandra.cassandra.ClusandraDao;
import clusandra.core.CluMessage;
import clusandra.utils.BTree;
import clusandra.core.AbstractProcessor;
import static clusandra.utils.BTree.MAX_ENTRIES;

/**
 * 
 * The BTreeClusterer accepts microclusters from many KmeansClusterers and
 * maintains the microclusters in a BTree from where they are persisted to the
 * Cassandra DB.
 * 
 * Microclusters that are to be inserted into the tree can be absorbed by
 * existing microclusters. Also, microclusters being inserted can replace
 * existing microclusters that are no longer temporally relevant.
 * 
 * @author jfernandez
 * 
 */
public class BTreeClusterer extends AbstractProcessor {

	private static final Log LOG = LogFactory.getLog(BTreeClusterer.class);

	private static final double OVERLAP_FACTOR = 1.00d;

	// this clusterer's CassandraDao
	private ClusandraDao clusandraDao = null;

	// The overlap factor controls the merging of microclusters. If the factor
	// is set to 1.0, then the two clusters will merge iff their radii
	// overlap. If the factor is set to 0.5, then the two will merge iff
	// one-half their radii overlap. So in the latter case, the clusters
	// must be much closer to one another for the merging to take place.
	// Note that merging and absorbing are the same.
	private double overlapFactor = OVERLAP_FACTOR;

	// Lambda is the forgetfulness factor. It dictates how quickly a
	// microcluster becomes "temporally" irrelevant. The lower the value for
	// lambda, the quicker the microcluster becomes irrelevant. When a
	// microcluster becomes irrelevant it becomes a candidate for replacement or
	// removal from the tree.
	private double lambda = 0.5d;

	// the density, as a factor of maximum density, that a microcluster is
	// considered irrelevant. So if the factor is set to 0.25, then the
	// microcluster becomes temporally irrelevant if its density falls below 25%
	// of its maximum density.
	private double sparseFactor = 0.25d;

	// the maximum number of entries (children) that can occupy a tree node.
	private int maxEntries = MAX_ENTRIES;

	// the BTree used by this clusterer
	private BTree bTree;

	public BTreeClusterer() {
	}

	public BTreeClusterer(BTree bTree) {
		super();
		this.bTree = bTree;
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
	 * Get the BTree for this clusterer
	 * 
	 * @param bTree
	 */
	public BTree getBTree() {
		return bTree;
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

	public void setMaxEntries(int maxEntries) throws IllegalArgumentException {
		this.maxEntries = maxEntries;
	}

	public int getMaxEntries() {
		return maxEntries;
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
	 * Called by the QueueAgent to give the Clusterer a collection of
	 * micro-clusters to insert into the BTree.
	 * 
	 * @param cluMessages
	 * @throws Exception
	 */
	List<MicroCluster> debugList = new ArrayList<MicroCluster>();

	public void processCluMessages(List<CluMessage> cluMessages)
			throws Exception {

		LOG.debug("processCluMessages: entered");

		// begin by doing some validation
		if (cluMessages == null || cluMessages.isEmpty()) {
			LOG.warn("processCluMessages: entered with no messages");
			return;
		}

		LOG.debug("processCluMessages: received this many CluMessages: "
				+ cluMessages.size());

		if (!(cluMessages.get(0).getBody() instanceof MicroCluster)) {
			LOG.error("processCluMessages: object is not of type MicroCluster");
			throw new Exception(
					"processCluMessages: object is not of type MicroCluster");
		}

		// peek at the clusters
		MicroCluster cluster = (MicroCluster) cluMessages.get(0).getBody();

		if (getBTree() != null) {
			if (cluster.getCenter().length != getBTree().getNumDims()) {
				LOG.error("processCluMessages: cluster dimension does not match tree's given dimension");
				throw new Exception(
						"processCluMessages: cluster dimension does not match tree's given dimension");
			}
		} else {
			// create a BTree with a background house keeping thread
			BTree tree = new BTree(this.getMaxEntries(),
					cluster.getCenter().length, getQueueAgent()
							.getJmsReadTemplate().getReceiveTimeout());
			tree.setLambda(getLambda());
			tree.setOverlapFactor(getOverlapFactor());
			tree.setSparseFactor(this.getSparseFactor());
			setBTree(tree);
		}

		// insert the clusters into the BTree, but first make sure to lock the
		// tree
		getBTree().lock();
		try {
			for (CluMessage cluMessage : cluMessages) {
				cluster = (MicroCluster) cluMessage.getBody();
				getBTree().insert(cluster);
			}
		} finally {
			getBTree().unlock();
		}

		LOG.debug(getBTree().printStats());
		LOG.debug("processCluMessages: exit");
	}

}
