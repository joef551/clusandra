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
 * $Id: CassandraDao.java 125 2011-08-23 19:06:55Z jose $
 */
package clusandra.cassandra;

import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.ClusterManager;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.springframework.beans.factory.InitializingBean;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * This is a Cassandra Data Access Object (DAO) for CluSandra. It is not meant
 * to be a general purpose Cassandra DAO. However, some portions can perhaps be
 * used to develop a general purpose Cassandra DAO.
 * 
 * @author jfernandez
 * 
 */
public class CassandraDao implements InitializingBean {

	private static final Log LOG = LogFactory.getLog(CassandraDao.class);

	private ClusterManager clusterManager = null;
	// The Cassandra cluster that will be used by this DAO
	private Cluster cassandraCluster = null;
	// The default key space in the Cassandra cluster
	private String keySpace = "clusandra";
	// The Cassandra connection pool
	private CommonsBackedPool commonsBackedPool;
	// the consistency level to be used by this DAU
	private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
	private String consistency = null;

	public CassandraDao() {
	}

	/**
	 * Set the Cassandra consistency level for this DAO. This is a Spring
	 * injection method only.
	 * 
	 * @param consistencyLevel
	 * @throws Exception
	 */
	public void setConsistency(String consistency) throws Exception {
		this.consistency = consistency;
		setConsistencyLevel(consistency);
	}

	/**
	 * Get the consistency level being used by this DAO. This String version is
	 * used to keep Spring happy.
	 * 
	 * @return
	 */
	public String getConsistency() {
		return consistency;
	}

	/**
	 * Set the Cassandra consistency level for this DAO
	 * 
	 * @param consistencyLevel
	 * @throws Exception
	 */
	public void setConsistencyLevel(String consistencyLevel) throws Exception {
		this.consistencyLevel = ConsistencyLevel.valueOf(consistencyLevel);
	}

	/**
	 * Get the consistency level being used by this DAO.
	 * 
	 * @return
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	/**
	 * The Cassandra cluster for this DAO. 
	 * 
	 * @param cluster
	 */
	public void setCassandraCluster(Cluster cluster) {
		this.cassandraCluster = cluster;
	}

	/**
	 * Get the Cassandra cluster that is being used by this DAO
	 * 
	 * @return
	 */
	public Cluster getCassandraCluster() {
		return cassandraCluster;
	}

	/**
	 * Set the Cassandra key space for this DAO.
	 * 
	 * @param keySpace
	 */
	public void setKeySpace(String keySpace) {
		this.keySpace = keySpace;
	}

	/**
	 * Get the Cassandra key space that is being used by this DAO
	 * 
	 * @return
	 */
	public String getKeySpace() {
		return keySpace;
	}

	/**
	 * Get a selector from the pool. The selector is used to fetch clusters
	 * from the Cassandra data store.
	 * 
	 * @return
	 */
	public Selector getSelector() throws Exception {
		return commonsBackedPool.createSelector();
	}

	/**
	 * Get a mutator from the pool. The mutator is used to write clusters to the
	 * data store.
	 * 
	 * @return
	 */
	public Mutator getMutator() throws Exception {
		return commonsBackedPool.createMutator();
	}

	/**
	 * Return the cluster manager for this DAO. This has nothing to do with
	 * CluSandra micro or super clusters. It is the "Cassandra" cluster.
	 * 
	 * @return
	 */
	public ClusterManager getCassandraClusterManager() {
		return clusterManager;
	}

	/**
	 * Get the name of the Cassandra cluster. Only nodes with the same cluster
	 * name communicate using the Gossip P2P protocol.
	 * 
	 * @return The name of the cluster
	 * @throws Exception
	 */
	public String getCassandraClusterName() throws Exception {
		return getCassandraClusterManager().getClusterName();
	}

	/**
	 * Get the version of the Cassandra software being run by the cluster.
	 * 
	 * @return The version of the Cassandra software
	 * @throws Exception
	 */
	public String getCassandraVersion() throws Exception {
		return getCassandraClusterManager().getCassandraVersion();
	}

	/**
	 * Return the thrift pool being used by this CassandraDAO.
	 * 
	 * @return
	 */
	public IThriftPool getThriftPool() {
		return this.commonsBackedPool;
	}

	/**
	 * Get a slice predicate based on the "time horizon" specified by the start
	 * and end times.
	 * 
	 * @param startMills
	 * @param endMills
	 * @return
	 */
	@SuppressWarnings("static-access")
	public SlicePredicate getSlicePredicate(long startMills, long endMills)
			throws Exception {
		return getSelector().newColumnsPredicate(Long.toString(startMills),
				Long.toString(endMills), false, Integer.MAX_VALUE);
	}

	/**
	 * This method is invoked by Spring after this DAO's (POJO) properties have been
	 * set. It performs the final initialization.
	 */
	public void afterPropertiesSet() throws Exception {
		if (getCassandraCluster() == null) {
			throw new Exception("ERROR: CassandraDao was not given a cluster");
		} else if (getKeySpace() == null) {
			throw new Exception("ERROR: CassandraDao was not given a keySpace");
		}
		commonsBackedPool = new CommonsBackedPool(getCassandraCluster(),
				getKeySpace());
		clusterManager = Pelops.createClusterManager(getCassandraCluster());
	}

}
