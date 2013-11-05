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
 * $Date:  $
 * $Revision: $
 * $Author: $
 * $Id: $
 * */
package clusandra.core;

import java.util.Map;
import java.util.List;

import clusandra.cassandra.ClusandraDao;
import clusandra.core.DataRecord;
import clusandra.core.QueueAgent;

/**
 * A Processor reads DataRecords or CluMessages from a JMS queue.
 * 
 * Even though this interface was originally intended for clusterers that act on
 * DataRecords, it can also be used for things other than clustering. For
 * example, perhaps it is used for gathering statistics across a sliding or
 * damped window or performing principal component analysis (PCA) on a set of
 * points prior to those points undergoing clustering.
 * 
 * @author jfernandez
 * 
 */

public interface Processor {

	/**
	 * Invoked by Spring to set the Map that contains configuration parameters
	 * for this Clusterer.
	 * 
	 * @param map
	 * @exception thrown
	 *                if there is an invalid cfg parameter
	 */
	public void setConfig(Map<String, String> map) throws Exception;

	/**
	 * Called by QueueAgent to initialize this Clusterer.
	 */
	public boolean initialize() throws Exception;

	/**
	 * Called by the QueueAgent to give the Processor a collection of objects,
	 * of type CluMessage, to process.
	 * 
	 * @param dataRecords
	 * @throws Exception
	 */
	public void processCluMessages(List<CluMessage> cluMessages)
			throws Exception;

	/**
	 * Called by the QueueAgent to give the Clusterer a collection of
	 * DataRecords to process.
	 * 
	 * @param dataRecords
	 * @throws Exception
	 */
	public void processDataRecords(List<DataRecord> dataRecords)
			throws Exception;

	/**
	 * Invoked by Spring to set the QueueAgent for this Processor. This is
	 * optional, as the QueueAgent can do the wiring.
	 * 
	 * @param map
	 */
	public void setQueueAgent(QueueAgent queueAgent);

	/**
	 * Returns the QueueAgent that is wired to this Clusterer.
	 * 
	 * @param map
	 */
	public QueueAgent getQueueAgent();

	/**
	 * The optional Cassandra DAO that is wired to this Clusterer.
	 * 
	 * @param cassandraDao
	 */
	public void setClusandraDao(ClusandraDao clusandraDao);
}
