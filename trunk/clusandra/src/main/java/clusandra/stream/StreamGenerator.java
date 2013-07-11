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
 * $Date: 2011-08-25 22:45:57 -0400 (Thu, 25 Aug 2011) $
 * $Revision: 130 $
 * $Author: jose $
 * $Id: StreamGenerator.java 130 2011-08-26 02:45:57Z jose $
 */
package clusandra.stream;

import java.util.Map;
import clusandra.core.QueueAgent;

/**
 * A StreamGenerator reads data records (tuples) off or from a stream, transforms
 * those records into objects of type clusandra.core.DataRecord and then sends
 * those DataRecords to a work queue that is serviced by one or more instances
 * of a Clusterer. When a DataRecord is created, it must be time stamped. It
 * will, by default, time stamp itself.
 * 
 * The StreamGenerator's startReader() method is invoked by a QueueAgent that has
 * been wired to the StreamGenerator.
 * 
 * The StreamGenerator invokes the QueueAgent's sendQ() method to send DataRecords
 * to the JMS queue that has been wired to the QueueAgent.
 * 
 * @author jfernandez
 * 
 */

public interface StreamGenerator {

	/**
	 * Invoked by Spring to set the Map that contains configuration parameters
	 * for this StreamGenerator.
	 * 
	 * @param map
	 */
	public void setConfig(Map<String, String> map) throws Exception;

	/**
	 * Invoked by Spring to set the QueueAgent for this StreamGenerator.
	 * 
	 * @param map
	 */
	public void setQueueAgent(QueueAgent queueAgent);

	/**
	 * Returns the QueueAgent that is wired to this StreamGenerator.
	 * 
	 * @param map
	 */
	public QueueAgent getQueueAgent();

	/**
	 * This method is invoked by the QueueAgent to start and give control to the
	 * StreamGenerator.
	 */
	public void startGenerator() throws Exception;

}
