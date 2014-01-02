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

import java.util.List;
import clusandra.core.QueueAgent;

/**
 * A Processor acts on and/or produces CluMessages. A stream generator is an
 * example of a Processor that produces CluMessages, while a clusterer is an
 * exmple of a Processor that processes or consumes CluMessages. A Processor can
 * both produce and consume messages.
 * 
 * @author jfernandez
 * 
 */
public interface Processor {

	/**
	 * Called by the QueueAgent to give the Processor a List of CluMessages to
	 * process or consume.
	 * 
	 * @param dataRecords
	 * @throws Exception
	 */
	public void processCluMessages(List<CluMessage> cluMessages)
			throws Exception;

	/**
	 * Called by the QueueAgent to give control to the Processor; as would be
	 * the case when the QueueAgent has not been assigned a read queue.
	 * 
	 * @param dataRecords
	 * @throws Exception
	 */
	public void produceCluMessages() throws Exception;

	/**
	 * Invoked by Spring to inject the QueueAgent for this Processor.
	 * 
	 * @param map
	 */
	public void setQueueAgent(QueueAgent queueAgent);

	/**
	 * Returns the QueueAgent that is wired to this Processor.
	 * 
	 * @param map
	 */
	public QueueAgent getQueueAgent();

}
