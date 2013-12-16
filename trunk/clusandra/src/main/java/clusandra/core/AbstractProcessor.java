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
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.BeanNameAware;

public abstract class AbstractProcessor implements Processor, InitializingBean,
		BeanNameAware {

	private QueueAgent queueAgent;
	private String beanName;

	/**
	 * Must be implemented by the subclass
	 */
	public abstract void setConfig(Map<String, String> map) throws Exception;

	/**
	 * Needs to be implemented by the subclass
	 */
	public void processCluMessages(List<CluMessage> cluMessages)
			throws Exception {
	}

	/**
	 * Needs to be implemented by the subclass
	 */
	public void produceCluMessages() throws Exception {
	}

	/**
	 * Invoked by Spring to set the QueueAgent for this StreamGenerator.
	 * 
	 * @param map
	 */
	public void setQueueAgent(QueueAgent queueAgent) {
		this.queueAgent = queueAgent;
	}

	/**
	 * Returns the QueueAgent that is wired to this StreamGenerator.
	 * 
	 * @param map
	 */
	public QueueAgent getQueueAgent() {
		return queueAgent;
	}

	public void afterPropertiesSet() throws Exception {
		if (getQueueAgent() == null) {
			throw new Exception(
					"This Processor was not assigned a QueueAgent: "
							+ getBeanName());
		}
	}

	/**
	 * Called by Spring to set the name of this bean
	 */
	public void setBeanName(String name) {
		beanName = name;
	}

	public String getBeanName() {
		return beanName;
	}

}
