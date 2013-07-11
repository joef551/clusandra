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
 * $Date: 2011-07-28 22:52:59 -0400 (Thu, 28 Jul 2011) $
 * $Revision: 94 $
 * $Author: jose $
 * $Id: QueueAgent.java 94 2011-07-29 02:52:59Z jose $
 */
package clusandra.core;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.JmsException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.jms.support.converter.MessageConversionException;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.io.Serializable;
import java.util.Vector;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;

import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import static javax.jms.Session.CLIENT_ACKNOWLEDGE;
import javax.jms.JMSException;

import clusandra.stream.StreamGenerator;

/**
 * The QueueAgent is a Clusandra Runnable object that is wired to a JmsTemplate
 * for reading and/or a different JmsTemplate for writing.
 * 
 * The QueueAgent is also wired to either a StreamGenerator or Processor; it
 * cannot be wired to both.
 * 
 * A StreamGenerator will only use the JmsTemplate for writing to a queue,
 * whilst a Processor must use the read JmsTemplate and may optionally use a
 * write JmsTemplate.
 * 
 * The KmeansClusterer is an example of a Processor that acts as clusterer. It
 * produces micro-clusters than can then be sent to a cluster reducer
 * (BTreeClusterer) from where the reduced micro-clusters are persisted to the
 * Cassandra DB. The general idea is that the processing, or in this case
 * clustering, of the data stream can be fanned out or balanced across multiple
 * instances of a Processor. Thos instances then send their resulting
 * micro-clusters to the reducer.
 * 
 * Another example of a Processor would be a component that executes a running
 * query on the data stream. The query would be based on a sliding or damped
 * window.
 * 
 * @author jfernandez
 * 
 */
@ManagedResource(objectName = "CluSandra:name=CluRunnable")
public class QueueAgent implements CluRunnable, Runnable, BeanNameAware {

	private static final Log LOG = LogFactory.getLog(QueueAgent.class);
	// The JMS template to use for reading from a queue
	JmsTemplate jmsReadTemplate;
	// The JMS template to use for writing to a queue
	JmsTemplate jmsWriteTemplate;
	// The JMS destination (queue) to read from
	String jmsReadDestination;
	// The JMS destination (queue) to write to
	String jmsWriteDestination;
	// The max size of the send buffer
	int sendSize = 20;
	// the maximum number of DataRecords to read
	int readSize = 20;
	// The send buffer
	Vector<Serializable> sendBuffer = new Vector<Serializable>();
	// thread that will do all the running
	Thread runner;
	// the Processor that is wired to this QueueAgent
	Processor processor = null;
	// the StreamGenerator that is wired to this QueueAgent
	StreamGenerator streamGenerator = null;
	// this cluRunnable's unique name
	String name = "";

	boolean testing = true;

	public QueueAgent() {
	}

	public QueueAgent(JmsTemplate jmsReadTemplate, JmsTemplate jmsWriteTemplate) {
		this.jmsReadTemplate = jmsReadTemplate;
		this.jmsWriteTemplate = jmsWriteTemplate;
	}

	/**
	 * This method is invoked by the CluRunner to start the QueueAgent and its
	 * wired beans.
	 * 
	 * Stream readers and clusterers must be assigned a JMS destination.
	 * Clusterers may be assigned a CassandraDao.
	 * 
	 */
	public void cluRun() throws Exception {
		if (getJmsReadTemplate() == null && isProcessor()) {
			LOG.error("ERROR: Processor has not been assigned a JmsReadTemplate");
			throw new Exception("JmsReadTemplate has not been set");
		} else if (getJmsWriteTemplate() == null
				&& getStreamGenerator() != null) {
			LOG.error("ERROR: StreamGenerator has not been assigned a JmsWriteTemplate");
			throw new Exception("JmsWriteTemplate has not been set");
		} else if (getStreamGenerator() == null && !isProcessor()) {
			LOG.error("ERROR: QueueAgent is wired neither to a StreamGenerator nor Processor");
			throw new Exception(
					"QueueAgent is wired neither to a StreamGenerator nor Processor");
		} else if (getStreamGenerator() != null && isProcessor()) {
			LOG.error("ERROR: QueueAgent cannot be wired to both StreamGenerator and Processor");
			throw new Exception(
					"QueueAgent cannot be wired to both StreamGenerator and Processor");
		}
		//runner = new Thread(this, "CluSandra QueueAgent: " + toString());
		//runner.setDaemon(true);
		//runner.start();
		run();
	}

	/**
	 * JMX-invoked operation to shut down this CluRunnable
	 */
	@ManagedOperation(description="Shut down or stop this component")
	public void shutdown() {
		// needs work
		System.exit(0);
	}

	/**
	 * Set this CluSandra component's unique name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Get this CluSandra component's unique name
	 */
	@ManagedAttribute
	public String getName() {
		return name;
	}

	/**
	 * Called by Spring to pass this QueueAgent (bean) its bean name.
	 * 
	 * @param beanName
	 */
	public void setBeanName(String beanName) {
		name = beanName;
	}

	/**
	 * Set the name of the write queue for this QueueAgent.
	 * 
	 * @param jmsDestination
	 */
	public void setJmsWriteDestination(String jmsDestination) {
		this.jmsWriteDestination = jmsDestination;
	}

	/**
	 * Get the name of the write queue being used by this QueueAgent.
	 * 
	 * @return
	 */
	public String getJmsWriteDestination() {
		return jmsWriteDestination;
	}

	/**
	 * Assign this QueueAgent its write queue JmsTemplate.
	 * 
	 * @param jmsTemplate
	 */
	public void setJmsWriteTemplate(JmsTemplate jmsTemplate) {
		this.jmsWriteTemplate = jmsTemplate;
	}

	/**
	 * Get the JMS template for the write queue.
	 * 
	 * @return
	 */
	public JmsTemplate getJmsWriteTemplate() {
		return jmsWriteTemplate;
	}

	/**
	 * Set the name of the read queue for this QueueAgent.
	 * 
	 * @param jmsDestination
	 */
	public void setJmsReadDestination(String jmsDestination) {
		this.jmsReadDestination = jmsDestination;
	}

	/**
	 * Get the name of the read queue being used by this QueueAgent.
	 * 
	 * @return
	 */
	public String getJmsReadDestination() {
		return jmsReadDestination;
	}

	/**
	 * Assign this QueueAgent its read queue JmsTemplate.
	 * 
	 * @param jmsTemplate
	 */
	public void setJmsReadTemplate(JmsTemplate jmsTemplate) {
		this.jmsReadTemplate = jmsTemplate;
	}

	/**
	 * Get the JMS template for the read queue being used by this QueueAgent
	 * 
	 * @return
	 */
	public JmsTemplate getJmsReadTemplate() {
		return jmsReadTemplate;
	}

	/**
	 * Set the max size of the send buffer. The buffer is flushed when it
	 * reaches this size.
	 * 
	 * @param sendSize
	 */
	public void setSendSize(int sendSize) {
		this.sendSize = sendSize;
	}

	/**
	 * Get the size of the send buffer
	 * 
	 * @return
	 */
	@ManagedAttribute
	public int getSendSize() {
		return sendSize;
	}

	/**
	 * Set the max number of DataRecords to read from queue. The Clusterer's
	 * processDataRecords method is called when this max is reached, or
	 * breached, or the read timeout expires and DataRecords have been read.
	 * 
	 * @param sendSize
	 */
	public void setReadSize(int readSize) {
		this.readSize = readSize;
	}

	/**
	 * Get the max read size
	 * 
	 * @return
	 */
	@ManagedAttribute
	public int getReadSize() {
		return this.readSize;
	}

	/**
	 * Place a message in the send buffer. You cannot mix message types in the
	 * send buffer. They must all be of type CluMessage or DataRecord. The
	 * buffer is automatically flushed to the JMS provider when it reaches its
	 * maximum size or you can invoke flush() to flush it manually. After the
	 * buffer is flushed, it can be reloaded with a different message type.
	 * 
	 * @param message
	 * @return the number of messages that were flushed to the JMS queue or 0 if
	 *         the DataRecords were not yet flushed
	 */
	public synchronized int sendMessage(Serializable message) throws Exception {
		if (message == null) {
			LOG.warn("WARNING: message is null");
			return 0;
		}
		if (this.getJmsWriteTemplate() == null) {
			LOG.warn("ERROR: this QueueAgent has not been wired to a JmsWriteTemaplte");
			throw new Exception(
					"ERROR: this QueueAgent has not been wired to a JmsWriteTemaplte");
		}
		getSendBuffer().add(message);
		if (getSendBuffer().size() == getSendSize()) {
			return sendQ();
		}
		return 0;
	}

	/**
	 * Immediately flushes any buffered objects to the JMS queue. Ignores buffer
	 * size.
	 * 
	 * @return number of DataRecords flushed to JMQ queue.
	 */
	public synchronized int flush() {
		return sendQ();
	}

	/**
	 * Spring invokes this method to wire the QueueAgent to a Processor.
	 * 
	 * @param Processor
	 */
	public void setProcessor(Processor processor) {
		this.processor = processor;
	}

	/**
	 * Get the Processor that is wired to this QueueAgent.
	 * 
	 * @return
	 */
	public Processor getProcessor() {
		return processor;
	}

	/**
	 * Spring invokes this method to wire the QueueAgent to a StreamGenerator.
	 * 
	 * @param clusterer
	 */
	public void setStreamGenerator(StreamGenerator streamGenerator) {
		this.streamGenerator = streamGenerator;
	}

	/**
	 * Get the StreamGenerator that is wired to this QueueAgent.
	 * 
	 * @return
	 */
	public StreamGenerator getStreamGenerator() {
		return streamGenerator;
	}

	/**
	 * Start this QueueAgent's thread.
	 */
	public void run() {

		LOG.info("QueueAgent started");

		// See if this QueueAgent is hosting a StreamGenerator or Processor. If
		// it is the latter then read from assigned queue, else give control to
		// the StreamGenerator.
		if (getStreamGenerator() != null) {
			if (getStreamGenerator().getQueueAgent() == null) {
				LOG.error("ERROR: the StreamGenerator has not been wired to a QueueAgent");
				return;
			} else if (!getStreamGenerator().getQueueAgent().getName()
					.equals(getName())) {
				LOG.error("ERROR: the StreamGenerator is not wired to this QueueAgent");
				return;
			}
			try {
				getStreamGenerator().startGenerator();
			} catch (Exception exc) {
				LOG.error("ERROR: exception from QueueAgent's StreamGenerator - "
						+ exc.getMessage());
				exc.printStackTrace(System.out);
			}
		} else if (getProcessor() != null) {
			// if we're not wired to a StreamGenerator, then we must be wired to
			// a Processor. Ensure our Processor is wired to this QueueAgent
			if (getProcessor().getQueueAgent() == null) {
				getProcessor().setQueueAgent(this);
			} else if (!getProcessor().getQueueAgent().getName()
					.equals(getName())) {
				LOG.error("ERROR: the Processor is prewired to a QueueAgent, but it is not this QueueAgent");
				return;
			}
			try {
				if (getProcessor().initialize()) {
					// block on the read queue
					readQ();
				}
			} catch (Exception exc) {
				LOG.error("ERROR: exception from QueueAgent's queue reader - "
						+ exc.getMessage());
				exc.printStackTrace(System.out);
			}
		}
		LOG.info("QueueAgent completed");
		return;
	}

	private Vector<Serializable> getSendBuffer() {
		return sendBuffer;
	}

	// Send the contents of the message buffer to the JMS provider.
	private int sendQ() {
		int sendSize = 0;
		if (getSendBuffer().isEmpty()) {
			return sendSize;
		}
		sendSize = getSendBuffer().size();
		try {
			getJmsWriteTemplate().send(getJmsWriteDestination(),
					new MessageCreator() {
						public Message createMessage(Session session)
								throws JMSException {
							return session.createObjectMessage(getSendBuffer());
						}
					});
		} catch (JmsException exc) {
			LOG.error("ERROR, received this JmsException when sending:"
					+ exc.getMessage());
			exc.printStackTrace(System.out);
			return 0;
		}
		getSendBuffer().clear();
		return sendSize;
	}

	/*
	 * This is the method that does the reading from the JMS queue. The
	 * DataRecords or CluMessages that are read from the queue are given to the
	 * Clusterer or Processor to process. The read from the queue takes place
	 * until: 1. The read times out and messages had been previously received or
	 * 2. The max number of messages have been received.
	 * 
	 * 
	 * 
	 * TODO: DataRecord and Clusterer are deprecated and code should eventually
	 * remove references to them
	 */
	void readQ() throws Exception {
		List<DataRecord> dataRecords = new ArrayList<DataRecord>();
		List<CluMessage> cluMessages = new ArrayList<CluMessage>();
		Message msg = null;
		Message lastMsgRead = null;
		while (true) {
			try {

				// wait for a message (payload) to arrive - the wait time is
				// specified in the Spring XML file for this QueueAgent
				LOG.trace("readQ: blocking on queue");
				if ((msg = getJmsReadTemplate()
						.receive(getJmsReadDestination())) != null) {

					LOG.trace("readQ: receive returns " + msg.toString());
					// An object message with payload has been read
					lastMsgRead = msg;

					if (!(msg instanceof ObjectMessage)) {
						LOG.error("ERROR: message received was not of type ObjectMessage");
						throw new MessageConversionException(
								"message received was not of type ObjectMessage");
					}

					Object ob1 = ((ObjectMessage) msg).getObject();
					if (!(ob1 instanceof Vector)) {
						LOG.error("ERROR: object received was not of type Vector");
						throw new MessageConversionException(
								"object received was not of type Vector");
					}
					@SuppressWarnings("rawtypes")
					Vector v2 = (Vector) ob1;

					ob1 = v2.get(0);
					if (ob1 instanceof DataRecord) {
						for (Object ob2 : v2) {
							if (ob2 instanceof DataRecord) {
								dataRecords.add((DataRecord) ob2);
							} else {
								LOG.error("ERROR: object in received "
										+ "Vector was not of type DataRecord");
								throw new MessageConversionException(
										"object in received  "
												+ "Vector was not of type DataRecord");
							}
						}
					} else if (ob1 instanceof CluMessage) {
						for (Object ob2 : v2) {
							if (ob2 instanceof CluMessage) {
								cluMessages.add((CluMessage) ob2);
							} else {
								LOG.error("ERROR: object in received "
										+ "Vector was not of type CluMessage");
								throw new MessageConversionException(
										"object in received  "
												+ "Vector was not of type CluMessage");
							}
						}

					} else {
						LOG.error("ERROR: object in received Vector was of unknow type");
						throw new MessageConversionException(
								"object in received  "
										+ "Vector was of uknown type");
					}
				}

				/*
				 * process the just-received payload iff one of the following
				 * has occurred: 1. The receive timed out, but messages had been
				 * previously read, or 2. The payload has exceeded its max size.
				 * Acknowledge all messages after the processor does its thing.
				 */
				if ((msg == null && !dataRecords.isEmpty())
						|| dataRecords.size() >= getReadSize()) {
					try {
						getProcessor().processDataRecords(dataRecords);
						// Ok to now acknowledge all messages read
						if (getJmsReadTemplate().getSessionAcknowledgeMode() == CLIENT_ACKNOWLEDGE) {
							lastMsgRead.acknowledge();
						}

					} finally {
						lastMsgRead = null;
						dataRecords.clear();
					}
				}
				if ((msg == null && !cluMessages.isEmpty())
						|| cluMessages.size() >= getReadSize()) {
					try {
						getProcessor().processCluMessages(cluMessages);
						// Ok to now acknowledge all messages read
						if (getJmsReadTemplate().getSessionAcknowledgeMode() == CLIENT_ACKNOWLEDGE) {
							lastMsgRead.acknowledge();
						}
					} finally {
						lastMsgRead = null;
						cluMessages.clear();
					}
				}

			} catch (JmsException exc) {
				LOG.error("ERROR, received this JmsException when receiving: "
						+ exc.getMessage());
				exc.printStackTrace();
				return;
			} catch (Exception exc) {
				LOG.error("ERROR, received this Exception when receiving: "
						+ exc.getMessage());
				exc.printStackTrace();
				return;
			}
		} // while(true)
	}

	private boolean isProcessor() {
		return (getProcessor() != null);
	}

}
