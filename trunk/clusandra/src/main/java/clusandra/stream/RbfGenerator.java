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
 * $Date: 2011-11-14 13:53:15 -0500 (Mon, 14 Nov 2011) $
 * $Revision: 163 $
 * $Author: jose $
 * $Id: RbfGenerator.java 163 2011-11-14 18:53:15Z jose $
 */
package clusandra.stream;

import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import clusandra.core.QueueAgent;
import clusandra.core.DataRecord;
//import moa.streams.generators.RandomRBFGenerator;
import moa.options.IntOption;
import weka.core.Instance;


/**
 * This stream reader/generator, which is used for testing and experimentation,
 * uses the MOA RandomRBFGenerator to generate a synthetic data stream.
 * 
 * @author jfernandez
 * 
 */
public class RbfGenerator implements StreamGenerator {

	private static final Log LOG = LogFactory.getLog(RbfGenerator.class);

	private static final String dataSetSizeKey = "dataSetSize";
	private static final String classCountKey = "classCount";
	private static final String attributeCountKey = "attributeCount";
	

	private QueueAgent queueAgent;

	// the default number of data records to generate
	private int dataSetSize = 100;
	// the default number of classes to generate
	private int classCount = 2;
	// the default number of attributes
	private int attributeCount = 10;
	
	// the file name to use for writing out the stream
	private String fileName = null;
	
	
	// The RBF Generator for this Reader
	RandomRbfGenerator generator = new RandomRbfGenerator();

	/**
	 * Invoked by Spring to set the Map that contains configuration parameters
	 * for this StreamGenerator.
	 * 
	 * @param map
	 */
	public void setConfig(Map<String, String> map) throws Exception {
		if (map == null || map.isEmpty()) {
			return;
		}
		for (String key : map.keySet()) {
			if (dataSetSizeKey.equals(key)) {
				setDataSetSize(map.get(key));
				LOG.trace("setConfig:dataSetSize = " + getDataSetSize());
			} else if (classCountKey.equals(key)) {
				setClassCount(map.get(key));
				LOG.trace("setConfig:classCount = " + getClassCount());
			} else if (attributeCountKey.equals(key)) {
				setAttributeCount(map.get(key));
				LOG.trace("setConfig:attributeCount = " + getAttributeCount());
			} 
		}
	}
	
	/**
	 * Set the file name that is used to hold the stream.  
	 * @param fileName
	 * @throws Exception
	 */
	public void setFileName(String fileName) throws Exception {
		if(fileName == null || fileName.length() == 0){
			throw new IllegalArgumentException();
		}
		this.fileName = fileName;
	}
	

	/**
	 * Called by setConfig() to set the data set size.
	 * 
	 * @param clusterDistanceFactor
	 * @throws Exception
	 */
	public void setDataSetSize(String dataSetSize) throws Exception {
		setDataSetSize(Integer.parseInt(dataSetSize));
	}

	public void setDataSetSize(int dataSetSize) {
		this.dataSetSize = dataSetSize;
	}

	/**
	 * Returns the data set size being used.
	 * 
	 * @return
	 */
	public int getDataSetSize() {
		return dataSetSize;
	}

	/**
	 * Called by setConfig() to set the class count.
	 * 
	 * @param clusterDistanceFactor
	 * @throws Exception
	 */
	public void setClassCount(String classCount) throws Exception {
		setClassCount(Integer.parseInt(classCount));
	}

	public void setClassCount(int classCount) {
		this.classCount = classCount;
	}

	/**
	 * Returns the class count being used.
	 * 
	 * @return
	 */
	public int getClassCount() {
		return classCount;
	}

	/**
	 * Called by setConfig() to set the attribute count.
	 * 
	 * @param clusterDistanceFactor
	 * @throws Exception
	 */
	public void setAttributeCount(String attributeCount) throws Exception {
		setAttributeCount(Integer.parseInt(attributeCount));
	}

	public void setAttributeCount(int attributeCount) {
		this.attributeCount = attributeCount;
	}

	/**
	 * Returns the attribute count being used.
	 * 
	 * @return
	 */
	public int getAttributeCount() {
		return attributeCount;
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

	/**
	 * This method is invoked by the QueueAgent to start and give control to the
	 * generator.
	 */
	public void startGenerator() {
		setOptions();
		getGenerator().prepareForUse();
		generateStream();
	}

	/**
	 * Generate the stream and send it to the MQS.
	 */
	public void generateStream() {
		// generate the stream records
		LOG.info("RbfGenerator: dataSetSize = " + dataSetSize);
		int sampleCnt = 0;
		for (int i = 0; i < dataSetSize; i++, sampleCnt++) {
			double[] payload = new double[getAttributeCount()];
			Instance instance = getGenerator().nextInstance();
			System.arraycopy(instance.toDoubleArray(), 0, payload, 0,
					payload.length);
			// Create a DataRecord and give it to the QueueAgent to send it to
			// the CluSandra messaging system.
			DataRecord dRecord = new DataRecord(payload);
			getQueueAgent().sendMessage(dRecord);
		}
		LOG.info("RbfGenerator: final send count = " + sampleCnt);
	}
	
	/**
	 * Generate the stream and send it to a file.
	 */
	public void generateStreamToFile() {
		// generate the stream records
		LOG.info("RbfGenerator: dataSetSize = " + dataSetSize);
		int sampleCnt = 0;
		for (int i = 0; i < dataSetSize; i++, sampleCnt++) {
			double[] payload = new double[getAttributeCount()];
			Instance instance = getGenerator().nextInstance();
			System.arraycopy(instance.toDoubleArray(), 0, payload, 0,
					payload.length);
			// Create a DataRecord and give it to the QueueAgent to send it to
			// the CluSandra messaging system.
			DataRecord dRecord = new DataRecord(payload);
			getQueueAgent().sendMessage(dRecord);
		}
		LOG.info("RbfGenerator: final send count = " + sampleCnt);
	}

	/**
	 * Set the options for this generator.
	 */
	public void setOptions() {
		getGenerator().numClassesOption = new IntOption("numClasses", 'c',
				"The number of classes to generate.", getClassCount());
		getGenerator().numAttsOption = new IntOption("numAtts", 'a',
				"The number of attributes to generate.", getAttributeCount());
	}
	
	public RandomRbfGenerator getGenerator(){
		return generator;
	}
	
	public static void main(String[] args){
		
		RbfGenerator stream = new RbfGenerator();
		stream.setAttributeCount(5);
		stream.setClassCount(10);
		stream.setOptions();
		stream.getGenerator().prepareForUse();
		stream.getGenerator().restart();
		Instance instance = stream.getGenerator().nextInstance();
		//instance.
		System.out.println("Attribute count = " + instance.numAttributes());
		System.out.println("Attribute to string = " + instance.toString());
		
	}

}
