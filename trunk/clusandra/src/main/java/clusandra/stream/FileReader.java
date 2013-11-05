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
 * $Date: $
 * $Revision: $
 * $Author: $
 * $Id: $
 */
package clusandra.stream;

import java.util.Map;
import java.io.BufferedReader;
import java.io.File;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import clusandra.core.QueueAgent;
import clusandra.core.DataRecord;

/**
 * This is a StreamGenerator that reads a series of multidimensional vectors
 * found in a file. It is assumed that the vectors comprise numerical/continuous
 * attributes and that any data munging is not required. Each vector represents
 * an occurrence in a point space.
 * 
 * This site includes sample cluster data sets:
 * 
 * http://cs.joensuu.fi/sipu/datasets/
 * 
 * 
 * @author jfernandez
 * 
 */
public class FileReader implements StreamGenerator {

	private static final Log LOG = LogFactory.getLog(FileReader.class);

	private QueueAgent queueAgent;
	private String fileKey = "fileKey";
	private String delimKey = "delimeterKey";
	private String fileName = null;
	private String delimiter = "\\s+";
	private Random random = new Random();

	/**
	 * Invoked by Spring to set the Map that contains configuration parameters
	 * for this StreamGenerator.
	 * 
	 * @param map
	 */
	public void setConfig(Map<String, String> map) throws Exception {
		for (String key : map.keySet()) {
			if (fileKey.equals(key)) {
				setFileName(map.get(key));
				LOG.trace("setConfig:FileName = " + getFileName());
			} else if (delimKey.equals(key)) {
				setDelimiter(map.get(key));
				LOG.trace("setConfig:Delimeter = " + getDelimiter());
			}
		}
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
	 * Set the name (including path) of the file to read.
	 * 
	 * @param fileName
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * Get the file name
	 * 
	 * @return
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * Set the delimiter for the input records.
	 * 
	 * @param delimeter
	 */
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	/**
	 * Get the delimiter being used for the input records.
	 * 
	 * @return String delimiter
	 */
	public String getDelimiter() {
		return this.delimiter;
	}

	/**
	 * This method is invoked by the QueueAgent to start and give control to the
	 * StreamGenerator.
	 */
	public void startGenerator() throws Exception {
		double sampleCnt = 0L;

		LOG.trace("Using this file = " + getFileName());

		BufferedReader reader = null;

		// Open a buffered reader to the file.
		try {
			reader = new BufferedReader(new java.io.FileReader(new File(
					getFileName())));
		} catch (Exception e) {
			System.out.println("ERROR: Unable to open configuration file\n");
			throw e;
		}

		// start reading the records
		long startTime = System.currentTimeMillis();
		long endTime = 0L;
		String line = null;
		int dimensions = -1;
		double[] location = null;
		while ((line = reader.readLine()) != null) {

			// ignore empty lines
			line = line.trim();
			if (line.length() == 0) {
				continue;
			}

			// Parse the tokens in this line
			String[] tokens = line.split(delimiter);
			if (tokens.length == 0) {
				throw new Exception("ERROR: no tokens present in line");
			}

			// if not already set, set the number of dimensions; else, ensure
			// all records have the same number of dimensions
			if (dimensions < 0) {
				dimensions = tokens.length;
			} else if (tokens.length != dimensions) {
				throw new Exception("encountered varying number of dimensions");
			}

			// create the array that represents the vector
			if (location == null) {
				location = new double[dimensions];
			}

			// read in the attributes/components for the vector
			for (int i = 0; i < dimensions; i++) {
				location[i] = Double.parseDouble(tokens[i].trim());
			}

			// Create a DataRecord and give it to the QueueAgent to send it to
			// the CluSandra messaging system. Note that the new instance of the
			// data record 'copies' the given record; this allows us to reuse it
			++sampleCnt;
			DataRecord dRecord = new DataRecord(location);
			// place the datarecord in the queue; the queue will automatically 
			// get flushed when it reaches it configurable capacity. 
			getQueueAgent().sendMessage(dRecord);

			// int rT = random.nextInt(5);
			// if(rT > 0){
			// Thread.sleep(1000 + (rT * 100));
			// }

		}
		// flush out any stragglers
		getQueueAgent().flush();
		endTime = System.currentTimeMillis();
		double elapsedTime = (endTime - startTime) / 1000.00;
		elapsedTime = (elapsedTime == 0.0) ? 1.0 : elapsedTime;
		LOG.info("startReader: start time = " + startTime);
		LOG.info("startReader: end time = " + endTime);
		LOG.info("startReader: final count = " + sampleCnt);
		LOG.info("startReader: elapsed time  = " + elapsedTime);
		LOG.info("startReader: DataRecords per second = "
				+ (sampleCnt / elapsedTime));
	}
}
