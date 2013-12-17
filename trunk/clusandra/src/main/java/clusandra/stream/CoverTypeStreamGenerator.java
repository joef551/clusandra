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
 * $Date: 2011-09-02 17:38:55 -0400 (Fri, 02 Sep 2011) $
 * $Revision: 144 $
 * $Author: jose $
 * $Id: KddStreamGenerator.java 144 2011-09-02 21:38:55Z jose $
 */
package clusandra.stream;

import java.util.Map;
import java.util.Scanner;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import clusandra.clusterers.DataRecord;
import clusandra.core.QueueAgent;
import clusandra.core.AbstractProcessor;

/**
 * This is a test StreamGenerator that works off the Cover Type data set acquired
 * from the UCI Machine Learning Repository.
 * 
 * http://archive.ics.uci.edu/ml/datasets/Covertype
 * 
 * 
 * @author jfernandez
 * 
 */
public class CoverTypeStreamGenerator extends AbstractProcessor {

	private static final Log LOG = LogFactory
			.getLog(CoverTypeStreamGenerator.class);

	private QueueAgent queueAgent;
	private String fileKey = "fileKey";
	private String fileName = null;

	private static final String COMMA = ",";

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
			}
		}
	}



	/**
	 * Set the name (including path) of the file to use.
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
	 * This method is invoked by the QueueAgent to start and give control to the
	 * StreamGenerator.
	 */
	public void produceCluMessages() throws Exception {
		double sampleCnt = 0L;
		String[] s1 = null;

		LOG.trace("Using this file = " + getFileName());

		// Open a Scanner for the file to read from
		Scanner scanner = new Scanner(new File(getFileName()));

		// start reading the records - there are only 33 continuous numerical
		// attributes that will be used
		long startTime = System.currentTimeMillis();
		long endTime = 0L;
		// while (scanner.hasNextLine()) {
		while (scanner.hasNextLine()) {
			// read a record and parse it, all the attributes are CSVs
			s1 = scanner.nextLine().split(COMMA);
			double[] payload = new double[10];
			for (int i = 0; i < payload.length; i++) {
				payload[i] = Double.parseDouble(s1[i]);
			}
			// Create a DataRecord and give it to the QueueAgent to send it to
			// the CluSandra messaging system.
			++sampleCnt;
			DataRecord dRecord = new DataRecord(payload);
			getQueueAgent().sendMessage(dRecord);
		}
		// send out any stragglers
		getQueueAgent().flush();
		endTime = System.currentTimeMillis();
		double elapsedTime = (endTime - startTime) / 1000.00;
		elapsedTime = (elapsedTime == 0.0) ? 1.0 : elapsedTime;
		LOG.info("CoverTypeStreamGenerator: start time = " + startTime);
		LOG.info("CoverTypeStreamGenerator: end time = " + endTime);
		LOG.info("CoverTypeStreamGenerator: final count = " + sampleCnt);
		LOG.info("CoverTypeStreamGenerator: elapsed time  = " + elapsedTime);
		LOG.info("CoverTypeStreamGenerator: DataRecords per second = "
				+ (sampleCnt / elapsedTime));
	}
}
