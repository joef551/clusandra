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
 * $Date: 2011-08-25 22:48:13 -0400 (Thu, 25 Aug 2011) $
 * $Revision: 133 $
 * $Author: jose $
 * $Id: RbfGenerator.java 133 2011-08-26 02:48:13Z jose $
 */
package clusandra.stream;

import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import clusandra.core.DataRecord;
import clusandra.core.AbstractProcessor;


/**
 * This stream generator, which is used for testing and experimentation, uses
 * the MOA AgrawalGenerator to generate a synthetic data stream.
 * 
 * @author jfernandez
 * 
 */
public class AgrawalGenerator extends AbstractProcessor {

	private static final Log LOG = LogFactory.getLog(AgrawalGenerator.class);

	private static final String dataSetSizeKey = "dataSetSize";
	

	// the default number of data records to generate
	private int dataSetSize = 100;
	

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
			} 
		}
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

	public void setDataSetSize(int dataSetSize) throws IllegalArgumentException {
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
	 * This method is invoked by the QueueAgent to start and give control to the
	 * generator.
	 */
	public void produceCluMessages() throws Exception {
		moa.streams.generators.AgrawalGenerator gen = new moa.streams.generators.AgrawalGenerator();
		gen.prepareForUse();
		gen.restart();

		// generate the stream records
		
		LOG.info("AgrawalGenerator: dataSetSize = " + dataSetSize);
		int sampleCnt = 0;
		for (int i = 0; i < dataSetSize; i++,sampleCnt++) {
			double[] payload = new double[6];
			double [] instance = gen.nextInstance().toDoubleArray();
			// skip the discrete or categorical attributes
			payload[0] = instance[0];
			payload[1] = instance[1];
			payload[2] = instance[2];
			payload[3] = instance[6];
			payload[4] = instance[7];
			payload[5] = instance[8];
			DataRecord dRecord = new DataRecord(payload);
			getQueueAgent().sendMessage(dRecord);
		}
		LOG.info("AgrawalGenerator: final send count = " + sampleCnt);
	}

}
