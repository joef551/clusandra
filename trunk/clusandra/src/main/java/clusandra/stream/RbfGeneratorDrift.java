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
 * $Date: 2011-09-02 00:00:53 -0400 (Fri, 02 Sep 2011) $
 * $Revision: 143 $
 * $Author: jose $
 * $Id: RbfGeneratorDrift.java 143 2011-09-02 04:00:53Z jose $
 */
package clusandra.stream;

import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import clusandra.core.DataRecord;
import moa.streams.generators.RandomRBFGeneratorDrift;
import moa.options.IntOption;
import moa.options.FloatOption;
import weka.core.Instance;

/**
 * This stream reader/generator, which is used for testing and experimentation,
 * uses the MOA RandomRBFGeneratorDrift to generate a synthetic data stream with
 * drift.
 * 
 * @author jfernandez
 * 
 */
public class RbfGeneratorDrift extends RbfGenerator implements StreamGenerator {

	private static final Log LOG = LogFactory.getLog(RbfGeneratorDrift.class);

	private static final String driftSpeedKey = "driftSpeed";
	private static final String drifCentroidCountKey = "driftCentroidCount";

	private double driftSpeed = 0.0;
	private int driftCentroidCount = 50;

	/**
	 * Invoked by Spring to set the Map that contains configuration parameters
	 * for this StreamGenerator/Generator.
	 * 
	 * @param map
	 */
	public void setConfig(Map<String, String> map) throws Exception {
		super.setConfig(map);
		if (map == null || map.isEmpty()) {
			return;
		}
		for (String key : map.keySet()) {
			if (driftSpeedKey.equals(key)) {
				setDriftSpeed(map.get(key));
				LOG.trace("setConfig:driftSpeed = " + getDriftSpeed());
			} else if (drifCentroidCountKey.equals(key)) {
				setDriftCentroidCount(map.get(key));
				LOG.trace("setConfig:drifCentroidCount = "
						+ getDriftCentroidCount());
			}
		}
	}

	/**
	 * Called by setConfig() to set the number of centroids in the drift.
	 * 
	 * @param clusterDistanceFactor
	 * @throws Exception
	 */
	public void setDriftCentroidCount(String driftCentroidCount)
			throws Exception {
		setDriftCentroidCount(Integer.parseInt(driftCentroidCount));
	}

	public void setDriftCentroidCount(int driftCentroidCount)
			throws IllegalArgumentException {
		this.driftCentroidCount = driftCentroidCount;
	}

	/**
	 * Returns the number of centroids used for drift.
	 * 
	 * @return
	 */
	public int getDriftCentroidCount() {
		return driftCentroidCount;
	}

	/**
	 * Called by setConfig() to set the number of centroids in the drift.
	 * 
	 * @param clusterDistanceFactor
	 * @throws Exception
	 */
	public void setDriftSpeed(String driftSpeed) throws Exception {
		setDriftSpeed(Double.parseDouble(driftSpeed));
	}

	public void setDriftSpeed(double driftSpeed) {
		this.driftSpeed = driftSpeed;
	}

	/**
	 * Returns the number of centroids used for drift.
	 * 
	 * @return
	 */
	public double getDriftSpeed() {
		return driftSpeed;
	}

	/**
	 * This method is invoked by the QueueAgent to start and give control to the
	 * generator.
	 */
	public void startGenerator() {
		RandomRBFGeneratorDrift stream = new RandomRBFGeneratorDrift();
		setOptions(stream);
		stream.prepareForUse();
		stream.restart();
		generateStream(stream);
	}

	/**
	 * Set the options for this generator.
	 * 
	 * @param stream
	 */
	public void setOptions(RandomRBFGeneratorDrift stream) {
		super.setOptions(stream);
		stream.speedChangeOption = new FloatOption("speedChange", 's',
				"Speed of change of centroids in the model.", getDriftSpeed(),
				getDriftSpeed(), Float.MAX_VALUE);

		stream.numDriftCentroidsOption = new IntOption("numDriftCentroids", 'k',
				"The number of centroids with drift.", getDriftCentroidCount(),
				0, Integer.MAX_VALUE);
	}

}
