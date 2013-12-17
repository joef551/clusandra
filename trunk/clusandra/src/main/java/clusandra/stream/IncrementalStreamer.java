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

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import clusandra.clusterers.DataRecord;
import clusandra.core.QueueAgent;
import moa.streams.generators.AgrawalGenerator;
import moa.streams.generators.RandomRBFGenerator;
import moa.streams.generators.RandomTreeGenerator;
import moa.options.IntOption;
import moa.cluster.Clustering;
import moa.cluster.Cluster;
import moa.clusterers.clustream.Clustream;
import moa.clusterers.clustream.ClustreamKernel;
import weka.core.Instance;

/**
 * 
 * 
 * @author jfernandez
 * 
 */
public class IncrementalStreamer implements StreamGenerator {

	private static final Log LOG = LogFactory.getLog(IncrementalStreamer.class);
	private QueueAgent queueAgent;
	

	/**
	 * Invoked by Spring to set the Map that contains configuration parameters
	 * for this StreamGenerator.
	 * 
	 * @param map
	 */
	public void setConfig(Map<String, String> map) {
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
	 * StreamGenerator.
	 */
	public void startGenerator() throws Exception {

		double[] seed = new double[35];
		double seedMult = 0.0;
		int sampleCnt = 0;

		long startTime = System.currentTimeMillis();
		long endTime = 0L;
		
		for(int i = 0; i < seed.length; i++){
			seed[i]=1.0;
		}
		
		// while (scanner.hasNextLine()) {
		for (; sampleCnt < 20000; sampleCnt++) {
			double[] payload = Arrays.copyOf(seed, seed.length);
			if(sampleCnt%5000 == 0){
				++seedMult;
			}
			for(int i = 0; i < payload.length; i++){
				payload[i] *= seedMult;
			}
			DataRecord dRecord = new DataRecord(payload);
			getQueueAgent().sendMessage(dRecord);
		}
		endTime = System.currentTimeMillis();
		LOG.info("IncrementalStreamer: final count = " + sampleCnt);
		LOG.info("IncrementalStreamer: " + sampleCnt
				/ ((endTime - startTime) / 1000) + " msgs/second");
	}
	
	public void runAgrawal(){	
		AgrawalGenerator gen = new AgrawalGenerator();
		
		gen.functionOption = new IntOption("function", 'f',
				"Classification function used, as defined in the original paper.",
				2, 2, 10);
		gen.prepareForUse();
		gen.restart();
		for(int i = 0; i < 30; i++){
			
			Instance instance = gen.nextInstance();
			System.out.println(instance.toString());
			
		}
	}
	
	public void runRBF(){	
		RandomRBFGenerator gen = new RandomRBFGenerator();
		gen.numClassesOption = new IntOption("numClasses", 'c',
				"The number of classes to generate.", 6, 6, Integer.MAX_VALUE);
		gen.numCentroidsOption = new IntOption("numCentroids", 'n',
				"The number of centroids in the model.", 6, 1,
				Integer.MAX_VALUE);
		gen.prepareForUse();
		gen.restart();
		
		Clustream clstrm = new Clustream();
		clstrm.timeWindowOption  = new IntOption("timeWindow",
				't', "Range of the window.", 10000);
		
		clstrm.maxNumKernelsOption = new IntOption(
				"maxNumKernels", 'k',
				"Maximum number of micro kernels to use.", 100);
		clstrm.prepareForUse();
		clstrm.resetLearning();
		clstrm.resetLearningImpl();
		
		
		for(int i = 0; i < 100000; i++){
			clstrm.trainOnInstance(gen.nextInstance());
		}
		System.out.println("clustering result size = " + clstrm.getMicroClusteringResult().size());
		Clustering clstring = clstrm.getMicroClusteringResult();
		double weight = 0.0;
		for(int i = 0; i < 5000; i++){
			Cluster cluster = clstring.get(i);
			if(cluster == null){
				break;
			}
			weight += cluster.getWeight();
			System.out.println("clustering " + i + "'s N = " + cluster.getWeight() + " radius = " + ((ClustreamKernel)cluster).getRadius());
		}
		System.out.println("weight = " + weight); 
	}
	
	public void runRandomTree(){	
		RandomTreeGenerator gen = new RandomTreeGenerator();
		gen.prepareForUse();
		gen.restart();
		for(int i = 0; i < 30; i++){
			Instance instance = gen.nextInstance();
			System.out.println(instance.toString());
			
		}
	}
	
	
	public static void main (String[] args){
		 //new IncrementalStreamer().runAgrawal();
		 new IncrementalStreamer().runRBF();
	}

}
