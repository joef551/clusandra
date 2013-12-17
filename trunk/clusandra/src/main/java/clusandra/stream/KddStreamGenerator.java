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
 * $Date: 2011-09-22 22:11:06 -0400 (Thu, 22 Sep 2011) $
 * $Revision: 155 $
 * $Author: jose $
 * $Id: KddStreamGenerator.java 155 2011-09-23 02:11:06Z jose $
 */
package clusandra.stream;

import java.util.Map;
import java.util.Scanner;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import clusandra.clusterers.DataRecord;
import clusandra.core.QueueAgent;

/**
 * This is a test StreamGenerator that works off the 1999 KDD Cup data set.
 * 
 * http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html.
 * 
 * This is a list of the different attacks and their types. . For example 'smurf
 * dos', means a smurf attack and it is a denial of service type of attack.
 * 
 * back dos, buffer_overflow u2r, ftp_write r2l, guess_passwd, r2l imap, r2l
 * ipsweep probe, land dos, loadmodule u2r, multihop r2l, neptune dos, nmap
 * probe, perl u2r, phf r2l, pod dos, portsweep probe, rootkit u2r, satan probe,
 * smurf dos, spy r2l, teardrop dos, warezclient r2l, warezmaster r2l
 * 
 * All the records are labeled (last field) with the name of attack that the
 * record is associated with. Records can also be labeled as 'normal'. There are
 * 4898431 records in the data set.
 * 
 * 
 * @author jfernandez
 * 
 */
public class KddStreamGenerator implements StreamGenerator {

	private static final Log LOG = LogFactory.getLog(KddStreamGenerator.class);

	// These are the data set's attributes. Only the continuous numerical
	// attributes are used.
	private static final String attributes = "duration,protocol_type,service,flag," // 0,1,2,3
			+ "src_bytes,dst_bytes,land,wrong_fragment," // 4,5,6,7
			+ "urgent,hot,num_failed_logins,logged_in," // 8,9,10,11
			+ "num_compromised,root_shell,su_attempted," // 12,13,14
			+ "num_root,num_file_creations,num_shells,num_access_files,"// 15,16,17,18
			+ "num_outbound_cmds,is_hot_login,is_guest_login,"// 19,20,21
			+ "count,srv_count,serror_rate,srv_serror_rate,"// 22,23,24,25
			+ "rerror_rate,srv_rerror_rate,same_srv_rate,"// 26,27,28
			+ "diff_srv_rate,srv_diff_host_rate,dst_host_count,"// 29,30,31
			+ "dst_host_srv_count,dst_host_same_srv_rate,"// 32,33
			+ "dst_host_diff_srv_rate,dst_host_same_src_port_rate,"// 34,35
			+ "dst_host_srv_diff_host_rate,dst_host_serror_rate,"// 36,37
			+ "dst_host_srv_serror_rate,dst_host_rerror_rate,"// 38,39
			+ "dst_host_srv_rerror_rate,class";// 40 - don't include class
	/*
	 * 
	 * The data sets attributes and types. This reader will only use the
	 * continuous attributes 0- duration: continuous. 1- protocol_type:
	 * symbolic. 2- service: symbolic. 3 -flag: symbolic. 4 src_bytes:
	 * continuous. 5 dst_bytes: continuous. 6 land: symbolic. 7 wrong_fragment:
	 * continuous. 8 urgent: continuous. 9 hot: continuous. 10
	 * num_failed_logins: continuous. 11 logged_in: symbolic. 12
	 * num_compromised: continuous. 13 root_shell: continuous. 14 su_attempted:
	 * continuous. 15 num_root: continuous. 16 num_file_creations: continuous.
	 * 17 num_shells: continuous. 18 num_access_files: continuous. 19
	 * num_outbound_cmds: continuous. 20 is_host_login: symbolic. 21
	 * is_guest_login: symbolic. 22 count: continuous. 23 srv_count: continuous.
	 * 24 serror_rate: continuous. 25 srv_serror_rate: continuous. 26
	 * rerror_rate: continuous. 27 srv_rerror_rate: continuous. 28
	 * same_srv_rate: continuous. 29 diff_srv_rate: continuous. 30
	 * srv_diff_host_rate: continuous. 31 dst_host_count: continuous. 32
	 * dst_host_srv_count: continuous. 33 dst_host_same_srv_rate: continuous. 34
	 * dst_host_diff_srv_rate: continuous. 35 dst_host_same_src_port_rate:
	 * continuous. 36 dst_host_srv_diff_host_rate: continuous. 37
	 * dst_host_serror_rate: continuous. 38 dst_host_srv_serror_rate:
	 * continuous. 39 dst_host_rerror_rate: continuous. 40
	 * dst_host_srv_rerror_rate: continuous. 41 label - symbolic
	 */

	private QueueAgent queueAgent;
	private String kddFileKey = "kddFileKey";
	private String kddFileName = null;

	private static final String dataSetSizeKey = "dataSetSize";

	private static final String COMMA = ",";
	private static final String SMURF = "smurf";
	private static final String NEPTUNE = "neptune";
	private static final String NORMAL = "normal";
	private static final String IPSWEEP = "ipsweep";
	private static final String PORTSWEEP = "portsweep";
	private static final String SATAN = "satan";

	// the default number of data records to generate
	private int dataSetSize = Integer.MAX_VALUE;

	/**
	 * Invoked by Spring to set the Map that contains configuration parameters
	 * for this StreamGenerator.
	 * 
	 * @param map
	 */
	public void setConfig(Map<String, String> map) throws Exception {
		for (String key : map.keySet()) {
			if (kddFileKey.equals(key)) {
				setKddFileName(map.get(key));
				LOG.trace("setConfig:kddFileName = " + getKddFileName());
			} else if (dataSetSizeKey.equals(key)) {
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
	 * Set the name (including path) of the kdd data set to use.
	 * 
	 * @param kddFileName
	 */
	public void setKddFileName(String kddFileName) {
		this.kddFileName = kddFileName;
	}

	/**
	 * Get the KDD data set file name
	 * 
	 * @return
	 */
	public String getKddFileName() {
		return kddFileName;
	}

	/**
	 * This method is invoked by the QueueAgent to start and give control to the
	 * StreamGenerator.
	 */
	public void startGenerator() throws Exception {
		double sampleCnt = 0L;
		String[] s1 = null;

		LOG.trace("Using this file = " + getKddFileName());

		// Open a Scanner for the file to read from
		Scanner scanner = new Scanner(new File(getKddFileName()));

		// start reading the records - there are only 33 continuous numerical
		// attributes that will be used
		long startTime = System.currentTimeMillis();
		long endTime = 0L;
		// while (scanner.hasNextLine()) {
		while (scanner.hasNextLine() && sampleCnt < dataSetSize) {

			// read a record and parse it, all the attributes are CSVs
			s1 = scanner.nextLine().split(COMMA);
			// first smurf occurs at line 77908 and runs until 114851
			// if (!s1[s1.length - 1].startsWith(IPSWEEP)) {
			// continue;
			// }
			if (!s1[s1.length - 1].startsWith(SMURF)) {
				// && !s1[s1.length - 1].startsWith(NEPTUNE))
				// && !s1[s1.length - 1].startsWith(NORMAL)) {
				continue;
			}

			// if (s1[s1.length - 1].startsWith(NORMAL)){
			// continue;
			// }

			double[] payload = new double[26];
			int j = 0;
			for (int i = 0; i < s1.length; i++) {
				// only use those attributes that are relevant
				if (i != 1 && i != 2 && i != 3 && i != 6 && i != 8 && i != 10
						&& i != 11 && i != 12 && i != 13 && i != 14 && i != 16
						&& i != 17 && i != 19 && i != 20 && i != 21 && i != 41) {
					payload[j++] = Double.parseDouble(s1[i]);
				}
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
		LOG.info("KddStreamGenerator: start time = " + startTime);
		LOG.info("KddStreamGenerator: end time = " + endTime);
		LOG.info("KddStreamGenerator: final count = " + sampleCnt);
		LOG.info("KddStreamGenerator: elapsed time  = " + elapsedTime);
		LOG.info("KddStreamGenerator: DataRecords per second = "
				+ (sampleCnt / elapsedTime));
	}
}
