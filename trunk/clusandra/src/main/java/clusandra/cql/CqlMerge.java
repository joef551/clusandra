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
 * $Date: 2011-11-15 17:05:29 -0500 (Tue, 15 Nov 2011) $
 * $Revision: 173 $
 * $Author: jose $
 * $Id: CqlMerge.java 173 2011-11-15 22:05:29Z jose $
 */

package clusandra.cql;

import java.util.*;

import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyCluster;
import static clusandra.cql.CqlMain.cassyDao;
import static clusandra.cql.CqlSelect.COMMA;
import clusandra.clusterers.MicroCluster;

import clusandra.cassandra.ClusandraDao;

/**
 * CluSandra Query Language (CQL) Merge
 * 
 * This CQL statement is used for merging two or more clusters into a new
 * supercluster. The list of clusters that are provided can be either 
 * microclusters or other superclusters. Microclusters are preserved, but 
 * superclusters are removed from the cluster data store. 
 * 
 * merge id1, id2, ..., idn;
 */
public class CqlMerge {

	static void merge(ArrayList<String> tokens) {

		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid merge statement, insufficent tokens");
			return;
		}

		// place the ids into a list; keeping in mind the tokens and their 
		// possible formats. 
		ArrayList<String> clusterids = new ArrayList<String>();
		while (!tokens.isEmpty()) {
			String[] buffer = tokens.remove(0).split(COMMA);
			if (buffer.length >= 1) {
				for (String s1 : buffer) {
					s1 = s1.trim();
					if (s1.length() > 0) {
						clusterids.add(s1);
					}
				}
			}
		}

		if (clusterids.size() < 2) {
			sessionState.out
					.println("invalid merge statement, insufficent ids");
			return;
		}

		MicroCluster c1 = null;
		MicroCluster c2 = null;
		// begin the merging process
		for (String id : clusterids) {
			try {
				c1 = cassyDao.getCluster(id);
				if (c1 == null) {
					sessionState.out.println("Unable to get cluster " + id
							+ " from Cassandra DAO");
					return;
				}
			} catch (Exception e) {
				sessionState.out.println("Unable to get cluster " + id
						+ " from Cassandra DAO. Exception = " + e.getMessage());
				return;
			}
			if (c2 == null) {
				// if this was the first cluster, then assign it to c2 which 
				// will host all the merging. 
				c2 = new MicroCluster(c1);
			} else if (c1.isSuper()) {
				// the just acquired cluster is a super cluster, so merge all of
				// its microclusters with the ongoing super cluster
				for (byte[] id2 : c1.getIDLIST()) {
					MicroCluster cluster = null;
					try {
						cluster = cassyDao.getCluster(id2);
					} catch (Exception e) {
						sessionState.out
								.println("got this exception when trying to get cluster with id = "
										+ new String(id2));
						return;
					}
					// then, add it to the c2 supercluster
					try {
						c2.add(cluster);
					} catch (Exception e) {
						sessionState.out
								.println("got this exception when trying to add cluster with id = "
										+ cluster.getID());
						return;
					}
				}
			} else {
				// the just acquired cluster is a microcluster
				try {
					c2.add(c1);
				} catch (Exception e) {
					sessionState.out
							.println("got this exception when trying to add cluster with id = "
									+ c1.getID());
					return;
				}
			}
		}

		// save the resulting super cluster out to the database.
		if(c2 == null){
			sessionState.out.println("error: merging did not take place");
		} else try {
			cassyDao.writeClusandraKernel(c2);
		} catch (Exception e) {
			sessionState.out
					.println("got this exception when trying to write super cluster with id = "
							+ c2.getID());
		}

	}
}
