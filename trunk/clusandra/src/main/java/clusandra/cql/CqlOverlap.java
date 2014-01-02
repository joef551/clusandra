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
 * $Id: CqlOverlap.java 144 2011-09-02 21:38:55Z jose $
 */
package clusandra.cql;

import java.util.*;

import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyDao;
import static clusandra.cql.CqlSelect.WHERE;
import static clusandra.cql.CqlSelect.RADIUS;
import static clusandra.cql.CqlSelect.START;
import static clusandra.cql.CqlSelect.N;
import static clusandra.cql.CqlSelect.processN;
import static clusandra.cql.CqlSelect.processRadius;
import static clusandra.cql.CqlSelect.checkForAnd;
import static clusandra.cql.CqlSelect.processDates;
import static clusandra.cql.CqlSelect.reset;
import static clusandra.cql.CqlSelect.markProject;
import static clusandra.cql.CqlSelect.startDate;
import static clusandra.cql.CqlSelect.endDate;
import clusandra.clusterers.MicroCluster;

/**
 * CluSandra Query Language (CQL) Overlap
 * 
 */
public class CqlOverlap {

	static void overlap(ArrayList<String> tokens) {

		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid overlap statement, insufficent tokens");
			return;
		}

		String token = tokens.get(0);

		if (!WHERE.equals(token)) {
			sessionState.out
					.println("invalid overlap statement, expecting WHERE got this "
							+ token);
			return;
		}
		tokens.remove(0);

		reset();

		// process remaining tokens
		while (!tokens.isEmpty()) {
			token = tokens.remove(0);
			if (RADIUS.equals(token)) {
				if (!processRadius(tokens)) {
					return;
				}
			} else if (N.equals(token)) {
				if (!processN(tokens)) {
					return;
				}
			} else if (START.equals(token)) {
				if (!processDates(tokens)) {
					return;
				}
			} else {
				break;
			}
			// skip over and validate the ands
			if (!checkForAnd(tokens)) {
				return;
			}
		}

		if (!tokens.isEmpty()) {
			sessionState.out
					.println("invalid overlap statement, unknown dangling tokens, starting with "
							+ tokens.get(0));
			return;
		}

		if (startDate != null && endDate == null) {
			endDate = startDate;
		}

		List<MicroCluster> clusters = null;
		try {
			if (startDate != null) {
				clusters = cassyDao.getClusters(startDate, endDate);
			} else {
				clusters = cassyDao.getClusters();
			}
		} catch (Exception e) {
			sessionState.out
					.println("unable to get clusters, got this exception: "
							+ e.getMessage());
			return;
		}
		if (clusters == null || clusters.isEmpty()) {
			sessionState.out.println("did not find any clusters");
			return;
		}

		// iterate thru the clusters and find those that can be included in this
		// operation
		markProject(clusters);
		int clusterCnt = clusters.size();
		for (int i = 0; i < clusterCnt; i++) {
			if (clusters.get(i).getProject()) {
				MicroCluster c1 = clusters.get(i);
				String c1ShortId = c1.getID().substring(0,
						c1.getID().indexOf('-'));
				for (int j = i + 1; j < clusterCnt; j++) {
					if (clusters.get(j).getProject()) {
						MicroCluster c2 = clusters.get(j);
						String c2ShortId = c2.getID().substring(0,
								c2.getID().indexOf('-'));
						double sumRadius = c1.getRadius() + c2.getRadius();
						double distance = c1.getCenterDistance(c2);
						if (sumRadius > distance) {
							Formatter formatter = new Formatter(new StringBuilder());
							formatter.format("%-7.3f", (((sumRadius - distance) / distance) * 100));
							sessionState.out
									.println(c1ShortId
											+ "[n="
											+ c1.getN()
											+ "] and "
											+ c2ShortId
											+ "[n="
											+ c2.getN()
											+ "] = "
											+ formatter.toString()
											+ "%");

						} else {
							sessionState.out.println(c1ShortId + "[n="
									+ c1.getN() + "] and " + c2ShortId + "[n="
									+ c2.getN() + "] = 0%");
						}
					}
				}
			}
		}
	}
}
