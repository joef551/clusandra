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
 * $Date: 2011-08-31 20:43:51 -0400 (Wed, 31 Aug 2011) $
 * $Revision: 140 $
 * $Author: jose $
 * $Id: CqlDistance.java 140 2011-09-01 00:43:51Z jose $
 */

package clusandra.cql;

import java.util.*;

import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyCluster;
import static clusandra.cql.CqlMain.cassyDao;
import static clusandra.cql.CqlSelect.COMMA;
import clusandra.clusterers.ClusandraKernel;

import clusandra.cassandra.ClusandraDao;

/**
 * CluSandra Query Language (CQL) Distance
 * 
 * distance id1, id2;
 */
public class CqlDistance {

	static void distance(ArrayList<String> tokens) {

		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid distance statement, insufficent tokens");
			return;
		}

		// pick up the first cluster id
		String clstr1ID = tokens.remove(0);
		
		// look for strings like 'id1,id2' and split them up
		// strings like "d1, d2", "d1 , d2", and "d1 ,d2" are 
		// taken care of...
		String[] buffer = clstr1ID.split(COMMA);
		if (buffer.length > 1) {
			for (int i = buffer.length - 1; i >= 0; i--) {
				if (buffer[i].length() > 0) {
					tokens.add(0, buffer[i]);
					if(i > 0){
						tokens.add(0, COMMA);
					}
				}
			}
			clstr1ID = tokens.remove(0);
		}
		
		
		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid distance statement, insufficent tokens");
			return;
		}
		// remove trailing commas and ignore commas
		if (clstr1ID.endsWith(COMMA)) {
			clstr1ID = clstr1ID.substring(0, clstr1ID.length() - 1);
		}

		// pick up the second cluster id
		String clstr2ID = tokens.remove(0);
		if (COMMA.equals(clstr2ID)) {
			if (tokens.isEmpty()) {
				sessionState.out
						.println("invalid distance statement, insufficent tokens");
				return;
			}
			clstr2ID = tokens.remove(0);
		} else if (clstr2ID.startsWith(COMMA)) {
			clstr2ID = clstr2ID.substring(1);
			if (clstr2ID.length() == 0) {
				sessionState.out
						.println("invalid distance statement, missing second cluster id");
				return;
			}
		}

		if (!tokens.isEmpty()) {
			sessionState.out
					.println("invalid distance statement, too many tokens");
			return;
		}

		ClusandraKernel c1 = null;
		ClusandraKernel c2 = null;
		try {
			c1 = cassyDao.getCluster(clstr1ID);
			c2 = cassyDao.getCluster(clstr2ID);
		} catch (Exception e) {
			sessionState.out
					.println("Unable to get cluster from Cassandra DAO. Exception = "
							+ e.getMessage());
		}
		if (c1 == null || c2 == null) {
			if (c1 == null) {
				sessionState.out
						.println("unable to retrieve cluster with id = "
								+ clstr1ID);
			}
			if (c2 == null) {
				sessionState.out
						.println("unable to retrieve cluster with id = "
								+ clstr2ID);
			}
		} else {
			sessionState.out.println("distance = " + c1.getCenterDistance(c2));
		}
	}
}
