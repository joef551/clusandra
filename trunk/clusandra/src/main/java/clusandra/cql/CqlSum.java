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
 * $Id: CqlSum.java 140 2011-09-01 00:43:51Z jose $
 */

package clusandra.cql;

import java.util.*;

import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyDao;
import clusandra.clusterers.ClusandraKernel;

/**
 * CluSandra Query Language (CQL) Sum
 * 
 */
public class CqlSum {
	static void sum(ArrayList<String> tokens) {

		if (!tokens.isEmpty()) {
			sessionState.out
					.println("invalid sum statement, unknown dangling tokens, starting with "
							+ tokens.get(0));
			return;
		}
		
		List<ClusandraKernel> clusters = null;
		try {
			clusters = cassyDao.getClusters();
		} catch (Exception e) {
			sessionState.out
					.println("unable to get clusters, got this exception: "
							+ e.getMessage());
			return;
		}
		double sum = 0.0;
		for (ClusandraKernel cluster : clusters) {
			sum += cluster.getN();
		}
		sessionState.out.println("Total number of data records absorbed = "
				+ sum);
	}
}
