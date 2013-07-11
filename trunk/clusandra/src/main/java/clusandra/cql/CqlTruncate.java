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
 * $Date: 2011-08-16 11:37:18 -0400 (Tue, 16 Aug 2011) $
 * $Revision: 105 $
 * $Author: jose $
 * $Id: CqlTruncate.java 105 2011-08-16 15:37:18Z jose $
 */

package clusandra.cql;

import java.util.*;
import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyCluster;
import static clusandra.cql.CqlMain.cassyDao;
import org.scale7.cassandra.pelops.ColumnFamilyManager;

/**
 * CluSandra Query Language (CQL) Truncate
 */
public class CqlTruncate {

	static void truncate(ArrayList<String> tokens) {

		if (tokens.size() < 1) {
			sessionState.out
					.println("invalid truncate statement, missing column family name");
			return;
		} else if (tokens.size() > 2) {
			sessionState.out
					.println("invalid truncate statement, too many parameters");
			return;
		} else if (cassyCluster == null) {
			sessionState.out
					.println("you must first connect to a cluster before issuing the truncate statement");
			return;
		} else if (cassyDao == null) {
			sessionState.out
					.println("you must first specify a keyspace (use statement) before issuing the truncate statement");
			return;
		}

		ColumnFamilyManager cfm = new ColumnFamilyManager(cassyCluster,
				cassyDao.getKeySpace());

		try {
			cfm.truncateColumnFamily(tokens.get(0));
		} catch (Exception e) {
			throw new IllegalStateException("Failed to truncate column family "
					+ tokens.get(0), e);
		}
	}

}
