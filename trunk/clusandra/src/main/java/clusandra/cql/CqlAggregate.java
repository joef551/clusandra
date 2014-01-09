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
 * $Date: 2011-11-14 21:31:35 -0500 (Mon, 14 Nov 2011) $
 * $Revision: 171 $
 * $Author: jose $
 * $Id: CqlAggregate.java 171 2011-11-15 02:31:35Z jose $
 */

package clusandra.cql;

import java.util.*;

import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyCluster;
import static clusandra.cql.CqlMain.cassyDao;
import static clusandra.cql.CqlSelect.EQUALS;
import static clusandra.cql.CqlSelect.START;
import static clusandra.cql.CqlSelect.END;
import static clusandra.cql.CqlSelect.checkForAnd;
import clusandra.clusterers.MicroCluster;
import clusandra.clusterers.KmeansClusterer;
import clusandra.utils.DateUtils;

/**
 * CluSandra Query Language (CQL) Aggregate
 * 
 */
public class CqlAggregate {

	// used for specifying the required percentage overlap.
	// the higher the percentage, the less microclusters
	// will be merged, and vice versa
	private static double overlapFactor = 0.01;
	private final static String OVERLAP = "overlap";
	// used for specifying the temporal radius of the
	// microcluster
	private static long clusterExpireTime = 10L;
	private final static String EXPIRE = "expire";

	// used for specifying the portion of the
	// data stream that will be aggregated.
	private static double startDateMills = -1.0;
	private static double endDateMills = -1.0;

	// an enum that defines the aggregate statement's fields
	public static enum AggregateField {
		START, END, OVERLAP, EXPIRE;
	}

	static void aggregate(ArrayList<String> tokens) {

		if (cassyCluster == null) {
			sessionState.out
					.println("you must first connect to a Cassandra cluster before "
							+ "issuing the " + "aggregate statement");
			return;
		} else if (cassyDao == null) {
			sessionState.out
					.println("you must first specify a Cassandra keyspace (via use statement)"
							+ " before issuing " + "the aggregate statement");
			return;
		}

		// reset all working variables
		startDateMills = -1.0;
		endDateMills = -1.0;
		overlapFactor = 0.01;
		clusterExpireTime = 10L;
		String token = null;

		// process the tokens for the aggregate statement
		while (!tokens.isEmpty()) {
			token = tokens.remove(0);
			if (START.equals(token)) {
				if (!processTokens(tokens, AggregateField.START)) {
					return;
				}
			} else if (END.equals(token)) {
				if (!processTokens(tokens, AggregateField.END)) {
					return;
				}
			} else if (OVERLAP.equals(token)) {
				if (!processTokens(tokens, AggregateField.OVERLAP)) {
					return;
				}
			} else if (EXPIRE.equals(token)) {
				if (!processTokens(tokens, AggregateField.EXPIRE)) {
					return;
				}
			} else {
				sessionState.out
						.println("invalid aggregate statement, unknown token = "
								+ token);
				return;
			}
			// skip over and validate the ands
			if (!checkForAnd(tokens)) {
				return;
			}
		}

		// if start and end horizon not specified, then pick
		// up the whole lot or portion of...
		if (endDateMills < 0 || startDateMills < 0) {
			boolean setEndDateMills = false;
			boolean setStartDateMills = false;
			if (endDateMills < 0) {
				endDateMills = Double.MIN_VALUE;
				setEndDateMills = true;
			}
			if (startDateMills < 0) {
				startDateMills = Double.MAX_VALUE;
				setStartDateMills = true;
			}
			List<MicroCluster> clusters = null;

			// retrieve all the clusters by way of the cluster recorder
			try {
				clusters = cassyDao.getClusters();
			} catch (Exception e) {
				sessionState.out
						.println("unable to get clusters, got this exception: "
								+ e.getMessage());
				return;
			}
			if (clusters == null || clusters.isEmpty()) {
				sessionState.out
						.println("did not retrieve any clusters from data store");
				return;
			}

			for (MicroCluster cluster : clusters) {
				// skip over super clusters
				if (!cluster.isSuper()) {
					if (setStartDateMills && cluster.getCT() < startDateMills) {
						startDateMills = cluster.getCT();
					}
					if (setEndDateMills && cluster.getLAT() > endDateMills) {
						endDateMills = cluster.getLAT();
					}
				}
			}
			if (endDateMills < startDateMills) {
				sessionState.out
						.println("end date must be greater than start date");
				return;
			}
		}

		sessionState.out.println("aggregation process started, please wait");
		KmeansClusterer clusterer = new KmeansClusterer();
		clusterer.setOverlapFactor(overlapFactor);
		clusterer.setClusterExpireTime(clusterExpireTime);
		clusterer.setClusandraDao(cassyDao);
		clusterer.setStartTime(DateUtils
				.getClusandraDate((long) startDateMills));
		clusterer.setEndTime(DateUtils.getClusandraDate((long) endDateMills));

		try {
			clusterer.runAggregator();
		} catch (Exception e) {
			sessionState.out
					.println("caught this exception when running aggregator = "
							+ e.getMessage());
		}
		sessionState.out.println("aggregation completed");
	}

	private static boolean processTokens(ArrayList<String> tokens,
			AggregateField field) {

		if (tokens.size() < 2) {
			sessionState.out.println("invalid date filter, insuffient tokens");
			return false;
		}

		String token = tokens.remove(0);
		if (!EQUALS.equals(token)) {
			sessionState.out.println("invalid date filter, expecting '=' got "
					+ token);
			return false;
		}

		// Get the next token
		token = tokens.remove(0);

		if (field == AggregateField.START || field == AggregateField.END) {
			long dateMills = 0L;
			try {
				dateMills = DateUtils.validateDate(token);
			} catch (IllegalArgumentException e) {
				sessionState.out.println("this date is not valid - " + token);
				return false;
			}
			if (field == AggregateField.START) {
				startDateMills = dateMills;
			} else {
				endDateMills = dateMills;
			}
		} else if (field == AggregateField.OVERLAP) {
			try {
				overlapFactor = Double.parseDouble(token);
				if (overlapFactor < 0.0) {
					sessionState.out.println("invalid overlap factor value of "
							+ overlapFactor);
					return false;
				}
			} catch (IllegalArgumentException e) {
				sessionState.out.println("this token is not valid - " + token);
				return false;
			}
		} else if (field == AggregateField.EXPIRE) {
			try {
				clusterExpireTime = Long.parseLong(token);
				if (clusterExpireTime == 0) {
					sessionState.out.println("invalid expire time value of "
							+ clusterExpireTime);
					return false;
				}
			} catch (IllegalArgumentException e) {
				sessionState.out.println("this token is not valid - " + token);
				return false;
			}
		}
		return true;
	}

}
