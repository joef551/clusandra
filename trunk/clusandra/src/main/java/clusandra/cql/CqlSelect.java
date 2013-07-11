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
 * $Id: CqlSelect.java 173 2011-11-15 22:05:29Z jose $
 */
package clusandra.cql;

import java.util.*;

import clusandra.utils.DateUtils;
import clusandra.clusterers.ClusandraKernel;

import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyCluster;
import static clusandra.cql.CqlMain.cassyDao;

import static clusandra.clusterers.ClusandraKernel.SortField;
import static clusandra.clusterers.ClusandraKernel.SortOrder;

/**
 * CluSandra Query Language (CQL) Select
 */
public class CqlSelect {

	static final String ASTERISTIC = "*";
	static final String WHERE = "where";
	static final String SORT = "sort";
	static final String DESC = "desc";
	static final String BY = "by";
	static final String AND = "and";
	static final String START = "start";
	static final String END = "end";
	static final String N = "n";
	static final String CT = "ct";
	static final String ID = "id";
	static final String LAT = "lat";
	static final String RADIUS = "radius";
	static final String TYPE = "type";
	static final String COUNT = "count";
	static final String COMMA = ",";
	static final String EQUALS = "=";
	static final String LESS_THAN = "<";
	static final String GREATER_THAN = ">";
	static final String LESS_THAN_EQUAL = "<=";
	static final String GREATER_THAN_EQUAL = ">=";
	static final String MICROCLUSTER = "micro";
	static final String SUPERCLUSTER = "super";

	private static final String RADIUS_FMT = "%-10.3f";
	private static final String N_FMT = "%-10.0f";
	private static final String ID_FMT = "%-37s";
	private static final String TYPE_FMT = "%-6s";
	private static final String CT_FMT = "%1$tb %1$td, %1$tY %1$tT ";
	private static final String LAT_FMT = "%1$tb %1$td, %1$tY %1$tT ";

	// This map contains the print formatting for the valid projection fields
	private static final Map<String, String> fmtMap;
	private static SortOrder sortOrder = SortOrder.ASCENDING;

	static {
		Map<String, String> m = new HashMap<String, String>();
		m.put(RADIUS, RADIUS_FMT);
		m.put(N, N_FMT);
		m.put(ID, ID_FMT);
		m.put(CT, CT_FMT);
		m.put(LAT, LAT_FMT);
		m.put(TYPE, TYPE_FMT);
		fmtMap = Collections.unmodifiableMap(m);
	}

	// the formats for the field headers
	private static final String N_HDR_FMT = "%-10s";
	private static final String RADIUS_HDR_FMT = "%-10s";
	private static final String ID_HDR_FMT = "%-37s";
	private static final String CT_HDR_FMT = "%-22s";
	private static final String LAT_HDR_FMT = "%-22s";
	private static final String TYPE_HDR_FORMAT = "%-6s";

	// This map contains the print formatting for the headers of valid
	// projection fields
	private static final Map<String, String> fmtHdrMap;
	static {
		Map<String, String> m = new HashMap<String, String>();
		m.put(RADIUS, RADIUS_HDR_FMT);
		m.put(N, N_HDR_FMT);
		m.put(ID, ID_HDR_FMT);
		m.put(CT, CT_HDR_FMT);
		m.put(LAT, LAT_HDR_FMT);
		m.put(TYPE, TYPE_HDR_FORMAT);
		fmtHdrMap = Collections.unmodifiableMap(m);
	}

	// This list contains all fields to display when '*' is selected
	private static final List<String> allFields2Project;
	static {
		List<String> l = new ArrayList<String>();
		l.add(ID);
		l.add(RADIUS);
		l.add(N);
		l.add(CT);
		l.add(LAT);
		l.add(TYPE);
		allFields2Project = Collections.unmodifiableList(l);
	}

	// Specifies how to sort the clusters
	private static SortField sortField = null;

	@SuppressWarnings("unused")
	static double radiusEquals = -1.0;
	@SuppressWarnings("unused")
	static double radiusLessThan = -1.0;
	@SuppressWarnings("unused")
	static double radiusGreaterThan = -1.0;
	@SuppressWarnings("unused")
	static double radiusGreaterThanEqual = -1.0;
	@SuppressWarnings("unused")
	static double radiusLessThanEqual = -1.0;

	@SuppressWarnings("unused")
	static double nEquals = -1.0;
	@SuppressWarnings("unused")
	static double nLessThan = -1.0;
	@SuppressWarnings("unused")
	static double nGreaterThan = -1.0;
	@SuppressWarnings("unused")
	static double nGreaterThanEqual = -1.0;
	@SuppressWarnings("unused")
	static double nLessThanEqual = -1.0;

	public static enum TypeField {
		MICRO, SUPER;
	}

	static TypeField typeField = null;
	static String startDate = null;
	static String endDate = null;
	static boolean count = false;

	private static List<String> fields2Display = null;

	static void select(ArrayList<String> tokens) {

		if (tokens.size() < 1) {
			sessionState.out.println("invalid select statement, no expression");
			return;
		} else if (cassyCluster == null) {
			sessionState.out
					.println("you must first connect to a Cassandra cluster before "
							+ "issuing the " + "select statement");
			return;
		} else if (cassyDao == null) {
			sessionState.out
					.println("you must first specify a Cassandra keyspace (via use statement)"
							+ " before issuing " + "the select statement");
			return;
		}

		reset();

		// collect the fields that user wants projected
		if (!collectFields2Display(tokens)) {
			return;
		}

		String token = null;

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
			} else if (TYPE.equals(token)) {
				if (!processType(tokens)) {
					return;
				}
			} else if (ID.equals(token)) {
				if (!processID(tokens)) {
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

		// see if there is a 'sort by' at the end of the select statement
		if (!tokens.isEmpty() && SORT.equals(token)) {
			if (!processSortBy(tokens)) {
				return;
			}
		}

		if (!tokens.isEmpty()) {
			sessionState.out
					.println("invalid select statement, unknown dangling tokens");
			return;
		}

		if (startDate != null && endDate == null) {
			endDate = startDate;
		}

		List<ClusandraKernel> clusters = null;
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

		// iterate thru the clusters and find those that can be projected
		markProject(clusters);

		if (sortField != null) {
			for (ClusandraKernel cluster : clusters) {
				cluster.setSortField(sortField);
				cluster.setSortOrder(sortOrder);
			}
			Collections.sort(clusters);
		}

		if (!count) {
			printHeader();
		}
		printRows(clusters);
	}

	private static boolean collectFields2Display(ArrayList<String> tokens) {
		fields2Display = new ArrayList<String>();
		boolean quit = false;
		while (!tokens.isEmpty() && !quit) {

			// pick up next token
			String token = tokens.remove(0);

			// remove trailing commas and ignore commas
			if (token.endsWith(COMMA)) {
				if (!tokens.isEmpty() && WHERE.equals(tokens.get(0))) {
					sessionState.out
							.println("invalid select syntax, dangling comma");
					return false;
				}
				token = token.substring(0, token.length() - 1);
			} else if (COMMA.equals(token)) {
				continue;
			}

			// look for lists like 'a,b,c' and split them up
			String[] buffer = token.split(COMMA);
			if (buffer.length > 1) {
				for (int i = buffer.length - 1; i >= 0; i--) {
					if (buffer[i].length() > 0) {
						tokens.add(0, buffer[i]);
					}
				}
				token = tokens.remove(0);
			}

			// ensure it is a valid field
			if (!ASTERISTIC.equals(token) && !WHERE.equals(token)
					&& !N.equals(token) && !CT.equals(token)
					&& !LAT.equals(token) && !RADIUS.equals(token)
					&& !ID.equals(token) && !COUNT.equals(token)
					&& !TYPE.equals(token)) {
				sessionState.out.println("invalid select project field - "
						+ token);
				return false;
			}

			// '*' and 'count' must be only fields to be projected
			if (ASTERISTIC.equals(token)) {
				if (!fields2Display.isEmpty()) {
					sessionState.out
							.println("invalid select, '*' cannot be accompanied by other fields"
									+ token);
					return false;
				}
				// check for 'select *' or 'select * sort by ...';
				if (tokens.isEmpty() || SORT.equals(tokens.get(0))) {
					fields2Display.add(token);
					return true;
				}
				if ((!tokens.isEmpty() && !WHERE.equals(tokens.get(0)))) {
					sessionState.out
							.println("invalid select, '*' cannot be accompanied by other fields"
									+ token);
					return false;
				}

			} else if (COUNT.equals(token)) {
				if (!fields2Display.isEmpty()
						|| (!tokens.isEmpty() && !WHERE.equals(tokens.get(0)))) {
					sessionState.out
							.println("invalid select, 'count' cannot be accompanied by other fields"
									+ token);
					return false;
				}
			}

			if (COUNT.equals(token)) {
				count = true;
			} else if (WHERE.equals(token)) {
				// quit when we've reached WHERE key word; there is no FROM
				quit = true;
			} else {
				fields2Display.add(token);
			}
		}
		if (!count && fields2Display.isEmpty()) {
			sessionState.out.println("you must select fields to project\n");
			return false;
		}
		return true;
	}

	static boolean processRadius(ArrayList<String> tokens) {

		if (tokens.size() < 2) {
			sessionState.out
					.println("invalid radius filter, insuffient tokens");
			return false;
		}

		String conditionToken = tokens.remove(0);
		// We expect '=' '>' '<' '<=' '>='
		if (!checkForConditional(conditionToken)) {
			sessionState.out
					.println("invalid radius filter, expecting condition got - "
							+ conditionToken);
			return false;
		}

		double radius = -1.0;
		String token = tokens.remove(0);
		try {
			radius = Double.parseDouble(token);
		} catch (Exception e) {
			sessionState.out.println("invalid value for radius - " + token);
			return false;
		}
		// if it was radius = <some number>, then we're done
		if (EQUALS.equals(conditionToken)) {
			radiusEquals = radius;
			return true;
		} else if (GREATER_THAN.equals(conditionToken)) {
			radiusGreaterThan = radius;
		} else if (GREATER_THAN_EQUAL.equals(conditionToken)) {
			radiusGreaterThanEqual = radius;
		} else if (LESS_THAN.equals(conditionToken)) {
			radiusLessThan = radius;
			return true;
		}

		// we're done if there are no more tokens or the next token is not 'and'
		if (tokens.isEmpty() || !AND.equals(tokens.get(0))) {
			return true;
		}

		// the next token is 'and', but is the one after that 'radius'?
		if (tokens.size() < 2) {
			sessionState.out
					.println("invalid radius filter, insuffient tokens");
		} else if (!RADIUS.equals(tokens.get(1))) {
			return true;
		}

		// remove the 'and'
		tokens.remove(0);

		// expecting more for radius
		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid radius filter, nothing follows 'and'");
			return false;
		}

		// the next token must be 'radius'
		token = tokens.remove(0);
		if (!RADIUS.equals(token)) {
			sessionState.out
					.println("invalid radius filter, expect token is 'radius',"
							+ " but got this instead - " + token);
			return false;
		}

		// else expecting more
		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid radius filter, nothing follows 'radius'");
			return false;
		}

		// now get the conditional
		conditionToken = tokens.remove(0);

		// else expecting more
		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid radius filter, nothing follows condition");
			return false;
		}

		// now we expect a numerical value
		radius = -1.0;
		token = tokens.remove(0);
		try {
			radius = Double.parseDouble(token);
		} catch (Exception e) {
			sessionState.out.println("invalid value for radius - " + token);
			return false;
		}

		// We expect '=' '>' '<' '<=' '>='
		if (!checkForConditional(conditionToken)) {
			sessionState.out.println("invalid radius conditon field - "
					+ conditionToken);
			return false;
		}

		if (LESS_THAN.equals(conditionToken)) {
			radiusLessThan = radius;
		} else if (LESS_THAN_EQUAL.equals(conditionToken)) {
			radiusLessThanEqual = radius;
		} else {
			sessionState.out.println("invalid select project field - "
					+ conditionToken);
			return false;
		}
		return true;
	}

	static boolean processN(ArrayList<String> tokens) {

		if (tokens.size() < 2) {
			sessionState.out.println("invalid N filter, insuffient tokens");
			return false;
		}

		String conditionToken = tokens.remove(0);
		// We expect '=' '>' '<' '<=' '>='
		if (!checkForConditional(conditionToken)) {
			sessionState.out
					.println("invalid N filter, expecting condition got - "
							+ conditionToken);
			return false;
		}

		double nVar = -1.0;
		String token = tokens.remove(0);
		try {
			nVar = Double.parseDouble(token);
		} catch (Exception e) {
			sessionState.out.println("invalid value for N - " + token);
			return false;
		}
		// if it was N = <some number>, then we're done
		if (EQUALS.equals(conditionToken)) {
			nEquals = nVar;
			return true;
		} else if (GREATER_THAN.equals(conditionToken)) {
			nGreaterThan = nVar;
		} else if (GREATER_THAN_EQUAL.equals(conditionToken)) {
			nGreaterThanEqual = nVar;
		} else if (LESS_THAN.equals(conditionToken)) {
			nLessThan = nVar;
			return true;
		}

		// we're done if there are no more tokens or the next token is not 'and'
		if (tokens.isEmpty() || !AND.equals(tokens.get(0))) {
			return true;
		}
		// the next token is 'and', but is the one after that 'N'?
		if (tokens.size() < 2) {
			sessionState.out.println("invalid N filter, insuffient tokens");
		} else if (!N.equals(tokens.get(1))) {
			return true;
		}

		// remove the 'and'
		tokens.remove(0);

		// else expecting more
		if (tokens.isEmpty()) {
			sessionState.out.println("invalid N filter, nothing follows 'and'");
			return false;
		}

		// the next token must be 'radius'
		token = tokens.remove(0);
		if (!N.equals(token)) {
			sessionState.out
					.println("invalid N filter, expect token is 'N', but got"
							+ " this instead - " + token);
			return false;
		}

		// else expecting more
		if (tokens.isEmpty()) {
			sessionState.out.println("invalid N filter, nothing follows 'N'");
			return false;
		}

		// now get the conditional
		conditionToken = tokens.remove(0);

		// else expecting more
		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid N filter, nothing follows condition");
			return false;
		}

		// now we expect a numerical value
		nVar = -1.0;
		token = tokens.remove(0);
		try {
			nVar = Double.parseDouble(token);
		} catch (Exception e) {
			sessionState.out.println("invalid value for N - " + token);
			return false;
		}

		// We expect '=' '>' '<' '<=' '>='
		if (!checkForConditional(conditionToken)) {
			sessionState.out.println("invalid N conditon field - "
					+ conditionToken);
			return false;
		}

		if (LESS_THAN.equals(conditionToken)) {
			nLessThan = nVar;
		} else if (LESS_THAN_EQUAL.equals(conditionToken)) {
			nLessThanEqual = nVar;
		} else {
			sessionState.out.println("invalid select project field - "
					+ conditionToken);
			return false;
		}
		return true;
	}

	static boolean processDates(ArrayList<String> tokens) {

		if (tokens.size() < 2) {
			sessionState.out.println("invalid date filter, insuffient tokens");
			return false;
		}

		String assignmentToken = tokens.remove(0);
		// We expect '='
		if (!EQUALS.equals(assignmentToken)) {
			sessionState.out.println("invalid date filter, expecting '=' got "
					+ assignmentToken);
			return false;
		}

		// Get the start date
		String token = tokens.remove(0);
		try {
			DateUtils.validateDate(token);
		} catch (IllegalArgumentException e) {
			sessionState.out.println("this date is not valid - " + token);
			return false;
		}
		startDate = token;

		// we're done if there are no more tokens or the next token is not 'and'
		if (tokens.size() == 0 || !AND.equals(tokens.get(0))) {
			return true;
		}

		// the next token is 'and', but is the one after that 'end'?
		if (tokens.size() < 2) {
			sessionState.out.println("invalid date filter, insuffient tokens");
		} else if (!END.equals(tokens.get(1))) {
			return true;
		}

		// remove the 'and'
		tokens.remove(0);

		// else expecting more
		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid date filter, nothing follows 'and'");
			return false;
		}

		// the next token must be 'end'
		token = tokens.remove(0);
		if (!END.equals(token)) {
			sessionState.out
					.println("invalid date filter, expecting 'end' token,"
							+ " but got this instead - " + token);
			return false;
		}

		// else expecting more
		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid date filter, nothing follows 'end'");
			return false;
		}

		// now get the assignment
		assignmentToken = tokens.remove(0);
		if (!EQUALS.equals(assignmentToken)) {
			sessionState.out.println("invalid date filter, expecting '=' got "
					+ assignmentToken);
			return false;
		}

		// else expecting more
		if (tokens.isEmpty()) {
			sessionState.out
					.println("invalid date filter, nothing follows '='");
			return false;
		}

		// Get the end date
		token = tokens.remove(0);
		try {
			DateUtils.validateDate(token);
		} catch (IllegalArgumentException e) {
			sessionState.out.println("this date is not valid - " + token);
			return false;
		}
		endDate = token;
		return true;
	}

	private static boolean processSortBy(ArrayList<String> tokens) {

		if (tokens.size() < 2) {
			sessionState.out.println("invalid 'sort by', insuffient tokens");
			return false;
		} else if (tokens.size() > 3) {
			sessionState.out.println("invalid 'sort by', too many tokens");
			return false;
		}

		String token = tokens.remove(0);
		if (!BY.equals(token)) {
			sessionState.out.println("invalid 'sort by', invalid token = "
					+ token);
			return false;
		}
		token = tokens.remove(0);

		if (RADIUS.equals(token)) {
			sortField = SortField.RADIUS;
		} else if (ID.equals(token)) {
			sortField = SortField.ID;
		} else if (N.equals(token)) {
			sortField = SortField.N;
		} else if (CT.equals(token)) {
			sortField = SortField.CT;
		} else if (LAT.equals(token)) {
			sortField = SortField.LAT;
		} else {
			sessionState.out
					.println("invalid 'sort by', unknown sort field of "
							+ token);
			return false;
		}

		// see if user wants sort in descending order
		if (!tokens.isEmpty() && DESC.equals(tokens.get(0))) {
			sortOrder = SortOrder.DESCENDING;
			tokens.remove(0);
		}
		return true;
	}

	// process the type select filter. for example, select * where type = micro
	static boolean processType(ArrayList<String> tokens) {

		if (tokens.size() < 2) {
			sessionState.out.println("invalid type filter, insuffient tokens");
			return false;
		}

		String token = tokens.remove(0);
		// We expect '='
		if (!EQUALS.equals(token)) {
			sessionState.out.println("invalid type filter, "
					+ "expecting '=' got " + token);
			return false;
		}

		// the next token must be either micro or super
		token = tokens.remove(0);
		if (MICROCLUSTER.equalsIgnoreCase(token)) {
			typeField = TypeField.MICRO;
		} else if (SUPERCLUSTER.equalsIgnoreCase(token)) {
			typeField = TypeField.SUPER;
		} else {
			sessionState.out.println("invalid type filter, expecting "
					+ "'micro' or 'super', got " + token);
			return false;
		}
		return true;
	}

	private static boolean processID(ArrayList<String> tokens) {
		return true;
	}

	static void reset() {
		fields2Display = null;
		radiusEquals = -1.0;
		radiusLessThan = -1.0;
		radiusGreaterThan = -1.0;
		radiusGreaterThanEqual = -1.0;
		radiusLessThanEqual = -1.0;
		nEquals = -1.0;
		nLessThan = -1.0;
		nGreaterThan = -1.0;
		nGreaterThanEqual = -1.0;
		nLessThanEqual = -1.0;
		startDate = null;
		endDate = null;
		sortField = null;
		typeField = null;
		sortOrder = SortOrder.ASCENDING;
		count = false;
	}

	static boolean checkForAnd(ArrayList<String> tokens) {
		if (!tokens.isEmpty() && AND.equalsIgnoreCase(tokens.get(0))) {
			tokens.remove(0);
			if (tokens.isEmpty()) {
				sessionState.out.println("invalid statement, dangling 'and'");
				return false;
			}
		}
		return true;
	}

	// iterate through the retrieved clusters and determine which will be
	// projected based on contents of where clause (if any)
	static void markProject(List<ClusandraKernel> clusters) {

		// check for N
		if (nEquals > 0) {
			for (ClusandraKernel cluster : clusters) {
				if (cluster.getN() != nEquals) {
					cluster.setProject(false);
				}
			}
		} else if (nGreaterThan > 0 || nGreaterThanEqual > 0) {
			for (ClusandraKernel cluster : clusters) {
				if (nGreaterThan > 0) {
					if (cluster.getN() <= nGreaterThan) {
						cluster.setProject(false);
						continue;
					}
				} else if (cluster.getN() < nGreaterThanEqual) {
					cluster.setProject(false);
					continue;
				}
				if ((nLessThan > 0 && cluster.getN() >= nLessThan)
						|| (nLessThanEqual > 0 && cluster.getN() > nLessThanEqual)) {
					cluster.setProject(false);
				}
			}
		} else if (nLessThan > 0) {
			// System.out.println("nLessThan = " + nLessThan);
			for (ClusandraKernel cluster : clusters) {
				if (cluster.getN() >= nLessThan) {
					cluster.setProject(false);
				}
			}
		}

		// check for radius
		if (radiusEquals > 0) {
			for (ClusandraKernel cluster : clusters) {
				if (cluster.getRadius() != radiusEquals) {
					cluster.setProject(false);
				}
			}
		} else if (radiusGreaterThan > 0 || radiusGreaterThanEqual > 0) {
			for (ClusandraKernel cluster : clusters) {
				if (radiusGreaterThan > 0) {
					if (cluster.getRadius() <= radiusGreaterThan) {
						cluster.setProject(false);
						continue;
					}
				} else if (cluster.getRadius() < radiusGreaterThanEqual) {
					cluster.setProject(false);
					continue;
				}
				if ((radiusLessThan > 0 && cluster.getRadius() >= radiusLessThan)
						|| (radiusLessThanEqual > 0 && cluster.getRadius() > radiusLessThanEqual)) {
					cluster.setProject(false);
				}
			}
		} else if (radiusLessThan > 0) {
			for (ClusandraKernel cluster : clusters) {
				if (cluster.getRadius() >= radiusLessThan) {
					cluster.setProject(false);
				}
			}
		}

		if (typeField != null) {
			for (ClusandraKernel cluster : clusters) {
				if ((typeField == TypeField.SUPER && !cluster.isSuper())
						|| (typeField == TypeField.MICRO && cluster.isSuper())) {
					cluster.setProject(false);
				}
			}
		}

	}

	private static void printHeader() {
		if (fields2Display == null || fields2Display.isEmpty()) {
			return;
		}
		// if the user entered '*', then project all fields
		if (fields2Display.size() == 1
				&& ASTERISTIC.equals(fields2Display.get(0))) {
			fields2Display = allFields2Project;
		}
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb);
		for (String field : fields2Display) {
			String fmtHdrStr = fmtHdrMap.get(field);
			if (fmtHdrStr != null) {
				formatter.format(fmtHdrStr, field.toUpperCase());
			}
		}
		sessionState.out.println(formatter.toString());
		sessionState.out.println(String.format(
				String.format("%%0%dd", sb.length()), 0).replace("0", "-"));
	}

	private static void printRows(List<ClusandraKernel> clusters) {
		if (!count && (fields2Display == null || fields2Display.isEmpty())) {
			return;
		}
		int rowCnt = 0;
		Calendar c = Calendar.getInstance();
		for (ClusandraKernel cluster : clusters) {
			if (!cluster.getProject()) {
				continue;
			}
			// don't bother putting out the row if all they're asking for is the
			// count
			if (!count) {
				StringBuilder sb = new StringBuilder();
				Formatter formatter = new Formatter(sb);
				for (String field : fields2Display) {
					String fmtStr = fmtMap.get(field);
					// ensure it is a valid field
					if (N.equals(field)) {
						formatter.format(fmtStr, cluster.getN());
					} else if (ID.equals(field)) {
						formatter.format(fmtStr, cluster.getID());
					} else if (RADIUS.equals(field)) {
						formatter.format(fmtStr, cluster.getRadius());
					} else if (CT.equals(field)) {
						c.setTimeInMillis((long) cluster.getCT());
						formatter.format(fmtStr, c);
					} else if (LAT.equals(field)) {
						c.setTimeInMillis((long) cluster.getLAT());
						formatter.format(fmtStr, c);
					} else if (TYPE.equals(field)) {
						if (cluster.isSuper()) {
							formatter.format(fmtStr, SUPERCLUSTER);
						} else {
							formatter.format(fmtStr, MICROCLUSTER);
						}
					}
				}
				sessionState.out.println(formatter.toString());
			}
			++rowCnt;
		}
		sessionState.out.println("\nclusters = " + rowCnt);
	}

	static boolean checkForConditional(String token) {
		if (token != null) {
			if (EQUALS.equals(token) || LESS_THAN.equals(token)
					|| LESS_THAN_EQUAL.equals(token)
					|| GREATER_THAN.equals(token)
					|| GREATER_THAN_EQUAL.equals(token)) {
				return true;
			}
		}
		return false;
	}

	public static void main(String[] args) {

		fields2Display = new ArrayList();
		fields2Display.add(ASTERISTIC);
		printHeader();

	}

}
