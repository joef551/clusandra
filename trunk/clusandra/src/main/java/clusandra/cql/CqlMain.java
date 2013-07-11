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
 * $Id: CqlMain.java 173 2011-11-15 22:05:29Z jose $
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package clusandra.cql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.scale7.cassandra.pelops.Cluster;

import static java.lang.System.out;
import static clusandra.cql.CqlConnect.connect;
import static clusandra.cql.CqlConnect.disconnect;
import static clusandra.cql.CqlDistance.distance;
import static clusandra.cql.CqlOverlap.overlap;
import static clusandra.cql.CqlUse.use;
import static clusandra.cql.CqlSelect.select;
import static clusandra.cql.CqlSum.sum;
import static clusandra.cql.CqlMerge.merge;
import static clusandra.cql.CqlTruncate.truncate;
import static clusandra.cql.CqlSelect.checkForConditional;
import static clusandra.cql.CqlAggregate.aggregate;
import clusandra.cassandra.ClusandraDao;

/**
 * Clusandra Query Language (CQL) Main.
 * 
 * Loosely modeled after the Cassandra Command Line Interface (CLI)
 */
public class CqlMain {

	static CqlSessionState sessionState = new CqlSessionState();
	// the Clusandra DAO that is used for interacting with Cassandra and the
	// Clusandra data store
	static ClusandraDao cassyDao = null;
	static Cluster cassyCluster = null;

	// scanner used for reading lines in from the console
	private static Scanner in = new Scanner(System.in);

	private static final String MAIN_PROMPT = "\ncql> ";
	private static final String WAIT_PROMPT = "\ncql>.... ";
	private static final String QUIT = "quit";
	private static final String SUM = "sum";
	private static final String EXIT = "exit";
	private static final String CONNECT = "connect";
	private static final String DISCONNECT = "disconnect";
	private static final String DISTANCE = "distance";
	private static final String OVERLAP = "overlap";
	private static final String TRUNCATE = "truncate";
	private static final String AGGREGATE = "aggregate";
	private static final String MERGE = "merge";
	private static final String USE = "use";
	private static final String SELECT = "select";
	private static final String QUESTION = "?";
	private static final String SEMICOLON = ";";
	private static final String EMPTY_STR = "";
	private static final String DBLDASH = "--";
	private static final String COMMENTSTART = "/*";
	private static final String COMMENTEND = "*/";
	private static final String SPACE = " ";

	// elements of array must be placed in the specified order!!
	private static final String[] conditionalOperators = { ">=", "<=", ">",
			"<", "=" };

	/**
	 * Process a statement (query) entered by user or via batch file.
	 * 
	 * @param query
	 */
	public static void processStatement(String query) {

		if (query.equals(QUESTION)) {
			// display help - TODO
			return;
		} else if (query.equals(SEMICOLON)) {
			return;
		}

		// separate out the tokens, base delimiter is the ' ' character,
		// and remove semicolon
		ArrayList<String> tokens = new ArrayList<String>();
		for (String token : query.substring(0, query.length() - 1)
				.toLowerCase().split(SPACE)) {

			if (token.length() == 0) {
				continue;
			} else if (checkForConditional(token)) {
				tokens.add(token);
				continue;
			}

			// look for tokens like 'x=y', 'x<=y', 'x=', 'x<=', etc.
			// and split them up
			int i = 0;
			for (; i < conditionalOperators.length; i++) {
				if (token.endsWith(conditionalOperators[i])) {
					tokens.add(token.substring(0, token.length()
							- conditionalOperators[i].length()));
					tokens.add(conditionalOperators[i]);
					break;
				} else if (token.startsWith(conditionalOperators[i])) {
					tokens.add(conditionalOperators[i]);
					tokens.add(token.substring(conditionalOperators[i].length()));
					break;
				} else {
					String[] sb = token.split(conditionalOperators[i]);
					if (sb.length == 2) {
						tokens.add(sb[0].trim());
						tokens.add(conditionalOperators[i]);
						tokens.add(sb[1].trim());
						break;
					}
				}
			}
			if (i == conditionalOperators.length) {
				tokens.add(token);
			}
		}

		String token = tokens.remove(0);
		if (token.equals(QUIT) || token.equals(EXIT)) {
			System.exit(0);
		} else if (token.equals(CONNECT)) {
			connect(tokens);
		} else if (token.equals(DISCONNECT)) {
			disconnect();
		} else if (token.equals(SUM)) {
			sum(tokens);
		} else if (token.equals(USE)) {
			use(tokens);
		} else if (token.equals(SELECT)) {
			select(tokens);
		} else if (token.equals(TRUNCATE)) {
			truncate(tokens);
		} else if (token.equals(DISTANCE)) {
			distance(tokens);
		} else if (token.equals(OVERLAP)) {
			overlap(tokens);
		} else if (token.equals(AGGREGATE)) {
			aggregate(tokens);
		} else if (token.equals(MERGE)) {
			merge(tokens);
		} else {
			out.println("invalid keyword");
		}
	}

	public static void main(String args[]) throws IOException {
		// process command line arguments
		CqlOptions cliOptions = new CqlOptions();
		cliOptions.processArgs(sessionState, args);

		// connect to cassandra server if host argument specified.
		if (sessionState.hostName != null) {
			try {
				connect(sessionState.hostName, sessionState.thriftPort);
			} catch (RuntimeException e) {
				sessionState.err.println(e.getMessage());
				System.exit(-1);
			}
		}

		if (cassyDao == null) {
			// Connection parameter was either invalid or not present.
			// User must connect explicitly using the "connect" CLI statement.
		}

		// load statements from file and process them
		if (sessionState.inFileMode()) {
			FileReader fileReader;

			try {
				fileReader = new FileReader(sessionState.filename);
			} catch (IOException e) {
				sessionState.err.println(e.getMessage());
				return;
			}

			evaluateFileStatements(new BufferedReader(fileReader));
			return;
		}

		String currentStatement = EMPTY_STR;
		String line = null;
		String prompt = MAIN_PROMPT;
		while (true) {

			try {
				line = readLine(prompt);
			} catch (Exception e) {
				e.printStackTrace(out);
				return;
			}

			// skipping empty and comment lines
			if (line.length() == 0 || line.startsWith(DBLDASH))
				continue;

			currentStatement += line;

			if (line.endsWith(SEMICOLON) || line.equals(QUESTION)) {
				processStatement(currentStatement);
				currentStatement = "";
				prompt = MAIN_PROMPT;
			} else {
				currentStatement += " "; // ready for new line
				prompt = WAIT_PROMPT;
			}
		}
	}

	private static void evaluateFileStatements(BufferedReader reader)
			throws IOException {
		String line = "";
		String currentStatement = EMPTY_STR;

		boolean commentedBlock = false;

		while ((line = reader.readLine()) != null) {
			line = line.trim();

			// skipping empty and comment lines
			if (line.length() == 0 || line.startsWith(DBLDASH))
				continue;

			if (line.startsWith(COMMENTSTART))
				commentedBlock = true;

			if (line.startsWith(COMMENTEND) || line.endsWith(COMMENTEND)) {
				commentedBlock = false;
				continue;
			}

			if (commentedBlock) // skip commented lines
				continue;

			currentStatement += line;

			if (line.endsWith(SEMICOLON)) {
				processStatement(currentStatement);
				currentStatement = EMPTY_STR;
			} else {
				currentStatement += SPACE; // ready for new line
			}
		}
	}

	private static String readLine(String prompt) {
		out.println();
		out.print(prompt);
		return readLine();
	}

	private static String readLine() {
		in.hasNextLine();
		String line = in.nextLine().trim().toLowerCase();
		out.println();
		return line;
	}

}
