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

import java.util.*;

import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyCluster;
import static clusandra.cql.CqlMain.cassyDao;

import clusandra.cassandra.ClusandraDao;

/**
 * CluSandra Query Language (CQL) Use
 */
public class CqlUse {

	static void use(ArrayList<String> tokens) {

		if (tokens.size() < 1) {
			sessionState.out.println("invalid use statement, missing keyspace");
			return;
		} else if (tokens.size() > 2) {
			sessionState.out
					.println("invalid use statement, too many parameters");
			return;
		} else if (cassyCluster == null) {
			sessionState.out
					.println("you must first connect to a cluster before issuing the use statement");
			return;
		}
		try {
			cassyDao = new ClusandraDao();
			cassyDao.setCassandraCluster(cassyCluster);
			cassyDao.setKeySpace(tokens.get(0));
			sessionState.keyspace = tokens.get(0);
			cassyDao.afterPropertiesSet();
		} catch (Exception e) {
			sessionState.out
					.println("Unable to initialize Cassandra DAO. Exception = "
							+ e.getMessage());
			throw new RuntimeException(
					"Unable to initialize Cassandra DAO. Exception = "
							+ e.getMessage());
		}

	}

}
