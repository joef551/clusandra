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
 * $Date: 2011-08-16 11:36:43 -0400 (Tue, 16 Aug 2011) $
 * $Revision: 104 $
 * $Author: jose $
 * $Id: CqlConnect.java 104 2011-08-16 15:36:43Z jose $
 */
package clusandra.cql;

import java.util.*;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.ClusterManager;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.SimpleConnectionAuthenticator;

import static clusandra.cql.CqlMain.sessionState;
import static clusandra.cql.CqlMain.cassyCluster;
import static clusandra.cql.CqlMain.cassyDao;

import clusandra.cassandra.ClusandraDao;

/**
 * CluSandra Query Language (CQL) Connect
 */

public class CqlConnect {

	static ClusterManager clusterManager = null;
	static SimpleConnectionAuthenticator authenticator = null;

	/**
	 * Establish a connection to cassandra instance
	 * 
	 * @param server
	 *            - hostname or IP of the server
	 * @param port
	 *            - Thrift port number
	 */
	public static void connect(String server, int port, String userName,
			String password) {
		
		authenticator = null;

		if (userName != null && password != null) {
			authenticator = new SimpleConnectionAuthenticator(userName,
					password);
		} else if (sessionState.username != null
				&& sessionState.password != null) {
			// Authenticate
			authenticator = new SimpleConnectionAuthenticator(
					sessionState.username, sessionState.password);
		}

		if (authenticator != null) {
			cassyCluster = new Cluster(server, port, 4000, false, authenticator);
		} else {
			cassyCluster = new Cluster(server, port, 4000, false);
		}

		if (sessionState.keyspace != null) {
			cassyDao = new ClusandraDao();
			cassyDao.setCassandraCluster(cassyCluster);
			cassyDao.setKeySpace(sessionState.keyspace);
			try {
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

		clusterManager = Pelops.createClusterManager(cassyCluster);

		// Lookup the cluster name, this is to make it clear which cluster the
		// user is connected to
		String clusterName = null;
		try {
			clusterName = clusterManager.getClusterName();
		} catch (Exception e) {
			sessionState.out.println("Unable to get cluster name. Exception = "
					+ e.getMessage());
			return;
		}

		sessionState.out.printf("Connected to: \"%s\" on %s/%d%n", clusterName,
				server, port);
	}
	
	static void connect(String host, int port) {
		connect(host, port, null, null);
	}

	/**
	 * Disconnect connection to cassandra instance
	 */
	static void disconnect() {
		if (cassyDao != null) {
			cassyDao.getThriftPool().shutdown();
			cassyDao = null;
		}
	}

	/**
	 * Checks whether the DAO is connected.
	 * 
	 * @return boolean - true when connected, false otherwise
	 */
	static boolean isConnected() {
		return (cassyDao == null) ? false : true;
	}

	static void connect(ArrayList<String> tokens) {

		if (tokens.size() < 2) {
			sessionState.out
					.println("invalid connect statement, missing host and/or port");
			return;
		} else if (tokens.size() == 3) {
			sessionState.out
					.println("invalid connect statement, need to specify password");
			return;
		} else if (tokens.size() > 4) {
			sessionState.out
					.println("invalid connect statement, too many parameters");
			return;
		}

		String host = tokens.get(0);
		String portStr = tokens.get(1);
		String userName = null;
		String password = null;

		if (tokens.size() > 2) {
			userName = tokens.get(2);
			password = tokens.get(3);
		}

		int port;
		try {
			port = Integer.parseInt(portStr);
			if (port == 0 || port > 65535) {
				System.out.println("ERROR: invalid port number\n");
				sessionState.out
						.println("invalid port number, not within proper range [1,65535]");
				return;
			}
		} catch (Exception e) {
			sessionState.out.println("invalid port string, not numeric");
			return;
		}
		connect(host, port, userName, password);
	}

}
