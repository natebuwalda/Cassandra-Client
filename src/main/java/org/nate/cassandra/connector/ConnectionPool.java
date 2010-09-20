package org.nate.cassandra.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.nate.cassandra.CassandraOperationException;
import org.nate.functions.functors.ActionFn;
import org.nate.functions.functors.FilterFn;
import org.nate.functions.functors.ListFunctions;
import org.nate.functions.functors.exceptions.FunctorException;
import org.nate.functions.options.Option;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

public class ConnectionPool {

	private Logger logger = org.slf4j.LoggerFactory.getLogger(ConnectionPool.class);
	private List<ConnectionFactory> connectionFactories = new ArrayList<ConnectionFactory>();
	private Map<String, Map<Connection, Boolean>> pool; // = new HashMap<String, Map<Connection, Boolean>>();
	private Integer connectionsPerHost;
	private Long waitForConnectionTimeout;
	
	public ConnectionPool() {
		this.connectionsPerHost = 1;
		this.waitForConnectionTimeout = 1000L;
	}
	
	public ConnectionPool(Integer connectionsPerHost, Long waitTimeout, List<ConnectionFactory> factories) {
		logger.debug("Setting up new connection pool (" + connectionsPerHost + " connections per host)");
		this.connectionsPerHost = connectionsPerHost;
		this.waitForConnectionTimeout = waitTimeout;
		this.connectionFactories = factories;
		this.pool = initializePool();
		logger.debug("Pool initialized successfully");
	}
	
	public Connection getConnection() throws CassandraOperationException {
		logger.debug("Getting a new connection");
		if (connectionFactories.size() == 0) {
			throw new CassandraOperationException("No connection factories defined");
		}
		
		Connection connection = null;
		long waitTime = 0L;
		while (connection == null && waitTime < waitForConnectionTimeout) {
			long start = System.currentTimeMillis();
			List<String> hosts = Lists.newArrayList(pool.keySet());
			int randomHostIndex = hosts.size() > 0 ? new Random().nextInt(hosts.size()) : 0;
			final Map<Connection, Boolean> targetHostPool = pool.get(hosts.get(randomHostIndex));
			Option<Connection> possible = ListFunctions.find(Lists.newArrayList(targetHostPool.keySet()), new FilterFn<Connection>(){
				public boolean apply(Connection it) throws FunctorException {
					return targetHostPool.get(it);
				}
			});
			if (possible.isSome()) {
				connection = possible.get();
				connection.openConnection();
				targetHostPool.put(connection, false);
			}
			long end = System.currentTimeMillis();
			waitTime += (end - start);
		}
		
		if (connection == null) {
			throw new CassandraOperationException("Timed out waiting for connection");
		}
		logger.debug("Connection established: " + connection.getHost());
		return connection;
	}
	
	public void setFactories(List<ConnectionFactory> factories) {
		// we need to re-init every time this is called
		this.connectionFactories = factories;
		this.pool = initializePool();
	}

	public void setConnectionsPerHost(Integer connectionsPerHost) {
		this.connectionsPerHost = connectionsPerHost;
	}

	protected Map<String, Map<Connection, Boolean>> initializePool() {
		final Map<String, Map<Connection, Boolean>> pool = new HashMap<String, Map<Connection, Boolean>>();
		ListFunctions.act(connectionFactories, new ActionFn<ConnectionFactory>() {
			public void apply(ConnectionFactory it) throws FunctorException {
				Map<Connection, Boolean> hostPool = new HashMap<Connection, Boolean>();
				int i = 0;
				while (i < connectionsPerHost) {
					i++;
					hostPool.put(it.createConnection(), true);
				}
				pool.put(it.getHost(), hostPool);
			}
		});
		return pool;
	}

	public void releaseConnection(Connection connection) {
		if (connection == null) {
			logger.debug("Attempting to release a null connection?  WTF!");
			return;
		}
		logger.debug("Attempting to release connection back to the pool: " + connection.getHost());
		if (connection.isOpen()) {
			connection.closeConnection();
		}
		
		Map<Connection, Boolean> targetHostPool = pool.get(connection.getHost());
		targetHostPool.put(connection, true);
		logger.debug("Connection closed and returned to pool successfully");
	}
}
