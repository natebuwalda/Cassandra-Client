package org.nate.cassandra;

import org.nate.cassandra.connector.Connection;
import org.slf4j.Logger;

public class OperationWorker {
	
	private Logger logger = org.slf4j.LoggerFactory.getLogger(OperationWorker.class);
	public Cassandra cassandra;
	
	public OperationWorker(Cassandra cassandra) {
		this.cassandra = cassandra;
	}

	public <T> T doWork(Operation<T> operation) {
		logger.debug("Performing specified Cassandra operation");
		
		Connection connection = null;
		try {
			connection = cassandra.getConnectionPool().getConnection();
			logger.debug("Connection established");
			operation.client = connection.getClient();
			T result = operation.work();
			return result;
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform operation", e);
		} finally {
			cassandra.getConnectionPool().releaseConnection(connection);
			logger.debug("Connection released");
		}

	}
}