package org.nate.cassandra;

import org.nate.cassandra.connector.Connection;

public class OperationWorker {
	
	public Cassandra cassandra;
	
	public OperationWorker(Cassandra cassandra) {
		this.cassandra = cassandra;
	}

	public <T> T doWork(Operation<T> operation) {
		Connection connection = null;
		try {
			connection = cassandra.getConnectionPool().getConnection();
			operation.client = connection.getClient();
			T result = operation.work();
			return result;
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform operation", e);
		} finally {
			cassandra.getConnectionPool().releaseConnection(connection);
		}

	}
}