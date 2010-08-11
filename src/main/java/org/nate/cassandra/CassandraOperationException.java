package org.nate.cassandra;

public class CassandraOperationException extends RuntimeException {

	
	private static final long serialVersionUID = 1L;

	public CassandraOperationException() {
		super();
	}

	public CassandraOperationException(String message, Throwable cause) {
		super(message, cause);
	}

	
}
