package org.nate.cassandra;

import org.apache.cassandra.thrift.Cassandra.Client;

public abstract class Operation<T> {

	private final Cassandra cassandra;

	public Operation(Cassandra cassandra) {
		this.cassandra = cassandra;
	}
	
	public Client client;
	public abstract T work() throws Exception;
	
}