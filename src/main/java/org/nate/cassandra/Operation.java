package org.nate.cassandra;

import org.apache.cassandra.thrift.Cassandra.Client;

public abstract class Operation<T> {
	
	public Client client;
	public abstract T work(Object... args);
	
}