package org.nate.cassandra;

import org.nate.cassandra.connector.ConnectionPool;



public interface Cassandra {
	
	int count(String columnFamily, String key) throws CassandraOperationException;
	Object get(Class<? extends Object> clazz, String key) throws CassandraOperationException;
	String getColumnValue(String columnFamily, String key, String column) throws CassandraOperationException;
	void insert(Object insertObject) throws CassandraOperationException;
	void insertColumnValue(String columnFamily, String key, String column, String value) throws CassandraOperationException;
	void remove(Class<? extends Object> clazz, String key) throws CassandraOperationException;
	void removeColumnValue(String columnFamily, String key, String column) throws CassandraOperationException;
	void update(Object updateObject) throws CassandraOperationException;
	String describe() throws CassandraOperationException;
	ConnectionPool getConnectionPool();
	String getKeyspaceName();
	
}
