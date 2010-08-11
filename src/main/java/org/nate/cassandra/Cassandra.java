package org.nate.cassandra;



public interface Cassandra {
	
	int count(String columnFamily, String key) throws CassandraOperationException;
	Object get(Class<? extends Object> clazz, String key);
	String getColumnValue(String columnFamily, String key, String column) throws CassandraOperationException;
	void insert(Class<? extends Object> clazz, Object insertObject) throws CassandraOperationException;
	void insertColumnValue(String columnFamily, String key, String column, String value) throws CassandraOperationException;
	void remove(Class<? extends Object> clazz, String key);
	void removeColumnValue(String columnFamily, String key, String column) throws CassandraOperationException;
	void update(Object updateObject);

	
}