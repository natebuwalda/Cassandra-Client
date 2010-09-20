package org.nate.cassandra;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.nate.cassandra.annotation.ColumnFamily;
import org.nate.cassandra.connector.ConnectionPool;
import org.nate.functions.functors.FilterFn;
import org.nate.functions.functors.ListFunctions;
import org.nate.functions.functors.TransformFn;
import org.nate.functions.functors.exceptions.FunctorException;
import org.nate.functions.options.Option;
import org.slf4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class CassandraOperations implements Cassandra {

	private static final char SPACE_SEPARATOR = ' ';
	private Logger log = org.slf4j.LoggerFactory.getLogger(CassandraOperations.class);
	private CassandraOperationUtils opUtils = new CassandraOperationUtils();
	private OperationWorker worker = new OperationWorker(this);

	private ConnectionPool connectionPool;
	private Client client;
	private String keyspaceName = "Keyspace1";
	
	public int count(final String columnFamily, final String key) throws CassandraOperationException {
		return worker.doWork(new Operation<Integer>(this) {
			public Integer work() throws Exception {
				SlicePredicate slicePredicate = opUtils.createEmptySlicePredicate();
				
				List<ColumnOrSuperColumn> sliceResults = client.get_slice(keyspaceName, key, new ColumnParent(columnFamily), slicePredicate, ConsistencyLevel.ONE);
	
				return sliceResults.size();
			}
		});
	}

	public String describe() throws CassandraOperationException {
		StringBuilder keyspaceDescription = new StringBuilder(keyspaceName);
		keyspaceDescription.append("{\n");
		
		keyspaceDescription.append(worker.doWork(new Operation<StringBuilder>(this) {
			public StringBuilder work() throws Exception {
				StringBuilder innerDescription = new StringBuilder();
				Map<String, Map<String, String>> keyspaceRepresentationMap = client.describe_keyspace(keyspaceName);
				
				for (String columnName : keyspaceRepresentationMap.keySet()) {
					innerDescription.append("\t").append(columnName).append("{\n"); 
					for (String columnAttribute : keyspaceRepresentationMap.get(columnName).keySet()){
						String columnAttributeValue = keyspaceRepresentationMap.get(columnName).get(columnAttribute);
						innerDescription.append("\t\t").append(columnAttribute).append(" : ").append(columnAttributeValue).append("\n");
					}
					innerDescription.append("\t}\n");
				}
				return innerDescription;
			}}).toString());

		keyspaceDescription.append("}");
		return keyspaceDescription.toString();
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object get(final Class clazz, final String key) throws CassandraOperationException {
		log.debug("Performing get operation - " + clazz.getSimpleName() + ":" + key);
		return worker.doWork(new Operation<Object>(this) {
			public Object work() throws Exception {
				final Object result = clazz.getConstructor(new Class[]{}).newInstance(new Object[]{});
				
				if (clazz.isAnnotationPresent(ColumnFamily.class)) {
					String columnFamilyName = opUtils.determineColumnFamily(clazz); 
					SlicePredicate slicePredicate = opUtils.createEmptySlicePredicate();
					
					final List<ColumnOrSuperColumn> sliceResults = client.get_slice(keyspaceName, key, new ColumnParent(columnFamilyName), slicePredicate, ConsistencyLevel.ONE);			
					
					if (sliceResults.isEmpty()) {
						return null;
					}
					
					List<Field> declaredFields = Lists.newArrayList(clazz.getDeclaredFields());				
					Option<Field> keyFieldOption = opUtils.keyFieldFor(declaredFields);
					keyFieldOption.get().setAccessible(true);
					keyFieldOption.get().set(result, key);
									
					for (final Field field : declaredFields) {
						field.setAccessible(true);
						Option<ColumnOrSuperColumn> columnOption = ListFunctions.find(sliceResults, new FilterFn<ColumnOrSuperColumn>(){
							public boolean apply(ColumnOrSuperColumn it) {
								return new String(it.getColumn().getName()).equals(field.getName());
							}							
						});
						if (columnOption.isSome()) {
							field.set(result, opUtils.convertStringToValue(new String(columnOption.get().getColumn().getValue()), field.getType()));
						}
					}
				
					return result;
				} else {
					throw new IllegalArgumentException(Joiner.on(SPACE_SEPARATOR).join("Class", clazz.getName(), "is not a ColumnFamily"));
				}
			}}); 
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<Object> getAll(final Class clazz) throws CassandraOperationException {
		log.debug("Performing get all operation - " + clazz.getSimpleName());
		final List result = new ArrayList();
			
		try {
			if (clazz.isAnnotationPresent(ColumnFamily.class)) {
				final String columnFamilyName = opUtils.determineColumnFamily(clazz); 
				final SlicePredicate slicePredicate = opUtils.createEmptySlicePredicate();
						
				List<KeySlice> sliceResults = worker.doWork(new Operation<List>(this) {
					public List work() throws Exception {
						return client.get_range_slice(keyspaceName, new ColumnParent(columnFamilyName), slicePredicate, "", "", 100, ConsistencyLevel.ONE);				
				}}); 
				
				result.addAll(ListFunctions.transform(sliceResults, new TransformFn<Object, KeySlice>(){
					public Object apply(KeySlice it) throws FunctorException {
						return get(clazz, it.getKey());						
					}	
				}));

				return result;
			} else {
				throw new IllegalArgumentException(Joiner.on(SPACE_SEPARATOR).join("Class", clazz.getName(), "is not a ColumnFamily"));
			}
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform getAll operation", e);
		}
	}

	
	public String getColumnValue(final String columnFamily, final String key, final String column) throws CassandraOperationException {
		return worker.doWork(new Operation<String>(this){
			public String work() throws Exception {
				try {
					ColumnPath columnPath = opUtils.createColumnPath(columnFamily, column.getBytes());
					Column resultColumn = client.get(keyspaceName, key, columnPath, ConsistencyLevel.ONE).getColumn();		
					return new String(resultColumn.value);
				} catch (NotFoundException nfe) {
					return null;
				} 
			}
		});
	
	}


	public ConnectionPool getConnectionPool() {
		return connectionPool;
	}

	public void insert(final Object insertObject) throws CassandraOperationException {
		if (insertObject == null) {
			throw new IllegalArgumentException("Object to be inserted cannot be null");
		}
		
		worker.doWork(new Operation<Void>(this) {
			public Void work() throws Exception {
				final Class clazz = insertObject.getClass();
				if (clazz.isAnnotationPresent(ColumnFamily.class)) {
					final String columnFamilyName = opUtils.determineColumnFamily(clazz);
					
					final List<Field> declaredFields = Lists.newArrayList(clazz.getDeclaredFields());	
					Option<Field> keyFieldOption = opUtils.keyFieldFor(declaredFields);
					keyFieldOption.get().setAccessible(true);
					final Object key = keyFieldOption.get().get(insertObject);
					
					if (key == null) {
						throw new IllegalArgumentException("No key found");
					} else {
						for (Field field : declaredFields) {
							if (field.isAnnotationPresent(org.nate.cassandra.annotation.Column.class)) {
								field.setAccessible(true);
								Object fieldValue = field.get(insertObject);
								if (fieldValue != null) {	
									ColumnPath columnPath = opUtils.createColumnPath(columnFamilyName, opUtils.determineColumnName(field).getBytes());
									client.insert(keyspaceName, opUtils.convertValueToString(key), columnPath, opUtils.convertValueToString(fieldValue).getBytes(), System.currentTimeMillis(), ConsistencyLevel.ANY);
								}
							}
						}
					}	
				} else {
					throw new IllegalArgumentException(Joiner.on(SPACE_SEPARATOR).join("Class", clazz.getName(), "is not a ColumnFamily"));
				}
				return null;
			}
			
		});
	}

	public void insertColumnValue(final String columnFamily, final String key, final String column, final String value) throws CassandraOperationException {
		worker.doWork(new Operation<Void>(this){
			public Void work() throws Exception {
				ColumnPath columnPath = opUtils.createColumnPath(columnFamily, column.getBytes());
				client.insert(keyspaceName, key, columnPath, value.getBytes(), System.currentTimeMillis(), ConsistencyLevel.ANY);
				return null;
			}
		});
	}



	public void remove(final Class<? extends Object> clazz, final String key) throws CassandraOperationException {
		worker.doWork(new Operation<Void>(this){
			public Void work() throws Exception {
				if (clazz.isAnnotationPresent(ColumnFamily.class)) {
					final String columnFamilyName = opUtils.determineColumnFamily(clazz);
					SlicePredicate slicePredicate = opUtils.createEmptySlicePredicate();
					
					List<ColumnOrSuperColumn> sliceResults = Lists.newArrayList(client.get_slice(keyspaceName, key, new ColumnParent(columnFamilyName), slicePredicate, ConsistencyLevel.ONE));
					
					for (ColumnOrSuperColumn it : sliceResults) {
						ColumnPath columnPath = opUtils.createColumnPath(columnFamilyName, it.getColumn().getName());
						client.remove(keyspaceName, key, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);			
					}
				}
				return null;
			}
		}); 
	}

	public void removeColumnValue(final String columnFamily, final String key, final String column) throws CassandraOperationException {
		worker.doWork(new Operation<Void>(this){
			public Void work() throws Exception {
				ColumnPath columnPath = new ColumnPath();
				columnPath.setColumn(column.getBytes());
				columnPath.setColumn_family(columnFamily);				
				client.remove(keyspaceName, key, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);
				return null;
			}	
		}); 
	}

	public void setConnectionPool(ConnectionPool connectionPool) {
		this.connectionPool = connectionPool;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void update(final Object updateObject) throws CassandraOperationException {
		if (updateObject == null) {
			throw new IllegalArgumentException("Object to be updated cannot be null");
		}
		
		worker.doWork(new Operation<Void>(this){
			public Void work() throws Exception {
				final Class updatedObjectsClass = updateObject.getClass();
				if (updatedObjectsClass.isAnnotationPresent(ColumnFamily.class)) {
					String columnFamilyName = opUtils.determineColumnFamily(updatedObjectsClass); 
					
					final List<Field> declaredFields = Lists.newArrayList(updatedObjectsClass.getDeclaredFields());	
					Option<Field> keyFieldOption = opUtils.keyFieldFor(declaredFields);
					keyFieldOption.get().setAccessible(true);
					final Object key = keyFieldOption.get().get(updateObject);
					
					if (key == null) {
						throw new IllegalArgumentException("No key found");
					} else {	
						List<Mutation> mutations = ListFunctions.transform(declaredFields, new TransformFn<Mutation, Field>(){
							public Mutation apply(Field field) throws FunctorException {
								try {
									Mutation mutation = null;
									if (field.isAnnotationPresent(org.nate.cassandra.annotation.Column.class)) {
										mutation = new Mutation();
										field.setAccessible(true);
										Object fieldValue = field.get(updateObject);
										Column column = opUtils.createColumnObject(field, fieldValue);
										
										ColumnOrSuperColumn columnOrSuper = new ColumnOrSuperColumn();
										columnOrSuper.setColumn(column);
																						
										mutation.setColumn_or_supercolumn(columnOrSuper);
									}
									return mutation;
								} catch (Exception e) {
									throw new FunctorException("mutateColumn function failed", e);
								} 
							}					
						});	
						
						List<Mutation> filteredMutations = ListFunctions.filter(mutations, new FilterFn<Mutation>() {
							public boolean apply(Mutation it) throws FunctorException {
								return it != null;
							}
						});
						
						Map<String, List<Mutation>> columnFamilyUpdates = new HashMap<String, List<Mutation>>();
						columnFamilyUpdates.put(columnFamilyName, filteredMutations);
						
						Map<String, Map<String, List<Mutation>>> rowUpdates = new HashMap<String, Map<String, List<Mutation>>>();
						rowUpdates.put(opUtils.convertValueToString(key), columnFamilyUpdates);
						
						client.batch_mutate(keyspaceName, rowUpdates, ConsistencyLevel.ALL);
					} 
				}
				return null;
			}}); 
	}

	
	public Client getClient() {
		return this.client;
	}
	
	
}
