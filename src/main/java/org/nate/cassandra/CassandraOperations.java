package org.nate.cassandra;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.nate.functions.functors.ActionFn;
import org.nate.functions.functors.FilterFn;
import org.nate.functions.functors.ListFunctions;
import org.nate.functions.functors.TransformFn;
import org.nate.functions.functors.exceptions.FunctorException;
import org.nate.functions.options.Option;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class CassandraOperations implements Cassandra {

	private static final char SPACE_SEPARATOR = ' ';
	private static final Log log = LogFactory.getLog(CassandraOperations.class);

	//these are the default settings for a single-node local cassandra instance
	private String host = "localhost";
	private Integer port = 9160;
	private Integer timeout = 1000;
	private TTransport transport;
	private Client client;
	private String keyspaceName = "Keyspace1";


	public int count(String columnFamily, String key) throws CassandraOperationException {
		int count = 0;
		try {
			openConnection();
			SlicePredicate slicePredicate = createSlicePredicate();
			
			List<ColumnOrSuperColumn> sliceResults = client.get_slice(keyspaceName, key, new ColumnParent(columnFamily), slicePredicate, ConsistencyLevel.ONE);

			count = sliceResults.size();
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform count operation", e);
		} finally {
			closeConnection();
		}
		return count;
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object get(Class clazz, final String key) {
		try {
			final Object result = clazz.getConstructor(new Class[]{}).newInstance(new Object[]{});
			
			openConnection();
			if (clazz.isAnnotationPresent(ColumnFamily.class)) {
				String columnFamilyName = determineColumnFamily(clazz); 
				SlicePredicate slicePredicate = createSlicePredicate();
				
				final List<ColumnOrSuperColumn> sliceResults = client.get_slice(keyspaceName, key, new ColumnParent(columnFamilyName), slicePredicate, ConsistencyLevel.ONE);			

				List<Field> declaredFields = Lists.newArrayList(clazz.getDeclaredFields());				
				Option<Field> keyFieldOption = keyFieldFor(declaredFields);
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
						field.set(result, convertStringToValue(new String(columnOption.get().getColumn().getValue()), field.getType()));
					}
				}
			
				return result;
			} else {
				throw new IllegalArgumentException(Joiner.on(SPACE_SEPARATOR).join("Class", clazz.getName(), "is not a ColumnFamily"));
			}
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform get operation", e);
		} finally {
			closeConnection();
		}
	}


	public String getColumnValue(String columnFamily, String key, String column) throws CassandraOperationException {
		String result = null;
		try {
			openConnection();
			ColumnPath columnPath = new ColumnPath();
			columnPath.setColumn(column.getBytes());
			columnPath.setColumn_family(columnFamily);

			Column resultColumn = client.get(keyspaceName, key, columnPath, ConsistencyLevel.ONE).getColumn();		
			result = new String(resultColumn.value);
		} catch (NotFoundException nfe) {
			// eat this, we will just return a null value if none is found
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform get string value operation", e);
		} finally {
			closeConnection();
		}
		
		return result;
	}


	public void insert(Class<? extends Object> clazz, final Object insertObject) throws CassandraOperationException {
		try {
			openConnection();
	
			if (clazz.isAnnotationPresent(ColumnFamily.class)) {
				final String columnFamilyName = determineColumnFamily(clazz);
				
				final List<Field> declaredFields = Lists.newArrayList(clazz.getDeclaredFields());	
				Option<Field> keyFieldOption = keyFieldFor(declaredFields);
				keyFieldOption.get().setAccessible(true);
				final Object key = keyFieldOption.get().get(insertObject);
				
				if (key == null) {
					throw new IllegalArgumentException("No key found");
				} else {
					ListFunctions.act(declaredFields, new ActionFn<Field>(){
						public void apply(Field field) throws FunctorException {
							try {
								if (field.isAnnotationPresent(org.nate.cassandra.Column.class)) {
									field.setAccessible(true);
									Object fieldValue = field.get(insertObject);
									if (fieldValue != null) {	
										ColumnPath columnPath = new ColumnPath();
										columnPath.setColumn(determineColumnName(field).getBytes());
										columnPath.setColumn_family(columnFamilyName);
										client.insert(keyspaceName, convertValueToString(key), columnPath, convertValueToString(fieldValue).getBytes(), System.currentTimeMillis(), ConsistencyLevel.ANY);
									}
								}
							} catch (Exception e) {
								throw new FunctorException("insertColumn function failed", e);
							} 
						}
					});	
				}
			} else {
				throw new IllegalArgumentException(Joiner.on(SPACE_SEPARATOR).join("Class", clazz.getName(), "is not a ColumnFamily"));
			}
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform insert by object opertion", e);
		} finally {
			closeConnection();
		}
	}

	public void insertColumnValue(String columnFamily, String key, String column, String value) throws CassandraOperationException {
		try {
			openConnection();
			ColumnPath columnPath = new ColumnPath();
			columnPath.setColumn(column.getBytes());
			columnPath.setColumn_family(columnFamily);
	
			client.insert(keyspaceName, key, columnPath, value.getBytes(), System.currentTimeMillis(), ConsistencyLevel.ANY);
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform insert string operation", e);
		} finally {
			closeConnection();
		}
	}


	public void remove(Class<? extends Object> clazz, final String key) {
		log.debug("***** CASSANDRA REMOVE COMMAND ISSUED *****");
		try {
			openConnection();
			if (clazz.isAnnotationPresent(ColumnFamily.class)) {
				final String columnFamilyName = determineColumnFamily(clazz);
				SlicePredicate slicePredicate = createSlicePredicate();
				
				List<ColumnOrSuperColumn> sliceResults = Lists.newArrayList(client.get_slice(keyspaceName, key, new ColumnParent(columnFamilyName), slicePredicate, ConsistencyLevel.ONE));
				log.debug(Joiner.on(SPACE_SEPARATOR).join(key, "slice size:", sliceResults.size()));
				
				ListFunctions.act(sliceResults, new ActionFn<ColumnOrSuperColumn>(){
					public void apply(ColumnOrSuperColumn it) throws FunctorException {
						log.debug(Joiner.on(SPACE_SEPARATOR).join("*** removing slice contents: ", new String(it.getColumn().getName()), ":", new String(it.getColumn().getValue())));
						try {
							ColumnPath columnPath = new ColumnPath();
							columnPath.setColumn(it.getColumn().getName());
							columnPath.setColumn_family(columnFamilyName);
							client.remove(keyspaceName, key, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);
						} catch (Exception e) {
							throw new FunctorException("removeColumn function failed", e);
						}
					}
				});
			}
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform remove operation for key " + key, e);
		} finally {
			closeConnection();
		}
	}

	public void removeColumnValue(String columnFamily, String key, String column) throws CassandraOperationException {
		try {
			openConnection();
			ColumnPath columnPath = new ColumnPath();
			columnPath.setColumn(column.getBytes());
			columnPath.setColumn_family(columnFamily);
				
			client.remove(keyspaceName, key, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);
			log.debug(Joiner.on(SPACE_SEPARATOR).join("Removed column", column, "for key", key, "successfully"));
		} catch (Exception e) {
			throw new CassandraOperationException(Joiner.on(SPACE_SEPARATOR).join("Unable to perform remove column value operation for key", key, "column", column), e);
		} finally {
			closeConnection();
		}
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public void setPort(Integer port) {
		this.port = port;
	}
	
	
	public void setTimeout(Integer timeout) {
		this.timeout = timeout;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void update(final Object updateObject) {
		log.debug("***** CASSANDRA UPDATE COMMAND ISSUED *****");
		try {
			openConnection();
			
			if (updateObject == null) {
				throw new IllegalArgumentException("Object to be updated cannot be null");
			}
			
			final Class updatedObjectsClass = updateObject.getClass();
			if (updatedObjectsClass.isAnnotationPresent(ColumnFamily.class)) {
				String columnFamilyName = determineColumnFamily(updatedObjectsClass); 
				
				final List<Field> declaredFields = Lists.newArrayList(updatedObjectsClass.getDeclaredFields());	
				Option<Field> keyFieldOption = ListFunctions.find(declaredFields, new FilterFn<Field>() {
					public boolean apply(Field it) throws FunctorException {
						if (it.isAnnotationPresent(Key.class)) {
							return true;
						}
						return false;
					}
					
				});
				keyFieldOption.get().setAccessible(true);
				final Object key = keyFieldOption.get().get(updateObject);
				
				if (key == null) {
					throw new IllegalArgumentException("No key found");
				} else {	
					List<Mutation> mutations = ListFunctions.transform(declaredFields, new TransformFn<Mutation, Field>(){
						public Mutation apply(Field field) throws FunctorException {
							try {
								Mutation mutation = null;
								if (field.isAnnotationPresent(org.nate.cassandra.Column.class)) {
									log.debug(field.getName());
									mutation = new Mutation();
									field.setAccessible(true);
									Object fieldValue = field.get(updateObject);

									Column column = new Column();
									column.setName(field.getName().getBytes());
									column.setValue(convertValueToString(fieldValue).getBytes());
									column.setTimestamp(System.currentTimeMillis());
									
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
							try {
								if (it != null) {
									return true;
								}
								return false;
							} catch (Exception e) {
								throw new FunctorException("filterMutations function failed", e);
							}
						}
					});
					
					Map<String, List<Mutation>> columnFamilyUpdates = new HashMap<String, List<Mutation>>();
					columnFamilyUpdates.put(columnFamilyName, filteredMutations);
					
					Map<String, Map<String, List<Mutation>>> rowUpdates = new HashMap<String, Map<String, List<Mutation>>>();
					rowUpdates.put(convertValueToString(key), columnFamilyUpdates);
					
					client.batch_mutate(keyspaceName, rowUpdates, ConsistencyLevel.ALL);
				} 
			}
			log.debug("Updated object " + updateObject + " successfully");
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform object update", e);
		} finally {
			closeConnection();
		}
	}
	
	private void closeConnection() throws CassandraOperationException {
		try {
			if (client != null && transport.isOpen()) {
				transport.flush();
				transport.close();
			}
			client = null;
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to close connection", e);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" }) 
	private Object convertStringToValue(String string, Class fieldType) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Object converted = null;
		
		if ("String".equals(fieldType.getSimpleName())) {
			converted = string;
		} else if ("byte[]".equals(fieldType.getSimpleName())) {
			converted = string.getBytes();
		}else if (StringUtils.isNumeric(string)) {
			converted = fieldType.getConstructor(new Class[]{String.class}).newInstance(new Object[]{string});
		}
		
		return converted;
	}

	private String convertValueToString(Object value) {
		String converted = null;
		if (value instanceof String) {
			converted = (String) value;
		} else if (value instanceof byte[]) {
			converted = new String((byte[]) value);
		} else if (value instanceof Number){
			converted = ((Number) value).toString();
		} else {
			throw new IllegalArgumentException("Value must be a String or a Number, was " + value.toString());
		}
		return converted;
	}

	private SlicePredicate createSlicePredicate() {
		SlicePredicate slicePredicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(new byte[] {});
		sliceRange.setFinish(new byte[] {});
		slicePredicate.setSlice_range(sliceRange);
		return slicePredicate;
	}

	private String determineColumnFamily(Class<? extends Object> clazz) throws IllegalAccessException,
			InvocationTargetException {
		String columnFamilyName = null;
		try {
			Annotation runtimeAnnotation = clazz.getAnnotation(ColumnFamily.class);
			if (runtimeAnnotation != null) {
				Method nameMethod = ((Class<? extends Annotation>) runtimeAnnotation.getClass()).getMethod("name", new Class<?>[]{});
				columnFamilyName  = (String) nameMethod.invoke(runtimeAnnotation, new Object[]{});
				log.debug(ColumnFamily.class + " annotated class name value " + columnFamilyName);
			}
		} catch (NoSuchMethodException nsme) {
			//eat this exception because we do not care
			log.debug("Annotation " + ColumnFamily.class + " does not have a 'name' method", nsme);
		}
		
		if (columnFamilyName  == null || columnFamilyName.isEmpty()) {
			columnFamilyName  = clazz.getSimpleName();
			log.debug(ColumnFamily.class + " did not have a name value, using class name " + columnFamilyName);
		}
		return columnFamilyName;
	}

	private String determineColumnName(Field field) throws IllegalAccessException, InvocationTargetException {
		String columnName = null;
		
		try {
			Annotation runtimeAnnotation = field.getAnnotation(org.nate.cassandra.Column.class);
			if (runtimeAnnotation != null) {
				Method nameMethod = ((Class<? extends Annotation>) runtimeAnnotation.getClass()).getMethod("name", new Class<?>[]{});
				columnName = (String) nameMethod.invoke(runtimeAnnotation, new Object[]{});
			}
		} catch (NoSuchMethodException nsme) {
			//eat this exception because we do not care
			log.debug("Annotation " + org.nate.cassandra.Column.class + " does not have a 'name' method", nsme);
		}
		
		if (columnName == null || columnName.isEmpty()) {
			columnName = field.getName();
			log.debug(org.nate.cassandra.Column.class + " did not have a name value, using field name " + columnName);
		} 
		return columnName;
	}

	private Option<Field> keyFieldFor(List<Field> declaredFields) {
		Option<Field> keyFieldOption = ListFunctions.find(declaredFields, new FilterFn<Field>(){
			public boolean apply(Field it) {
				return it.isAnnotationPresent(Key.class);
			}	
		});
		return keyFieldOption;
	}

	private void openConnection() throws TTransportException {
		transport = new TSocket(host, port, timeout);
		TProtocol protocol = new TBinaryProtocol(transport);
		client = new Client(protocol);
		transport.open();
	}

}
