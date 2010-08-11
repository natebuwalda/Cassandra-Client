package org.nate.cassandra;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.nate.functions.functors.ActionFn;
import org.nate.functions.functors.FilterFn;
import org.nate.functions.functors.ListFunctions;
import org.nate.functions.functors.exceptions.FunctorException;
import org.nate.functions.options.Option;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class CassandraOperations implements Cassandra {

	private static final char SEPARATOR = ' ';
	private static final Log log = LogFactory.getLog(CassandraOperations.class);

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

			log.debug("key " + key + " has a slice size of " + sliceResults.size());
			count = sliceResults.size();
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform count operation", e);
		} finally {
			closeConnection();
		}
		return count;
	}


	public Object get(Class<? extends Object> clazz, final String key) {
		log.debug("***** CASSANDRA GET COMMAND ISSUED *****");
		try {
			final Object result = clazz.getConstructor(new Class[]{}).newInstance(new Object[]{});
			
			openConnection();
			if (clazz.isAnnotationPresent(ColumnFamily.class)) {
				String columnFamilyName = determineColumnFamily(clazz); 
				SlicePredicate slicePredicate = createSlicePredicate();
				
				log.debug(Joiner.on(SEPARATOR).join("getting slice for key", key));
				final List<ColumnOrSuperColumn> sliceResults = client.get_slice(keyspaceName, key, new ColumnParent(columnFamilyName), slicePredicate, ConsistencyLevel.ONE);
				
				for (ColumnOrSuperColumn columnOrSuper : sliceResults) {
					log.debug(Joiner.on(SEPARATOR).join("*** slice contents:", new String(columnOrSuper.getColumn().getName()), ":", new String(columnOrSuper.getColumn().getValue())));
				}

				List<Field> declaredFields = Lists.newArrayList(clazz.getDeclaredFields());				
				ListFunctions.act(declaredFields, new ActionFn<Field>(){
					public void apply(Field it) {
						try {
							it.setAccessible(true);
							if (it.isAnnotationPresent(Key.class)) {
								it.set(result, key);
							} 
						} catch (Exception e) {
							throw new FunctorException("populateKey function failed", e);
						}
					}	
				});
				
				ListFunctions.act(declaredFields, new ActionFn<Field>(){
					public void apply(final Field field) {
						try {
							field.setAccessible(true);
							ListFunctions.act(sliceResults, new ActionFn<ColumnOrSuperColumn>(){
								public void apply(ColumnOrSuperColumn columnOrSuper) {
									if (new String(columnOrSuper.getColumn().getName()).equals(field.getName())) {
										try {
											field.set(result, convertStringToValue(new String(columnOrSuper.getColumn().getValue()), field.getType()));
										} catch (Exception e) {
											throw new FunctorException("columnValue function failed", e);
										}
									}
								}								
							});
						} catch (Exception e) {
							throw new FunctorException("populateField function failed", e);
						}
					}
				});
			
				log.debug(Joiner.on(SEPARATOR).join("Returning loaded object", result, "for key", key));
				return result;
			} else {
				throw new IllegalArgumentException(Joiner.on(SEPARATOR).join("Class", clazz.getName(), "is not a ColumnFamily"));
			}
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform count operation", e);
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
			log.debug(Joiner.on(SEPARATOR).join("Returning column value", result, "for key:", key, "column:" + column));
		} catch (NotFoundException nfe) {
			log.debug(Joiner.on(SEPARATOR).join("No columns found for key:column",key, ":", column));
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform get string value operation", e);
		} finally {
			closeConnection();
		}
		
		return result;
	}

	public String getHost() {
		return host;
	}
	
	public String getKeyspaceName() {
		return keyspaceName;
	}
	

	public Integer getPort() {
		return port;
	}

	public Integer getTimeout() {
		return timeout;
	}

	public void insert(Class<? extends Object> clazz, final Object insertObject) throws CassandraOperationException {
		log.debug("***** CASSANDRA INSERT COMMAND ISSUED *****");
		try {
			openConnection();
	
			if (clazz.isAnnotationPresent(ColumnFamily.class)) {
				final String columnFamilyName = determineColumnFamily(clazz);
				
				final List<Field> declaredFields = Lists.newArrayList(clazz.getDeclaredFields());	
				Option<Field> keyFieldOption = ListFunctions.find(declaredFields, new FilterFn<Field>() {
					public boolean apply(Field it) throws FunctorException {
						if (it.isAnnotationPresent(Key.class)) {
							return true;
						}
						return false;
					}
					
				});
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
										String columnName = determineColumnName(field);
										ColumnPath columnPath = new ColumnPath();
										columnPath.setColumn(columnName.getBytes());
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
				log.debug(Joiner.on(SEPARATOR).join("Inserted object", insertObject, "successfully"));
			} else {
				throw new IllegalArgumentException(Joiner.on(SEPARATOR).join("Class", clazz.getName(), "is not a ColumnFamily"));
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
			ColumnPath columnPath1 = new ColumnPath();
			columnPath1.setColumn(column.getBytes());
			columnPath1.setColumn_family(columnFamily);
	
			ColumnPath columnPath = columnPath1;
			client.insert(keyspaceName, key, columnPath, value.getBytes(), System.currentTimeMillis(), ConsistencyLevel.ANY);
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform insert string operation", e);
		} finally {
			closeConnection();
		}
	}

	public void remove(Class<? extends Object> clazz, String key) {
		log.debug("***** CASSANDRA REMOVE COMMAND ISSUED *****");
		try {
			openConnection();
			if (clazz.isAnnotationPresent(ColumnFamily.class)) {
				String name = null;
				String annotationNameValue = null;
				
				try {
					Annotation runtimeAnnotation = clazz.getAnnotation(ColumnFamily.class);
					Method nameMethod = ((Class<? extends Annotation>) runtimeAnnotation.getClass()).getMethod("name", new Class<?>[]{});
					annotationNameValue = (String) nameMethod.invoke(runtimeAnnotation, new Object[]{});
				} catch (NoSuchMethodException nsme) {
					//eat this exception because we do not care
					log.error("Annotation " + ColumnFamily.class + " does not have a 'name' method", nsme);
				}
				
				if (annotationNameValue == null || annotationNameValue.isEmpty()) {
					name = clazz.getSimpleName();
					log.debug(ColumnFamily.class + " did not have a name value, using class name " + name);
				} else {
					name = annotationNameValue;
					log.debug(ColumnFamily.class + " annotated class name value " + name);
				}
				String columnFamilyName = name;
				SlicePredicate slicePredicate = createSlicePredicate();
				
				List<ColumnOrSuperColumn> sliceResults1 = Lists.newArrayList(client.get_slice(keyspaceName, key, new ColumnParent(columnFamilyName), slicePredicate, ConsistencyLevel.ONE));
				List<ColumnOrSuperColumn> sliceResults = sliceResults1;
				log.debug(key + " slice size: " + sliceResults.size());
				
				for (ColumnOrSuperColumn columnOrSuper : sliceResults) {
					log.debug("*** removing slice contents: " + new String(columnOrSuper.getColumn().getName()) + ":" + new String(columnOrSuper.getColumn().getValue()));
					ColumnPath columnPath1 = new ColumnPath();
					columnPath1.setColumn(columnOrSuper.getColumn().getName());
					columnPath1.setColumn_family(columnFamilyName);
					ColumnPath columnPath = columnPath1;
					client.remove(keyspaceName, key, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);
				}
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
			ColumnPath columnPath1 = new ColumnPath();
			columnPath1.setColumn(column.getBytes());
			columnPath1.setColumn_family(columnFamily);
	
			ColumnPath columnPath = columnPath1;			
			client.remove(keyspaceName, key, columnPath, System.currentTimeMillis(), ConsistencyLevel.ALL);
			log.debug("Removed column " + column + " for key " + key +  " successfully");
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to perform remove column value operation for key " + key + " column " + column, e);
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
	
	public void update(Object updateObject) {
		log.debug("***** CASSANDRA UPDATE COMMAND ISSUED *****");
		try {
			openConnection();
			
			if (updateObject == null) {
				throw new IllegalArgumentException("Object to be updated cannot be null");
			}
			
			if (updateObject.getClass().isAnnotationPresent(ColumnFamily.class)) {
				String columnFamilyName = null;
				Class<? extends Object> targetClazz = updateObject.getClass();
				String name = null;
				String annotationNameValue = null;
				
				try {
					Annotation runtimeAnnotation = targetClazz.getAnnotation(ColumnFamily.class);
					Method nameMethod = ((Class<? extends Annotation>) runtimeAnnotation.getClass()).getMethod("name", new Class<?>[]{});
					annotationNameValue = (String) nameMethod.invoke(runtimeAnnotation, new Object[]{});
				} catch (NoSuchMethodException nsme) {
					//eat this exception because we do not care
					log.error("Annotation " + ColumnFamily.class + " does not have a 'name' method", nsme);
				}
				
				if (annotationNameValue == null || annotationNameValue.isEmpty()) {
					name = targetClazz.getSimpleName();
					log.debug(ColumnFamily.class + " did not have a name value, using class name " + name);
				} else {
					name = annotationNameValue;
					log.debug(ColumnFamily.class + " annotated class name value " + name);
				}
				columnFamilyName = name;
				Object key1 = null;
				for (Field field1 : updateObject.getClass().getDeclaredFields()) {
					field1.setAccessible(true);
					if (field1.isAnnotationPresent(Key.class)) {
						key1 = field1.get(updateObject);					
						break;
					}
				}
				
				Object key = key1;
				
				if (key == null) {
					throw new IllegalArgumentException("No key found");
				} else {
					List<Mutation> mutations = new ArrayList<Mutation>();
					
					for (Field field : updateObject.getClass().getDeclaredFields()) {
						field.setAccessible(true);
						Object fieldValue1 = null;
						if (field.isAnnotationPresent(org.nate.cassandra.Column.class)) {
							fieldValue1 = field.get(updateObject);
							log.debug("Field " + field.getName() + " value " + fieldValue1);
						}
						Object fieldValue = fieldValue1;
						if (fieldValue != null) {					
							Column column = new Column();
							String name1 = null;
							String annotationNameValue1 = null;
							
							try {
								Annotation runtimeAnnotation = field.getAnnotation(org.nate.cassandra.Column.class);
								Method nameMethod = ((Class<? extends Annotation>) runtimeAnnotation.getClass()).getMethod("name", new Class<?>[]{});
								annotationNameValue1 = (String) nameMethod.invoke(runtimeAnnotation, new Object[]{});
							} catch (NoSuchMethodException nsme) {
								//eat this exception because we do not care
								log.error("Annotation " + org.nate.cassandra.Column.class + " does not have a 'name' method", nsme);
							}
							
							if (annotationNameValue1 == null || annotationNameValue1.isEmpty()) {
								name1 = field.getName();
								log.debug(org.nate.cassandra.Column.class + " did not have a name value, using field name " + name1);
							} else {
								name1 = annotationNameValue1;
								log.debug(org.nate.cassandra.Column.class + " annotated field name value " + name1);
							}
							String determineAnnotatedFieldName = name1;
							column.setName(determineAnnotatedFieldName.getBytes());
							column.setValue(convertValueToString(fieldValue).getBytes());
							column.setTimestamp(System.currentTimeMillis());
							
							ColumnOrSuperColumn columnOrSuper = new ColumnOrSuperColumn();
							columnOrSuper.setColumn(column);
							
							Mutation mutation = new Mutation();
							mutation.setColumn_or_supercolumn(columnOrSuper);
							mutations.add(mutation);
						}
					}
					
					Map<String, List<Mutation>> columnFamilyUpdates = new HashMap<String, List<Mutation>>();
					columnFamilyUpdates.put(columnFamilyName, mutations);
					
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

	@SuppressWarnings("unchecked") 
	private Object convertStringToValue(String string, Class fieldType) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Object converted = null;
		
		if ("String".equals(fieldType.getSimpleName())) {
			converted = string;
		} else if (StringUtils.isNumeric(string)) {
			converted = fieldType.getConstructor(new Class[]{String.class}).newInstance(new Object[]{string});
		}
		
		return converted;
	}

	private String convertValueToString(Object value) {
		String converted = null;
		if (value instanceof String) {
			converted = (String) value;
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
			log.error("Annotation " + org.nate.cassandra.Column.class + " does not have a 'name' method", nsme);
		}
		
		if (columnName == null || columnName.isEmpty()) {
			columnName = field.getName();
			log.debug(org.nate.cassandra.Column.class + " did not have a name value, using field name " + columnName);
		} 
		return columnName;
	}

	private void openConnection() throws TTransportException {
		transport = new TSocket(host, port, timeout);
		TProtocol protocol = new TBinaryProtocol(transport);
		client = new Client(protocol);
		transport.open();
	}

}
