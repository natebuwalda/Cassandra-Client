package org.nate.cassandra;

import java.util.Collection;
import java.util.List;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.commons.lang.StringUtils;
import org.nate.cassandra.annotation.ColumnFamily;
import org.nate.functions.functors.FilterFn;
import org.nate.functions.functors.ListFunctions;
import org.nate.functions.functors.TransformFn;
import org.nate.functions.functors.exceptions.FunctorException;
import org.nate.functions.options.Option;
import org.nate.functions.tuple.Triplet;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class Query<T> {

	private Cassandra cassandra;
	private CassandraOperationUtils opUtils = new CassandraOperationUtils();
	private OperationWorker worker;
	
	public Query(Cassandra cassandra) {
		this.cassandra = cassandra;
		this.worker = new OperationWorker(cassandra);
	}
	
	public ResultSet<T> execute(final Class<T> clazz, final Triplet<String, QueryConditional, String>... queryArgs) throws CassandraOperationException {
		if (queryArgs == null || queryArgs.length == 0) {
			throw new CassandraOperationException("You must provide at least one query argument triplet");
		}
		
		try {
			ResultSet<T> consolidatedResults = new ResultSet<T>();
			for (final Triplet<String, QueryConditional, String> queryArg : queryArgs) {
				if (clazz.isAnnotationPresent(ColumnFamily.class)) {
					final String columnFamilyName = opUtils.determineColumnFamily(clazz);
					final SlicePredicate queryPredicate = new SlicePredicate();
					queryPredicate.setColumn_names(Lists.newArrayList(queryArg._1.getBytes()));
				
					final KeyRange keys = new KeyRange();
					keys.setStart_key("");
					keys.setEnd_key("");
				
					List<KeySlice> sliceResults = worker.doWork(new Operation<List<KeySlice>>(cassandra){
						public List<KeySlice> work() throws Exception {
							return client.get_range_slices(cassandra.getKeyspaceName(), new ColumnParent(columnFamilyName), queryPredicate, keys, ConsistencyLevel.ALL);
						}					
					});
				
					List<KeySlice> filteredSlices = ListFunctions.filter(sliceResults, new FilterFn<KeySlice>() {
						public boolean apply(KeySlice it) throws FunctorException {
							Option<ColumnOrSuperColumn> valueExistsOption = ListFunctions.find(it.getColumns(), new FilterFn<ColumnOrSuperColumn>(){
								public boolean apply(ColumnOrSuperColumn it) throws FunctorException {
									boolean exists = false;
									String stringRepresentationOfValue = new String(it.getColumn().getValue());
									Integer numericRepresentationOfValue = null;
									if (StringUtils.isNumeric(stringRepresentationOfValue)) {
										numericRepresentationOfValue = new Integer(stringRepresentationOfValue);
									}
									switch (queryArg._2) {
										case EQUAL:
											exists = stringRepresentationOfValue.equals(queryArg._3);
											break;
										case GREATER_THAN:
											exists = (numericRepresentationOfValue > new Integer(queryArg._3));
											break;
										case GREATER_THAN_EQUAL_TO:
											exists = (numericRepresentationOfValue >= new Integer(queryArg._3));
											break;
										case LESS_THAN_EQUAL_TO:
											exists = (numericRepresentationOfValue <= new Integer(queryArg._3));
											break;
										case LESS_THAN:
											exists = (numericRepresentationOfValue < new Integer(queryArg._3));
											break;
										case NOT_EQUAL:
											exists = !stringRepresentationOfValue.equals(queryArg._3);
											break;
										default:
											exists = stringRepresentationOfValue.equals(queryArg._3);
											break;
									}
									return exists;
								}					
							});
						return valueExistsOption.isSome();
						}
					});
					
					consolidatedResults.results.addAll((Collection<? extends T>) ListFunctions.transform(filteredSlices, new TransformFn<Object, KeySlice>(){
						public Object apply(KeySlice it) throws FunctorException {
							return cassandra.get(clazz, it.getKey());
						}				
					}));
					
				} else {
					throw new IllegalArgumentException(Joiner.on(" ").join("Class", clazz.getName(), "is not a ColumnFamily"));
				}
			}
			return consolidatedResults;
		} catch (Exception e) {
			throw new CassandraOperationException("Query operation failed", e);
		}
	}

}
