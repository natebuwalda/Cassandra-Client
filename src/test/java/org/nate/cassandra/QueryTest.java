package org.nate.cassandra;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.nate.cassandra.connector.ConnectionFactory;
import org.nate.cassandra.connector.ConnectionPool;
import org.nate.functions.tuple.Triplet;

import com.google.common.collect.Lists;

public class QueryTest {

	private StandardColumnTestClass firstObject;
	private StandardColumnTestClass secondObject;
	private StandardColumnTestClass thirdObject;
	private static final String STANDARD_1_COLUMN_FAMILY = "Standard1";
	private CassandraOperations cassandra;
	private Query<StandardColumnTestClass> query;
	
	@Before
	public void setup() {
		cassandra = new CassandraOperations();
		cassandra.setKeyspaceName("Keyspace1");

		ConnectionFactory testFactory = new ConnectionFactory("localhost", 9160, 1000);
		List<ConnectionFactory> factories = Lists.newArrayList(testFactory);
		ConnectionPool connectionPool = new ConnectionPool(1, 2000L, factories);
		cassandra.setConnectionPool(connectionPool);
		
		query = new Query<StandardColumnTestClass>(cassandra);
		
		firstObject = new StandardColumnTestClass();
		firstObject.setKey("firstKey");
		firstObject.setAStringColumn("AAAAA");
		firstObject.setAnIntegerColumn(10);
		
		secondObject = new StandardColumnTestClass();
		secondObject.setKey("secondKey");
		secondObject.setAStringColumn("BBBBB");
		secondObject.setAnIntegerColumn(20);
		
		thirdObject = new StandardColumnTestClass();
		thirdObject.setKey("thirdKey");
		thirdObject.setAStringColumn("CCCCC");
		thirdObject.setAnIntegerColumn(30);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testA_query() throws Exception {
		cassandra.insert(firstObject);
		cassandra.insert(secondObject);
		cassandra.insert(thirdObject);
		
		int afterInsertCount = cassandra.count(STANDARD_1_COLUMN_FAMILY, "firstKey")
			+ cassandra.count(STANDARD_1_COLUMN_FAMILY, "secondKey")
			+ cassandra.count(STANDARD_1_COLUMN_FAMILY, "thirdKey");

		Assert.assertEquals(6, afterInsertCount);
		
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, new Triplet("anIntegerColumn", QueryConditional.EQUAL, "10")).ascendingBy("anIntegerColumn").results;
		Assert.assertEquals(firstObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(0).getAStringColumn());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testB_queryGreaterThan() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, new Triplet("anIntegerColumn", QueryConditional.GREATER_THAN, "10")).ascendingBy("anIntegerColumn").results;
		Assert.assertEquals(secondObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(secondObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(secondObject.getAStringColumn(), results.get(0).getAStringColumn());
		Assert.assertEquals(thirdObject.getKey(), results.get(1).getKey());
		Assert.assertEquals(thirdObject.getAnIntegerColumn(), results.get(1).getAnIntegerColumn());
		Assert.assertEquals(thirdObject.getAStringColumn(), results.get(1).getAStringColumn());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testC_queryGreaterThanEqual() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, new Triplet("anIntegerColumn", QueryConditional.GREATER_THAN_EQUAL_TO, "10")).descendingBy("anIntegerColumn").results;
		Assert.assertEquals(firstObject.getKey(), results.get(2).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(2).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(2).getAStringColumn());
		Assert.assertEquals(secondObject.getKey(), results.get(1).getKey());
		Assert.assertEquals(secondObject.getAnIntegerColumn(), results.get(1).getAnIntegerColumn());
		Assert.assertEquals(secondObject.getAStringColumn(), results.get(1).getAStringColumn());
		Assert.assertEquals(thirdObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(thirdObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(thirdObject.getAStringColumn(), results.get(0).getAStringColumn());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testD_queryLessThanEqual() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, new Triplet("anIntegerColumn", QueryConditional.LESS_THAN_EQUAL_TO, "30")).ascendingBy("anIntegerColumn").results;
		Assert.assertEquals(firstObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(0).getAStringColumn());
		Assert.assertEquals(secondObject.getKey(), results.get(1).getKey());
		Assert.assertEquals(secondObject.getAnIntegerColumn(), results.get(1).getAnIntegerColumn());
		Assert.assertEquals(secondObject.getAStringColumn(), results.get(1).getAStringColumn());
		Assert.assertEquals(thirdObject.getKey(), results.get(2).getKey());
		Assert.assertEquals(thirdObject.getAnIntegerColumn(), results.get(2).getAnIntegerColumn());
		Assert.assertEquals(thirdObject.getAStringColumn(), results.get(2).getAStringColumn());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testE_queryLessThan() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, new Triplet("anIntegerColumn", QueryConditional.LESS_THAN, "30")).results;
		Assert.assertEquals(firstObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(0).getAStringColumn());
		Assert.assertEquals(secondObject.getKey(), results.get(1).getKey());
		Assert.assertEquals(secondObject.getAnIntegerColumn(), results.get(1).getAnIntegerColumn());
		Assert.assertEquals(secondObject.getAStringColumn(), results.get(1).getAStringColumn());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testF_queryNotEqual() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, new Triplet("anIntegerColumn", QueryConditional.NOT_EQUAL, "20")).results;
		Assert.assertEquals(firstObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(0).getAStringColumn());
		Assert.assertEquals(thirdObject.getKey(), results.get(1).getKey());
		Assert.assertEquals(thirdObject.getAnIntegerColumn(), results.get(1).getAnIntegerColumn());
		Assert.assertEquals(thirdObject.getAStringColumn(), results.get(1).getAStringColumn());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testG_queryEquals_StringValue() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, new Triplet("aStringColumn", QueryConditional.EQUAL, "AAAAA")).results;
		Assert.assertEquals(firstObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(0).getAStringColumn());
	}
	
	@Test
	public void testH_queryNotEqual_StringValue() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, new Triplet("aStringColumn", QueryConditional.NOT_EQUAL, "BBBBB")).results;
		Assert.assertEquals(firstObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(0).getAStringColumn());
		Assert.assertEquals(thirdObject.getKey(), results.get(1).getKey());
		Assert.assertEquals(thirdObject.getAnIntegerColumn(), results.get(1).getAnIntegerColumn());
		Assert.assertEquals(thirdObject.getAStringColumn(), results.get(1).getAStringColumn());
	}
	
	@Test
	public void testI_queryMultipleArgs() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, 
														new Triplet("aStringColumn", QueryConditional.EQUAL, "BBBBB"), 
														new Triplet("anIntegerColumn", QueryConditional.EQUAL, "10"))
														.results;
		Assert.assertEquals(secondObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(secondObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(secondObject.getAStringColumn(), results.get(0).getAStringColumn());
		Assert.assertEquals(firstObject.getKey(), results.get(1).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(1).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(1).getAStringColumn());
	}
	
	@Test(expected=CassandraOperationException.class)
	public void testJ_queryNoArgs() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, null).results;
	}
	
	@Test
	public void testK_twoQueriesSingleArgs_or() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, 
														new Triplet("aStringColumn", QueryConditional.EQUAL, "AAAAA"))
														.or(query.execute(StandardColumnTestClass.class, 
																new Triplet("anIntegerColumn", QueryConditional.EQUAL, "30")))
														.results;
		Assert.assertEquals(2, results.size());
		Assert.assertEquals(firstObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(0).getAStringColumn());
		Assert.assertEquals(thirdObject.getKey(), results.get(1).getKey());
		Assert.assertEquals(thirdObject.getAnIntegerColumn(), results.get(1).getAnIntegerColumn());
		Assert.assertEquals(thirdObject.getAStringColumn(), results.get(1).getAStringColumn());
	}
	
	@Test
	public void testK_twoQueriesSingleArgs_and() throws Exception {
		List<StandardColumnTestClass> results = query.execute(StandardColumnTestClass.class, 
														new Triplet("aStringColumn", QueryConditional.EQUAL, "AAAAA"))
														.and(query.execute(StandardColumnTestClass.class, 
																new Triplet("anIntegerColumn", QueryConditional.GREATER_THAN_EQUAL_TO, "10")))
														.results;
		Assert.assertEquals(1, results.size());
		Assert.assertEquals(firstObject.getKey(), results.get(0).getKey());
		Assert.assertEquals(firstObject.getAnIntegerColumn(), results.get(0).getAnIntegerColumn());
		Assert.assertEquals(firstObject.getAStringColumn(), results.get(0).getAStringColumn());
	}
	
	@Test
	public void testK_CleanUpTheMess() throws Exception {		
		cassandra.remove(StandardColumnTestClass.class, "firstKey");
		cassandra.remove(StandardColumnTestClass.class, "secondKey");
		cassandra.remove(StandardColumnTestClass.class, "thirdKey");
		int afterDeleteCount = cassandra.count(STANDARD_1_COLUMN_FAMILY, "firstKey")
								+ cassandra.count(STANDARD_1_COLUMN_FAMILY, "secondKey")
								+ cassandra.count(STANDARD_1_COLUMN_FAMILY, "thirdKey");
		
		Assert.assertEquals(0, afterDeleteCount);
		
		cassandra.remove(StandardColumnTestClass.class, "firstObject");
		cassandra.remove(StandardColumnTestClass.class, "secondObject");
		cassandra.remove(StandardColumnTestClass.class, "thirdObject");
	}
}
