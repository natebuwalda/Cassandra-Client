package org.nate.cassandra;

import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.nate.cassandra.CassandraOperationException;
import org.nate.cassandra.CassandraOperations;

public class CassandraOperationsTest {

	private static final String STANDARD_1_COLUMN_FAMILY = "Standard1";
	/* NOTE:
	 * 
	 * Tests are lettered because they need to be executed in order (for now)
	 * Sorry for the silly.  --Nate
	 * 
	 */
	private CassandraOperations cassandra;
	
	@Before
	public void setup() {
		cassandra = new CassandraOperations();
		cassandra.setKeyspaceName("Keyspace1");
		cassandra.setHost("localhost");
		cassandra.setPort(9160);
		cassandra.setTimeout(1000);
	}
	
	@Test
	public void testA_InsertAndRetrieve() throws Exception {
		cassandra.insertColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name", "nathan");		
		String result = cassandra.getColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name");
		
		Assert.assertEquals("nathan", result);
	}
	
	@Test
	public void testB_RetrieveNotExists() throws Exception {
		String result = cassandra.getColumnValue(STANDARD_1_COLUMN_FAMILY, "1", "name");
		
		Assert.assertNull("should have NotFoundException", result);	
	}

	@Test
	public void testC_Update() throws Exception {
		cassandra.insertColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name", "dan");
		String result = cassandra.getColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name");
		
		Assert.assertEquals("dan", result);
	}
	
	@Test
	public void testD_Count() throws Exception {
		int countResult = cassandra.count(STANDARD_1_COLUMN_FAMILY, "0");
		
		Assert.assertEquals(1, countResult);
	}
	
	@Test
	public void testE_Delete() throws Exception {
		cassandra.removeColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name");
		int afterDeleteCount = cassandra.count(STANDARD_1_COLUMN_FAMILY, "0");
		
		Assert.assertEquals(0, afterDeleteCount);
	}
	
	@Test
	public void testF_InsertObjectNotColumnFamily() {
		IncorrectlyAnnotatedStandardColumnTestClass failObject = new IncorrectlyAnnotatedStandardColumnTestClass();
		try {
			cassandra.insert(IncorrectlyAnnotatedStandardColumnTestClass.class, failObject);
			Assert.fail("CassandraOperationException expected");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof CassandraOperationException);
		}
	}
	
	@Test
	public void testF_InsertObjectAndRetrieveIt() throws Exception {		
		StandardColumnTestClass testObject = new StandardColumnTestClass();
		String key = "objectKey";
		testObject.setKey(key);
		Random random = new Random();
		String value = "my value " + random.nextInt();
		testObject.setAStringColumn(value);
		int intValue = 2;
		testObject.setAnIntegerColumn(intValue);
		testObject.setAnUnannotatedField("unannotated!");
		cassandra.insert(StandardColumnTestClass.class, testObject);
		
		String resultString = cassandra.getColumnValue("Standard1", key, "aStringColumn");
		Assert.assertEquals(value, resultString);
		
		StandardColumnTestClass resultObject = (StandardColumnTestClass) cassandra.get(StandardColumnTestClass.class, key);
		Assert.assertEquals(key, resultObject.getKey());
		Assert.assertEquals(value, resultObject.getAStringColumn());
		Assert.assertEquals(new Integer(intValue), resultObject.getAnIntegerColumn());
		Assert.assertNull(resultObject.getAnUnannotatedField());
	}
	
	@Test(expected=CassandraOperationException.class)
	public void testF2_InsertObject_NullKey() throws Exception {		
		StandardColumnTestClass testObject = new StandardColumnTestClass();	
		cassandra.insert(StandardColumnTestClass.class, testObject);	
	}
	
	@Test
	public void testG_UpdateObjectAndRetrieveIt() throws Exception {
		String key = "objectKey";
		StandardColumnTestClass startingObject = (StandardColumnTestClass) cassandra.get(StandardColumnTestClass.class, key);
		String originalValue = startingObject.getAStringColumn();
		startingObject.setAnUnannotatedField("more unannotated!");
		startingObject.setAnIntegerColumn(50);
		cassandra.update(startingObject);
		
		StandardColumnTestClass resultObject = (StandardColumnTestClass) cassandra.get(StandardColumnTestClass.class, key);
		Assert.assertEquals(key, resultObject.getKey());
		Assert.assertEquals(new Integer(50), resultObject.getAnIntegerColumn());
		Assert.assertEquals(originalValue, resultObject.getAStringColumn());
		Assert.assertNull(resultObject.getAnUnannotatedField());
	}
	
	@Test
	public void testH_CleanUpTheMess() throws Exception {
		
		StandardColumnTestClass result = (StandardColumnTestClass) cassandra.get(StandardColumnTestClass.class, "objectKey");
		System.out.println(result);

		cassandra.remove(StandardColumnTestClass.class, "objectKey");
		int afterDeleteCount = cassandra.count(STANDARD_1_COLUMN_FAMILY, "objectKey");
		
		Assert.assertEquals(0, afterDeleteCount);
	}
	
	public class IncorrectlyAnnotatedStandardColumnTestClass {
		
	}
}
