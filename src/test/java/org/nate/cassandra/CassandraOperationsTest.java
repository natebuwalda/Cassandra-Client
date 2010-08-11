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
	private CassandraOperations template;
	
	@Before
	public void setup() {
		template = new CassandraOperations();
		template.setKeyspaceName("Keyspace1");
		template.setHost("localhost");
		template.setPort(9160);
		template.setTimeout(1000);
	}
	
	@Test
	public void testA_InsertAndRetrieve() throws Exception {
		template.insertColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name", "nathan");		
		String result = template.getColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name");
		
		Assert.assertEquals("nathan", result);
	}
	
	@Test
	public void testB_RetrieveNotExists() throws Exception {
		String result = template.getColumnValue(STANDARD_1_COLUMN_FAMILY, "1", "name");
		
		Assert.assertNull("should have NotFoundException", result);	
	}

	@Test
	public void testC_Update() throws Exception {
		template.insertColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name", "dan");
		String result = template.getColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name");
		
		Assert.assertEquals("dan", result);
	}
	
	@Test
	public void testD_Count() throws Exception {
		int countResult = template.count(STANDARD_1_COLUMN_FAMILY, "0");
		
		Assert.assertEquals(1, countResult);
	}
	
	@Test
	public void testE_Delete() throws Exception {
		template.removeColumnValue(STANDARD_1_COLUMN_FAMILY, "0", "name");
		int afterDeleteCount = template.count(STANDARD_1_COLUMN_FAMILY, "0");
		
		Assert.assertEquals(0, afterDeleteCount);
	}
	
	@Test
	public void testF_InsertObjectNotColumnFamily() {
		IncorrectlyAnnotatedStandardColumnTestClass failObject = new IncorrectlyAnnotatedStandardColumnTestClass();
		try {
			template.insert(IncorrectlyAnnotatedStandardColumnTestClass.class, failObject);
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
		template.insert(StandardColumnTestClass.class, testObject);
		
		String resultString = template.getColumnValue("Standard1", key, "aStringColumn");
		Assert.assertEquals(value, resultString);
		
		StandardColumnTestClass resultObject = (StandardColumnTestClass) template.get(StandardColumnTestClass.class, key);
		Assert.assertEquals(key, resultObject.getKey());
		Assert.assertEquals(value, resultObject.getAStringColumn());
		Assert.assertEquals(new Integer(intValue), resultObject.getAnIntegerColumn());
	}
	
	@Test
	public void testG_UpdateObjectAndRetrieveIt() throws Exception {
		String key = "objectKey";
		StandardColumnTestClass startingObject = (StandardColumnTestClass) template.get(StandardColumnTestClass.class, key);
		String originalValue = startingObject.getAStringColumn();
		
		startingObject.setAnIntegerColumn(50);
		template.update(startingObject);
		
		StandardColumnTestClass resultObject = (StandardColumnTestClass) template.get(StandardColumnTestClass.class, key);
		Assert.assertEquals(key, resultObject.getKey());
		Assert.assertEquals(new Integer(50), resultObject.getAnIntegerColumn());
		Assert.assertEquals(originalValue, resultObject.getAStringColumn());
	}
	
	@Test
	public void testH_CleanUpTheMess() throws Exception {
		
		StandardColumnTestClass result = (StandardColumnTestClass) template.get(StandardColumnTestClass.class, "objectKey");
		System.out.println(result);

		template.remove(StandardColumnTestClass.class, "objectKey");
		int afterDeleteCount = template.count(STANDARD_1_COLUMN_FAMILY, "objectKey");
		
		Assert.assertEquals(0, afterDeleteCount);
	}
	
	public class IncorrectlyAnnotatedStandardColumnTestClass {
		
	}
}
