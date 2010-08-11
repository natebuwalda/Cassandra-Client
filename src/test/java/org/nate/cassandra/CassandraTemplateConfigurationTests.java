package org.nate.cassandra;

import static org.junit.Assert.assertNotNull;
import junit.framework.Assert;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.nate.cassandra.Cassandra;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class CassandraTemplateConfigurationTests {
	
	@Autowired
	private Cassandra cassandraTemplate;

	@Test
	public void testSimpleWiring() throws Exception {
		assertNotNull(cassandraTemplate);
	}
	
	@Test
	public void testInsertNewUser() throws Exception {
		cassandraTemplate.insertColumnValue("Users", "nbuwalda", "FirstName", "Nathan");
		cassandraTemplate.insertColumnValue("Users", "nbuwalda", "LastName", "Buwalda");
		cassandraTemplate.insertColumnValue("Users", "nbuwalda", "Notes", "hello from earth");
		
		String result = cassandraTemplate.getColumnValue("Users", "nbuwalda", "FirstName");
		Assert.assertEquals("Nathan", result);
	}
}
