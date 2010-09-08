package org.nate.cassandra;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class ConnectionFactoryTest {

	private ConnectionFactory factory;
	
	@Before
	public void setUp() throws Exception {
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		factory.setPort(9160);
	}
	
	@Test
	public void createConnection() throws Exception {		
		Connection conn = factory.createConnection();
		Assert.assertNotNull(conn);
		Assert.assertFalse(conn.isOpen());
	}
}
