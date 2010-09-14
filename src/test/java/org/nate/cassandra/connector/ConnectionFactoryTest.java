package org.nate.cassandra.connector;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.nate.cassandra.connector.Connection;
import org.nate.cassandra.connector.ConnectionFactory;

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
