package org.nate.cassandra.connector;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.nate.cassandra.CassandraOperationException;
import org.nate.cassandra.connector.Connection;
import org.nate.cassandra.connector.ConnectionFactory;
import org.nate.cassandra.connector.ConnectionPool;

import com.google.common.collect.Lists;

public class ConnectionPoolTest {

	private TestConnectionPool pool;

	@Before
	public void setup() {
		pool = new TestConnectionPool();
		
		List<ConnectionFactory> factories = Lists.newArrayList(new ConnectionFactory("localhost", 9160, 1000));
		pool.setFactories(factories);
	}
	
	@Test
	public void initialize() throws Exception {
		Map<String, Map<Connection, Boolean>> initialMap = pool.simulateInitialize();
		
		Assert.assertNotNull(initialMap);
		Assert.assertEquals(1, initialMap.size());
		Assert.assertNotNull(initialMap.get("localhost"));
	}
	
	@Test
	public void getConnection() throws Exception {	
		Connection conn = pool.getConnection();
		Assert.assertNotNull(conn);
		Assert.assertEquals("localhost", conn.getHost());
		Assert.assertEquals(new Integer(9160), conn.getPort());
	}
	 
	@Test
	public void multipleConnectionsWaitAndRelease() throws Exception {
		Connection conn1 = pool.getConnection();
		Assert.assertNotNull(conn1);

		try {
			Connection conn2 = pool.getConnection();
			Assert.fail("Expected timeout exception");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof CassandraOperationException);
			Assert.assertEquals("Timed out waiting for connection", e.getMessage());
		}
		
		pool.releaseConnection(conn1);
		
		Connection conn3 = pool.getConnection();
		Assert.assertNotNull(conn3);
	}
	
	private class TestConnectionPool extends ConnectionPool {
		
		public TestConnectionPool() {
			super();
		}
		
		public Map<String, Map<Connection, Boolean>> simulateInitialize() {
			return super.initializePool();
		}
		
	}
}
