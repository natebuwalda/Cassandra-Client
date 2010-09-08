package org.nate.cassandra;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
		
		conn.openConnection();
		conn.closeConnection();
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
