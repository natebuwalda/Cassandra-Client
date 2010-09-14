package org.nate.cassandra.aop;

import org.nate.cassandra.Cassandra;

public aspect UnitOfWorkListener {

	pointcut getObject(Cassandra c): execution(* Cassandra.get*(..)) && target(c);
	pointcut insertObject(Cassandra c): execution(* Cassandra.insert*(..)) && target(c);
	pointcut removeObject(Cassandra c): execution(* Cassandra.remove*(..)) && target(c);
	pointcut updateObject(Cassandra c): execution(* Cassandra.update*(..)) && target(c);
	pointcut count(Cassandra c): execution(* Cassandra.count(..)) && target(c);
	pointcut describe(Cassandra c): execution(* Cassandra.describe()) && target(c);

//	Object around(Cassandra c): 
//		getObject(c) {
//			try {
//				c.openConnection();
//				return proceed(c);
//			} catch (Exception e) {
//				throw new CassandraOperationException("exception occured during get call", e);
//			} finally {
//				c.closeConnection();
//			}
//		}
//	
//	void around(Cassandra c):
//		insertObject(c) {
//			try {
//				c.openConnection();
//				proceed(c);
//			} catch (Exception e) {
//				throw new CassandraOperationException("exception occured during insert call", e);
//			} finally {
//				c.closeConnection();
//			}
//		}
//	
//	void around(Cassandra c):
//		removeObject(c) {
//			try {
//				c.openConnection();
//				proceed(c);
//			} catch (Exception e) {
//				throw new CassandraOperationException("exception occured during remove call", e);
//			} finally {
//				c.closeConnection();
//			}
//		}
//	
//	void around(Cassandra c):
//		updateObject(c) {
//			try {
//				c.openConnection();
//				proceed(c);
//			} catch (Exception e) {
//				throw new CassandraOperationException("exception occured during update call", e);
//			} finally {
//				c.closeConnection();
//			}
//		}
//	
//	int around(Cassandra c): 
//		count(c) {
//			try {
//				c.openConnection();
//				return proceed(c);
//			} catch (Exception e) {
//				throw new CassandraOperationException("exception occured during count call", e);
//			} finally {
//				c.closeConnection();
//			}
//		}
//	
//	String around(Cassandra c): 
//		describe(c) {
//			try {
//				c.openConnection();
//				return proceed(c);
//			} catch (Exception e) {
//				throw new CassandraOperationException("exception occured during describe call", e);
//			} finally {
//				c.closeConnection();
//			}
//		}
}
