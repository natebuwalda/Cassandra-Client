package org.nate.cassandra;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Connection {

	private String host;
	private Integer port;
	private Integer timeout;
	private TTransport transport;
	private Client client;
	private boolean open = false;
	
	
	public Connection(String host, Integer port, Integer timeout) {
		this.host = host;
		this.port = port;
		this.timeout = timeout;
	}

	public void closeConnection() throws CassandraOperationException {
		try {
			if (client != null && transport.isOpen()) {
				transport.flush();
				transport.close();
			}
			client = null;
			open = false;
		} catch (Exception e) {
			throw new CassandraOperationException("Unable to close connection", e);
		}
	}
	
	public void openConnection() throws CassandraOperationException {
		transport = new TSocket(host, port, timeout);
		TProtocol protocol = new TBinaryProtocol(transport);
		client = new Client(protocol);
		try {
			transport.open();
			open = true;
		} catch (TTransportException e) {
			throw new CassandraOperationException("unable to open connetion", e);
		}
	}

	public boolean isOpen() {
		return open;
	}
	
}
