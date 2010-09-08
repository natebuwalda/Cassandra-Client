package org.nate.cassandra;

public class ConnectionFactory {

	private String host;
	private Integer port;
	private Integer timeout = 1000;
	
	public ConnectionFactory() {
		// default for spring configuration
	}
	
	public ConnectionFactory(String host, Integer port, Integer timeout) {
		this.host = host;
		this.port = port;
		this.timeout = timeout;
	}

	public Connection createConnection() {
		return new Connection(host, port, timeout);
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public Integer getTimeout() {
		return timeout;
	}

	public void setTimeout(Integer timeout) {
		this.timeout = timeout;
	}
	
	
}
