package clusandra.core;

import java.io.Serializable;
import java.util.Properties;
import java.util.Enumeration;

/**
 * CluMessage is the generic message object that is passed across message queues
 * and their QueueAgents. The body of the message can be any serializable
 * object. Each message also includes a set of properties.
 * 
 * @author jfernandez
 * 
 */
public class CluMessage implements Serializable {

	private static final long serialVersionUID = 1061844219769950238L;
	private Properties properties = new Properties();
	private Serializable body;

	public CluMessage() {
	}

	public CluMessage(Serializable body) {
		this.body = body;
	}

	public String getProperty(String key) {
		return properties.getProperty(key);
	}

	public Object setProperty(String key, String value) {
		return properties.setProperty(key, value);
	}

	public Enumeration<?> propertyNames() {
		return properties.propertyNames();
	}

	public Serializable getBody() {
		return body;
	}

	public void setBody(Serializable body) {
		this.body = body;
	}

}
