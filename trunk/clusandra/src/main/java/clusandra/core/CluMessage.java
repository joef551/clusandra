package clusandra.core;

import java.io.Serializable;
import java.util.Properties;
import java.util.Enumeration;

/**
 * CluMessage is the generic message object that is passed through the CluSandra
 * framework (i.e., across the message queues). The CluMessage encapsulates a
 * serializable object that is the entity body of the CluMessage. The entity
 * body can be an serializable object and is specific to the set of Processors
 * that act on it. properties.
 * 
 * @author jfernandez
 * 
 */
public class CluMessage implements Serializable {

	private static final long serialVersionUID = 1061844219769950238L;
	// the set of propeties that can be associated with the message
	private Properties properties = new Properties();
	// the entity body
	private Serializable body;

	public CluMessage() {
	}

	public CluMessage(Serializable body) {
		this.body = body;
	}

	/**
	 * Get the value of the property specified by the given key.
	 * 
	 * @param key
	 * @return
	 */
	public String getProperty(String key) {
		return properties.getProperty(key);
	}

	/**
	 * Set the given property in the property list. Each key and its
	 * corresponding value in the property list is a string.
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Object setProperty(String key, String value) {
		return properties.setProperty(key, value);
	}

	/**
	 * Returns an enumeration of all the keys in this property list.
	 * 
	 * @return
	 */
	public Enumeration<?> propertyNames() {
		return properties.propertyNames();
	}

	/**
	 * Returns this CluMessage's entity body.
	 * 
	 * @return
	 */
	public Serializable getBody() {
		return body;
	}

	public void setBody(Serializable body) {
		this.body = body;
	}

}
