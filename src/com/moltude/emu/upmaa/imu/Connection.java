package com.moltude.emu.upmaa.imu;



import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.springframework.util.StringUtils;

import com.kesoftware.imu.IMuException;
import com.kesoftware.imu.Map;
import com.kesoftware.imu.Module;
import com.kesoftware.imu.ModuleFetchResult;
import com.kesoftware.imu.Session;
import com.kesoftware.imu.Terms;

/**
 * This class handles the DB connection and updates.  This is a custom UPMAA layer above the KE iMu API because I 
 * got sick and tired of writing all the god damn connect statements and debugging into all of my g-d code. 
 * 
 * This is also helpful for debugging the dropped imu connections that still hasn't been resolved yet.
 * 
 * @author Scott Williams
 * 
 */

public class Connection {
	private Session session;
	private Map[] _rows;
	private String module = null;
	// imu properties
	private String address	= null;
	private int port 		= -1;
	private String user 	= null;
	private String pass 	= null;

	/**
	 * Constructor 
	 */
	public Connection() {
		this( null );
	}

	/**
	 * 
	 * @param module - The module to connect to 
	 */
	public Connection(String _module ) {
		module = _module;
		loadProperties();
	}
	
	/**
	 * Loads the properties from the imu.properties file
	 * This should be stored 
	 */
	private void loadProperties() {	
		Properties properties = new Properties();
		InputStream is = null;
		try {
			is = this.getClass().getClassLoader().getResourceAsStream("imu.properties");
			
			if(is != null) {
				properties.load(is); 
				is.close();
			} else {
				System.out.println("Could not get imu properties");
				System.exit(0);
			}

			// address of the emu server
			address = properties.getProperty("host");			
			// port imu is running on 
			port = new Integer(properties.getProperty("port")).intValue();
			// username to connect as
			user = properties.getProperty("user","emu");
			// user password
			pass = properties.getProperty("pass");
			
		} catch (FileNotFoundException fnfe) {
			
		} catch (IOException ioe) {
			
		}
	}

	/**
	 * TODO fix this method to print to log4j
	 */
	private void printProperties () {
//		connectionLogFile.appendLine("**** iMu Connection Properties ****");
//		connectionLogFile.appendLine("Host = " + this.address);
//		connectionLogFile.appendLine("Port = " + this.port );
//		connectionLogFile.appendLine("User = " + this.user );
//		// connectionLogFile.appendLine("Pass = " + this.pass );
//		connectionLogFile.appendLine("**** END ****");
	}

	/**
	 * Opens a connection to the EMu Production server
	 * 
	 * @return True if it was successfully connected, False if unable to connect 
	 */
	public boolean connect() {
		int x = 0;
		boolean isConnected = doConnect();

		// This is all debugging until the dropped connection bug is resolved by 
		// KE Software.
		// While unable to connect and number of attempts less than 10
		while (!isConnected && x < 10) { 
			// TODO log4j logging 
			// sleep for 5 seconds
			Thread.currentThread();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				
			}
			x = x + 1;
			// log it
			// try again but do not attempt more than 10 connections before fail
			isConnected = doConnect();
		} // end while
		return isConnected;
	} // end method

	/**
	 * Performs the actual connection to the imu sever 
	 * 
	 * @return True on successful connection 
	 * 			False if unsuccessful
	 */
	private boolean doConnect() {
		try {
			if (isOpen()) {
				session.disconnect();
			}
			session = new Session(address, port);
			session.connect();
			session.login(user, pass);
			return true;
		} catch (IMuException imuex) {
			// This is all debugging until the dropped connection bug is resolved by 
			// KE Software.
			// TODO Error log with imuex data
			session.disconnect();
			return false;
		} catch (Exception e) {
			// TODO Error log
			session.disconnect();
			return false;
		}
	}

	/**
	 * Closes the connection to the EMu Production server
	 * @return True if successfully disconnected 
	 * 			False if unsuccessful
	 */
	public boolean disconnect() {
		session.disconnect();
		if(this.isOpen())
			return false;
		else 
			return true;
	}

	/**
	 * Returns an instance of the Session
	 * 
	 * @return Session CON
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * Searches emu for the provided Terms
	 * 
	 * @param object
	 * @return
	 */
	public Module search(Terms object) {
		Module m = doSearch(object);
		while (m == null) {
			Thread.currentThread();
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Error log
			}
			m = doSearch(object);
		}
		return m;
	}

	/**
	 * 
	 * @return
	 */
	public Map[] getResults() {
		return this._rows;
	}

	public boolean anyMatchingResults(Terms object) {
		return this.anyMatchingResults(object, null);
	}

	/**
	 * Search for emu records and returns true if any exist false if not
	 * 
	 * @param object
	 * @return
	 */
	public boolean anyMatchingResults(Terms object, String fetchColumns) {
		if (fetchColumns == null)
			fetchColumns = "irn";
		Module m = search(object);
		try {
			ModuleFetchResult results = m.fetch("start", 0, -1, fetchColumns);
			_rows = results.getRows();
			// if there are no resutls return false
			if (_rows == null || _rows.length == 0) {
				return false;
			} else
				return true;
		} catch (IMuException e) {
			// TODO Error log
		}
		return false;
	}

	/**
	 * Queries EMu
	 * 
	 * @param object
	 *            - The query to run
	 * @return - The result of that query
	 */
	private Module doSearch(Terms object) {
		try {
			while (!isOpen()) {
				Thread.currentThread();
				Thread.sleep(1000);
				connect();
			}
			Module m = new Module(module, session);
			m.findTerms(object);
			return m;
		} catch (IMuException imuex) {
			// TODO Error log
			return null;
		} catch (Exception e) {
			// TODO Error log
			disconnect();
			return null;
		}
	}

	/**
	 * Searches EMu for the supplied IRN
	 * 
	 * @param key
	 * @return The result of that query
	 */
	public Module searchIRN(long key) {
		try {
			if (!isOpen()) {
				connect();
			}
			Module m = new Module(module, session);
			m.findKey(key);
			return m;
		} catch (Exception e) {
			// TODO ERROR log
			disconnect();
			return null;
		}
	}

	/**
	 * Updates a single record in EMu
	 * 
	 * @param key - The record to updated
	 * @param values - The values to insert or change
	 * @param column - The columns to update
	 * @throws IMuException 
	 */
	public void updateRecord(long key, Map values, String column) throws IMuException {
		try {
			if (!isOpen()) {
				connect();
			}
			Module m = new Module(module, session);
			m.findKey(key);
			m.fetch("start", 0, 1);
			if (column != null)
				m.update("start", 0, 1, values, column);
			else
				m.update("start", 0, 1, values);
		} catch (Exception e) {
			// TODO log4j ERROR msg
			
//			connectionLogFile.appendLine("Error uploading data to EMu");
//			connectionLogFile.appendLine("Map irn key: " + key);
//			for (Entry<String, Object> entry : values.entrySet()) {
//				connectionLogFile.appendLine("key=" + entry.getKey()
//						+ ", value=" + entry.getValue());
//			}
			disconnect();
			throw new IMuException("Error updating reocrds");
		}
	}

	/**
	 * Tests whether the connection to the imu service is open
	 * 
	 * @param con
	 * @return
	 */
	private boolean isOpen() {
		if (session == null)
			return false;
		else if ((session).getContext() != null) 
			return true;
		return false;
	}

	/**
	 * Creates a new record 
	 * 
	 * @param metadata
	 * @return the irn of the new record
	 */
	public long createRecord(Map metadata) {
		try {
			if (!isOpen()) {
				connect();
			}
			Module m = new Module(module, session);
			Map newRecord = m.insert(metadata); 
			return newRecord.getLong("irn"); 
		} catch (Exception e) {
			// TODO Cleanup error logging 
			for(Iterator <String> iterator = metadata.keySet().iterator(); iterator.hasNext(); ) {
				String key = iterator.next();
				Object value = metadata.get(key);
				
				if(value instanceof String) {
					// this.connectionLogFile.appendLine(key + " --> " + value.toString());
				} else if (value instanceof String[] ) {
					for(String val : (String [])value) {
						// this.connectionLogFile.appendLine(key + " --> " + val);
					}
				}
			}
			
			disconnect();
			return -1;
		}
	}

	/**
	 * 
	 * @param objectId
	 * @return
	 */
	public String getObjectName(String objectId) {
		Terms terms = new Terms();
		terms.add("CatObjectNumber", objectId);
		this.connect();
		Module m = this.search(terms);
		try {
			ModuleFetchResult mfr = m.fetch("start", 0, 1, "CatObjectName_tab");
			Map[] results = mfr.getRows();
			this.disconnect();
			if(results.length == 1)
				return StringUtils.arrayToCommaDelimitedString(results[0].getStrings("CatObjectName_tab") );
			else 
				return null;

		} catch (IMuException e) {
			this.disconnect();
			return null;
		}
	}
}