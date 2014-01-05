/*
 * COPYRIGHT(c) 2011 by Jose R. Fernandez
 *
 * This file is part of CluSandra.
 *
 * CluSandra is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CluSandra is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CluSandra.  If not, see <http://www.gnu.org/licenses/>.
 *
 * $Date: 2011-08-23 15:06:55 -0400 (Tue, 23 Aug 2011) $
 * $Revision: 125 $
 * $Author: jose $
 * $Id: CluRunner.java 125 2011-08-23 19:06:55Z jose $
 */
package clusandra.core;

import java.util.Map;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import clusandra.utils.CommandLineSupport;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.beans.BeansException;
import java.util.concurrent.CountDownLatch;

/**
 * The CluRunner is used to invoke any object that implements the CluRunnable
 * interface (e.g., QueueAgent). The CluRunner is given a Spring XML application
 * context file from which it searches for and invokes CluRunnable objects.
 * 
 * @author jfernandez
 * 
 */
public class CluRunner {

	private static final Log LOG = LogFactory.getLog(CluRunner.class);

	// the default name for the Spring XML file; i.e., application context file.
	private String springFile = "streamer.xml";
	private String[] args = null;

	// latches used to communicate with the Runnables
	CountDownLatch startSignal = new CountDownLatch(1);
	CountDownLatch doneSignal = null;

	public CluRunner(String[] args) {
		this.args = args;
	}

	public void start() {

		AbstractXmlApplicationContext applContext = null;

		// Read in any optional command line parameters. For example, the name
		// of a Spring XML application context file.
		String[] unknown = CommandLineSupport.setOptions(this, args);
		if (unknown.length > 0) {
			LOG.warn("Started with unknowns");
		}
		LOG.info("Started with this Spring xml file [" + getSpringFile() + "]");

		try {
			if (!getSpringFile().startsWith("file:")) {
				// Create our Spring application context. The Spring XML
				// file, which defines the context, needs to be in the class
				// path
				ClassPathXmlApplicationContext classPathContext = new ClassPathXmlApplicationContext(
						getSpringFile());
				applContext = (AbstractXmlApplicationContext) classPathContext;
			} else {
				FileSystemXmlApplicationContext filePathContext = new FileSystemXmlApplicationContext(
						getSpringFile());

				applContext = (AbstractXmlApplicationContext) filePathContext;
			}
		} catch (BeansException bexc) {
			LOG.error("ERROR: Unable to create spring context. Exception = "
					+ bexc.getMessage());
			System.exit(1);
		}

		try {
			// Read all the CluRunnable beans in the application context file
			// and invoke their cluRun() method
			Map<String, CluRunnable> runnables = applContext
					.getBeansOfType(CluRunnable.class);

			if (runnables != null && !runnables.isEmpty()) {
				doneSignal = new CountDownLatch(runnables.size());
				for (CluRunnable runnable : runnables.values()) {
					LOG.debug("CluRunner: starting this runner: "
							+ runnable.getName());
					runnable.cluRun(startSignal, doneSignal);
				}
				// let all threads proceed
				startSignal.countDown();
				// wait for all to finish
				doneSignal.await();
			} else {
				LOG.error("ERROR: Spring XML file does not contain any beans"
						+ "of tpye CluRunnable");
			}
		} catch (BeansException bexc) {
			LOG.error("ERROR: Spring XML file does not contain any beans"
					+ "of tpye CluRunnable");
		} catch (Exception exc) {
			LOG.error("ERROR: This exception caught while invoking CluRunnables ["
					+ exc.getMessage());
		}
		System.exit(0);
	}

	/**
	 * This method is invoked by CommandLineSupport to set the the path/name of
	 * the Spring XML file.
	 */
	public void setSpringFile(String springFile) {
		this.springFile = springFile;
	}

	/**
	 * Get the path/name of the Spring XML file being used by this Runner.
	 * 
	 * @return
	 */
	public String getSpringFile() {
		return springFile;
	}

	public static void main(String[] args) throws Exception {
		new CluRunner(args).start();
	}
}
