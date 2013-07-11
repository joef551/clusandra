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
 * $Date: 2011-07-28 22:52:59 -0400 (Thu, 28 Jul 2011) $
 * $Revision: 94 $
 * $Author: jose $
 * $Id: CluRunnable.java 94 2011-07-29 02:52:59Z jose $
 */

package clusandra.core;

import org.springframework.beans.factory.BeanNameAware;

/**
 * This interface represents a Clusandra Runnable object. A Clusandra CluRunner
 * object scans its Spring application context for objects that implement this
 * interface. It then invokes them via their cluRun() method. The object
 * typically spawns a background thread to do its deed.
 * 
 * The clusandra.core.QueueAgent is an example of an object that implements this
 * interface.
 * 
 * @author jfernandez
 * 
 */
public interface CluRunnable extends BeanNameAware {
	public void cluRun() throws Exception;

	public void setName(String name);

	public String getName();

	public void shutdown();
}
