/*
 * Copyright (C) 2014 
 *  - Ontology Engineering Group (OEG), http://www.oeg-upm.net/
 *  - Javier D. Fernandez, <jdfernandez@fi.upm.es>
 * 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.oegupm.compactstreaming.dictionary;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface for a dictionary of String2IDs
 * 
 * @author javi
 *
 */
public interface Dictionary {

	/**
	 * Load dictionary serialized in a file
	 * 
	 * @param filename
	 * @throws IOException
	 */
	public abstract void load(String filename) throws IOException;

	/**
	 * Insert new key
	 * 
	 * @param key
	 */
	public abstract void put(String key);

	/**
	 * Get value for the given key
	 * 
	 * @param key
	 * @return
	 */
	public abstract Integer get(String key);

	/**
	 * Get Number of elements in the dictionary
	 * 
	 * @return
	 */
	public abstract Integer size();

	/**
	 * Get an iterator of the entry set
	 * 
	 * @return
	 */
	public abstract Iterator<Map.Entry<String, Integer>> getIteratorEntrySet();

	

	

}