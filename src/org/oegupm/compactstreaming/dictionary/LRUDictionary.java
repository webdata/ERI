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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;

import org.oegupm.compactstreaming.exceptions.DictionaryException;

/**
 * Class for a Dictionary with a removal policy Last Recently Used (triggered when the dictionary reaches the maximum capacity)
 * 
 * @author javi
 * 
 * @param <String>
 * @param <integer>
 */
public class LRUDictionary<String, integer> extends LinkedHashMap<String, integer> {

	private static final long serialVersionUID = -2529587730105931115L;
	private int capacity; // Maximum number of items in the cache.
	Queue<Integer> freeIds;

	public LRUDictionary(int capacity) {
		super(capacity + 1, 1.0f, true); // Pass 'true' for accessOrder.
		this.capacity = capacity;

		// initialize queue of IDs
		freeIds = new LinkedList<Integer>();
		for (int i = 1; i <= this.capacity; i++) {
			freeIds.add(i);
		}
	}

	protected boolean removeEldestEntry(Entry<String, integer> entry) {

		if (size() >= this.capacity) {
			freeIds.add((Integer) entry.getValue());
			return true;
		}
		return false;
		// return (size() > this.capacity);
	}

	/**
	 * @param key
	 * @return the ID element if it has been inserted. Return -1 otherwise.
	 * @throws DictionaryException
	 */
	public Integer insert(String key) throws DictionaryException {

		if (!super.containsKey(key)) {
			if (!freeIds.isEmpty()) {
				// integer id = (integer) freeIds.poll();// retrieve and remove the
				Integer id = freeIds.poll(); // first element

				super.put(key, (integer) id);
				return id;
			} else {

				throw new DictionaryException();
			}
		}
		return -1;
	}

}
