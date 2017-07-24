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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Dictionary of prefixes String2ID
 * 
 * @author javi
 * 
 */
public class PrefixDictionary implements Dictionary {

	private TreeMap<String, Integer> prefixes; // key:long string,value:consecutive ID
	private Integer nextID;
	private boolean alphabeticalOrderIDs; // alphabetical order of IDs.

	public PrefixDictionary() {
		prefixes = new TreeMap<String, Integer>();
		nextID = 1;
	}

	
	/*
	 * (non-Javadoc)
	 * @see org.oegupm.compactstreaming.dictionary.Dictionary#load(java.lang.String)
	 */
	@Override
	public void load(String filename) throws IOException {
		load(filename, true);
	}

	public void load(String filename, boolean alphabeticalOrder) throws IOException {
		this.alphabeticalOrderIDs = alphabeticalOrder;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(filename));
			String line;
			nextID = 1;
			while ((line = reader.readLine()) != null) {
				// System.out.println("añadido prefix:"+line+";"+nextID);
				prefixes.put(line, nextID++);
			}
		} catch (FileNotFoundException e) {

		} finally {
			if (reader != null)
				reader.close();
		}
		if (alphabeticalOrder) {
			Iterator<Entry<String, Integer>> entries = prefixes.entrySet().iterator();

			nextID = 1;
			Entry<String, Integer> currentEntry;
			while (entries.hasNext()) {
				currentEntry = entries.next();
				currentEntry.setValue(nextID++);
			}
		}
	}

	
	/* (non-Javadoc)
	 * @see org.oegupm.compactstreaming.dictionary.Dictionary#put(java.lang.String)
	 */
	@Override
	public void put(String prefix) {
		prefixes.put(prefix, nextID++);
	}

	
	/* (non-Javadoc)
	 * @see org.oegupm.compactstreaming.dictionary.Dictionary#get(java.lang.String)
	 */
	@Override
	public Integer get(String key) {
		return prefixes.get(key);
	}

	public Entry<String, Integer> getEntryShared(String key) {
		
		/* Uncomment to reduce the search of prefixes in URIs. It produces an error with BNodes 	
		SortedMap<String, Integer> subset = prefixes.subMap(key.substring(0, key.lastIndexOf('/')), true,
				key.substring(0, key.lastIndexOf('/')) + Character.MAX_VALUE, true);
		// iterate over possibilities

		Iterator<Entry<String, Integer>> entries = subset.entrySet().iterator();
		 */
		Iterator<Entry<String, Integer>> entries= prefixes.entrySet().iterator();
		Entry<String, Integer> currentEntry, retEntry = null;
		while (entries.hasNext()) {
			currentEntry = entries.next();

			if (key.startsWith(currentEntry.getKey()))
				retEntry = currentEntry;
		}

		return retEntry;

	}

	public Integer getValueShared(String key) {

		SortedMap<String, Integer> subset = prefixes.subMap(key.substring(0, key.lastIndexOf('/')), true,
				key.substring(0, key.lastIndexOf('/') + 1), true);
		// iterate over possibilities
		Iterator<Entry<String, Integer>> entries = subset.entrySet().iterator();
		Integer ret = 0;
		Entry<String, Integer> currentEntry;
		while (entries.hasNext()) {
			currentEntry = entries.next();
			// test prefix
			if (key.startsWith(currentEntry.getKey()))
				ret = currentEntry.getValue();
		}
		return ret;

	}

	public SortedMap<String, Integer> getRange(String preffix) {
		return prefixes.subMap(preffix, preffix + Character.MAX_VALUE);
	}

	public SortedMap<String, Integer> getRange(String preffixStart, String preffixEnd) {
		return prefixes.subMap(preffixStart, preffixEnd);
	}

	/* (non-Javadoc)
	 * @see org.oegupm.compactstreaming.dictionary.Dictionary#size()
	 */
	@Override
	public Integer size() {
		return prefixes.size();
	}

	/* (non-Javadoc)
	 * @see org.oegupm.compactstreaming.dictionary.Dictionary#getIteratorEntrySet()
	 */
	@Override
	public Iterator<Map.Entry<String, Integer>> getIteratorEntrySet() {
		return prefixes.entrySet().iterator();
	}

	public Iterator<String> getKeyIteratorByID() {
		if (alphabeticalOrderIDs) {
			return prefixes.keySet().iterator();
		} else {
			// sort by Ids
			List<String> list = new LinkedList<String>(prefixes.keySet());
			Collections.sort(list, new Comparator<String>() {
				public int compare(String o1, String o2) {
					return ((Comparable) (prefixes.get(o1))).compareTo(prefixes.get(o2));
				}
			});

			return list.iterator();

		}
	}

}
