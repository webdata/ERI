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
package org.oegupm.compactstreaming.options;

import java.util.Properties;

/**
 * Options of Compressed Streaming
 * 
 * @author javi
 */
public class CSOptions {

	Properties properties;

	public CSOptions() {
		properties = new Properties();
	}

	public String get(String key) {
		return properties.getProperty(key);
	}

	public void set(String key, String value) {
		properties.setProperty(key, value);
	}

	public void setOptions(String options) {
		for (String item : options.split(";")) {
			int pos = item.indexOf('=');
			if (pos != -1) {
				String property = item.substring(0, pos);
				String value = item.substring(pos + 1);
				properties.setProperty(property, value);
			}
		}
	}

	public long getInt(String string) {
		String val = properties.getProperty(string.trim());
		if (val != null) {
			return Long.parseLong(val);
		}
		return 0;
	}

	public void setInt(String key, long value) {
		properties.setProperty(key, Long.toString(value));
	}

	public void clear() {
		properties.clear();
	}
}
