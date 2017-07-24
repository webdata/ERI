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

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Configuration file for RDF Compressed Streaming
 * 
 * @author javi
 */
public class CSSpecification extends CSOptions {

	public CSSpecification() {
		super();
	}

	public CSSpecification(String filename) throws IOException {
		super();
		load(filename);
	}

	public void load(String filename) throws IOException {
		FileInputStream fin = new FileInputStream(filename);
		try {
			properties.load(fin);
		} finally {
			fin.close();
		}

	}

}
