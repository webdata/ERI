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
package org.oegupm.compactstreaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;



/**
 * Interface of CompressedStreaming2RDF implementations
 * 
 * @author javi
 *
 */
public interface CompressedStreaming2RDF {

	/**
	 * Load a file in Compressed Streaming format and export the information to Ntriples
	 * 
	 * @param filename
	 *            The file in Compressed Streaming format
	 * @param out
	 *            the output stream to export the information to Ntriples
	 * @throws IOException
	 */
	public void loadFromCompressedStreaming(InputStream input, OutputStream out) throws IOException;
	
	/**
	 * @return Number of Triples exported
	 */
	public long getNumTriples();

	/**
	 * @return Number of blocks read
	 */
	public long getNumBlocks();

}