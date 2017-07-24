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

import java.io.UTFDataFormatException;

import org.oegupm.compactstreaming.exceptions.BlockException;
import org.oegupm.compactstreaming.exceptions.DictionaryException;
import org.rdfhdt.hdt.triples.TripleString;

/**
 * Interface of RDF2CompressedStreaming implementations
 * 
 * @author javi
 *
 */
public interface RDF2CompressedStreaming {

	public abstract void insert(TripleString triple)
			throws DictionaryException, BlockException, UTFDataFormatException;

	public abstract void startProcessing();
	
	public abstract void endProcessing();

	public abstract long getOriginalFileSize();

	public abstract void setOriginalFileSize(long originalFileSize);

	public abstract long getNtTriplesSize();

	public abstract void setNtTriplesSize(long ntTriplesSize);

	public abstract long getNumTriples();
	
	public abstract long getNumBlocks();
	
	public abstract String printConfiguration();

}