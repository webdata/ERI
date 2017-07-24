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

/**
 * Vocabulary and default values for Compressed Streaming
 * 
 * @author javi
 *
 */
public class CSVocabulary {

	/*
	 * Options for the configuration file
	 */
	public static final String SPEC_MAX_SIZE_DICTIONARY = "max_size_dictionary";
	public static final String SPEC_BLOCK_SIZE = "block_size";
	public static final String STORE_SUBJ_DICTIONARY = "store_subject_dictionary";
	public static final String STORE_OBJ_DICTIONARY = "store_object_dictionary";
	public static final String DISABLE_CONSISTENT_PREDICATES = "disable_consistent_predicates"; //predicates are not related with the same types and/or tags
	
	/*
	 * Values by DEFAULT
	 */
	public static final Boolean DEFAULT_STORE_SUBJ_DICTIONARY = false;
	public static final Boolean DEFAULT_STORE_OBJ_DICTIONARY = false;
	public static final Boolean DEFAULT_CONSISTENT_PREDICATES = false;
	
	// Max number of elements per dictionary before LRU replacement
	public static final int DEFAULT_MAX_SIZE_DICTIONARY = 1023; // 1023IDs+initial 0-ID, 10 bits can be used in log bits
	
	public static final int DEFAULT_BLOCK_SIZE = 1024; // Number of triples in each Block
	

	/* 
	 * Base and other URIs
	 */
	public static final String CS_BASE = "<http://purl.org/CS/cs#"; // FIXME: fake
	public static final String CS_SEQ_BASE = CS_BASE + "seq";
	
	// Sequences of numbers
	public static final String SEQ_TYPE_INT32 = CS_SEQ_BASE + "Int32>";
	public static final String SEQ_TYPE_LOG = CS_SEQ_BASE + "Log>";

	// Misc
	public static final String ORIGINAL_SIZE = CS_BASE + "originalSize>";
	public static final String CS_SIZE = CS_BASE + "hdtSize>";
}
