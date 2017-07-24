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
package org.oegupm.compactstreaming.impl;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.oegupm.compactstreaming.CSVocabulary;
import org.oegupm.compactstreaming.RDF2CompressedStreaming;
import org.oegupm.compactstreaming.dictionary.PrefixDictionary;
import org.oegupm.compactstreaming.enums.RDF2CompressedStreamingParsers;
import org.oegupm.compactstreaming.exceptions.NotImplementedException;
import org.oegupm.compactstreaming.options.CSSpecification;

/**
 * Factory of Compresses Streaming implementations
 * 
 * @author javi
 * 
 */
public class RDF2CompressedStreamingFactory {

	public static final String MOD_Buffered = "buffer";
	public static final String MOD_Time = "time";

	public static RDF2CompressedStreaming getParser(OutputStream out, RDF2CompressedStreamingParsers parser) {

		if (parser == RDF2CompressedStreamingParsers.Buffered) {
			return new BufferedCompressedStreaming(out, new CSSpecification(), new PrefixDictionary(), new HashMap<String, Boolean>());
			// ,new HashMap<String,Boolean>());
		}
		if (parser == RDF2CompressedStreamingParsers.BufferedNoDict) {
			return new BufferedCompressedStreamingNoDictionary(out, new CSSpecification(), new PrefixDictionary(),
					new HashMap<String, Boolean>());
		}

		if (parser == RDF2CompressedStreamingParsers.Time) {
			return new TimeBufferedCompressedStreaming(out, new CSSpecification(), new PrefixDictionary(), new HashMap<String, Boolean>());
		}

		throw new NotImplementedException("Mode not found for parser: " + parser);
	}

	public static RDF2CompressedStreaming getParser(OutputStream out, CSSpecification spec, PrefixDictionary prefixes,
			Map<String, Boolean> predicateDiscrete) {
		// ,Map<String, Boolean> predicateUniq) {

		String rdf2StreamingMode = spec.get("rdf2StreamingMode");
		String specStoreDictionary = spec.get(CSVocabulary.STORE_OBJ_DICTIONARY);

		Boolean store_obj_dictionary = CSVocabulary.DEFAULT_STORE_OBJ_DICTIONARY;
		if (specStoreDictionary != null && !"".equals(specStoreDictionary)) {
			store_obj_dictionary = Boolean.parseBoolean(specStoreDictionary);
		}

		if (RDF2CompressedStreamingFactory.MOD_Buffered.equals(rdf2StreamingMode)) {
			if (store_obj_dictionary)
				return new BufferedCompressedStreaming(out, spec, prefixes, predicateDiscrete);
			// ,predicateUniq);
			else
				return new BufferedCompressedStreamingNoDictionary(out, spec, prefixes, predicateDiscrete);
		}

		if (RDF2CompressedStreamingFactory.MOD_Time.equals(rdf2StreamingMode))
			return new TimeBufferedCompressedStreaming(out, spec, prefixes, predicateDiscrete);

		if (rdf2StreamingMode == null || "".equals(rdf2StreamingMode)) // make buffer by default
			if (store_obj_dictionary)
				return new BufferedCompressedStreaming(out, spec, prefixes, predicateDiscrete);
			// ,predicateUniq);
			else
				return new BufferedCompressedStreamingNoDictionary(out, spec, prefixes, predicateDiscrete);

		throw new NotImplementedException("Mode not found for parser: " + rdf2StreamingMode);
	}

}
