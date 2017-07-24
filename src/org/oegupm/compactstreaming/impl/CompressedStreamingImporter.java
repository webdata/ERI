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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.util.HashMap;
import java.util.Map;

import org.oegupm.compactstreaming.RDF2CompressedStreaming;
import org.oegupm.compactstreaming.dictionary.PrefixDictionary;
import org.oegupm.compactstreaming.exceptions.BlockException;
import org.oegupm.compactstreaming.exceptions.DictionaryException;
import org.oegupm.compactstreaming.options.CSSpecification;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserCallback.RDFCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.triples.TripleString;

/**
 * Importer of RDF to Compressed Streaming
 * 
 * @author javi
 * 
 */
public class CompressedStreamingImporter {

	public OutputStream out;

	class TripleAppender implements RDFCallback {
		private RDF2CompressedStreaming bufferedCompression;
		private long numTriples = 0;
		private long size = 0;

		public TripleAppender(RDF2CompressedStreaming bsc) {
			this.bufferedCompression = bsc;
		}

		public void processTriple(TripleString triple, long pos) {
			try {
				bufferedCompression.insert(triple);
				this.numTriples++;
				this.size += triple.getSubject().length() + triple.getPredicate().length() + triple.getObject().length() + 4; // Spaces and
																																// final dot
			} catch (DictionaryException e) {
				System.err.println("Error in the dictionary ");
				e.printStackTrace();
			} catch (BlockException e) {
				System.err.println("Error in a Block ");
				e.printStackTrace();
			} catch (UTFDataFormatException e) {
				System.err.println("Skip long literal:" + e.getMessage());
			}

		}

		public long getNumTriples() {
			return numTriples;
		}

		public long getSize() {
			return size;
		}
	};

	public CompressedStreamingImporter(OutputStream out) {
		this.out = out;
	}

	public RDF2CompressedStreaming loadFromRDF(String filename, String baseUri, RDFNotation notation) throws IOException, ParserException {
		// By default, Buffered Compression
		return loadFromRDF(filename, baseUri, notation, new CSSpecification(), new PrefixDictionary(), new HashMap<String, Boolean>());
		// return loadFromRDF(filename, baseUri, notation, new CSSpecification(), new PrefixDictionary(), new HashMap<String, Boolean>(),new
		// HashMap<String, Boolean>());
	}

	public RDF2CompressedStreaming loadFromRDF(String filename, String baseUri, RDFNotation notation, CSSpecification spec,
			PrefixDictionary prefixes, Map<String, Boolean> predicateDiscrete) throws IOException, ParserException {
		// PrefixDictionary prefixes, Map<String, Boolean> predicateDiscrete,Map<String, Boolean> predicateUniq) throws IOException,
		// ParserException {

		RDFParserCallback parser = RDFParserFactory.getParserCallback(notation);

		// Create RDF2Streaming Instance
		RDF2CompressedStreaming rdf2CS = RDF2CompressedStreamingFactory.getParser(out, spec, prefixes, predicateDiscrete);
		// RDF2CompressedStreaming rdf2CS = RDF2CompressedStreamingFactory.getParser(out, spec, prefixes, predicateDiscrete,predicateUniq);

		rdf2CS.startProcessing();

		TripleAppender appender = new TripleAppender(rdf2CS);

		// Load RDF and generate triples
		parser.doParse(filename, baseUri, notation, appender);

		rdf2CS.endProcessing();
		rdf2CS.setNtTriplesSize(appender.size);
		rdf2CS.setOriginalFileSize(new File(filename).length());

		return rdf2CS;
	}
}
