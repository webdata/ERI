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
package org.oegupm.compactstreaming.tests;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserCallback.RDFCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StopWatch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

/**
 * Class to convert an RDF file to a Compressed file using deflate
 * 
 * @author javi
 * 
 */
public class testHDTFile {

	public class IteratorTripleStringArray implements IteratorTripleString {

		ArrayList<TripleString> elements;
		int index = 0;

		public IteratorTripleStringArray(ArrayList<TripleString> elements) {
			this.elements = elements;
		}

		@Override
		public boolean hasNext() {
			//System.out.println("index:"+index);
			//System.out.println("elements.size:"+elements.size());
			return index < elements.size();
		}

		@Override
		public TripleString next() {
			//System.out.println(elements.get(index));
			return (elements.get(index++));
		}

		@Override
		public void remove() {
			elements.remove(index);

		}

		@Override
		public long estimatedNumResults() {
			return elements.size() - index;
		}

		@Override
		public void goToStart() {
			index = 0;

		}

		@Override
		public boolean hasPrevious() {
			return (index - 1) < elements.size();
		}

		@Override
		public ResultEstimationType numResultEstimation() {
			return ResultEstimationType.EXACT;
		}

		@Override
		public TripleString previous() {
			return (elements.get(--index));
		}

	}

	public int INITIAL_BUFFER_SIZE = 1024;
	public String rdfInput = null;
	public String streamOutput = null;

	/* START> Description of parameters */
	@Parameter(description = "<input RDF> <outputRDF_Comp>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-oblivious", description = "Compress full file without regard to triples")
	public Boolean oblivious = false;

	@Parameter(names = "-decompressOnly", description = "Decompress only, without parsing triples (only valid with -oblivious)")
	public Boolean decompressOnly = false;

	@Parameter(names = "-buffer", description = "Number of bytes for the compression buffer")
	public int buffersize = INITIAL_BUFFER_SIZE;

	DataOutputStream outData = null;

	/* START> Description of parameters */

	/**
	 * Main execution performing a compression and decompression using deflate for each triple string
	 * 
	 * @throws ParserException
	 */
	public void compressAndDecompressTripleString() throws ParserException {

		try {

			StopWatch sw = new StopWatch();

			// WRITE
			OutputStream output = new BufferedOutputStream(new FileOutputStream(streamOutput));

			RDFNotation notation;
			try {
				notation = RDFNotation.guess(rdfInput);
			} catch (IllegalArgumentException e) {
				System.out.println("Could not guess notation for " + rdfInput + " Trying NTriples");
				notation = RDFNotation.NTRIPLES;
			}

			RDFParserCallback parser = RDFParserFactory.getParserCallback(notation);
			TripleAppender appender = new TripleAppender();
			// Load RDF and generate triples
			try {
				parser.doParse(rdfInput, "", notation, appender);
			} catch (ParserException e) {
				e.printStackTrace();
			}

			appender.end();
			//
			// int i; while((i=fin.read())!=-1){ outData.write((byte)i); outData.flush(); }
			//

			output.close();
			// Show basic stats
			System.out.println("- Compression Time: " + sw.stopAndShow());

			System.out.println("\n Testing Decompression");

			// Test decompression
			sw.reset();
			// READ

			PrintStream outputDecompress = new PrintStream("salidaDecompress.txt", "UTF-8");

			for (int i = 1; i < appender.blocks; i++) {
				InputStream in = new BufferedInputStream(new FileInputStream(streamOutput+i));
				HDT hdt = HDTManager.loadHDT(in, null);

				try {
					IteratorTripleString it = hdt.search("", "", "");
					StringBuilder build = new StringBuilder(1024);
					while (it.hasNext()) {
						//System.out.println("new Triple from:"+i);
						TripleString triple = it.next();
						build.delete(0, build.length());
						triple.dumpNtriple(build);
						outputDecompress.print(build);
					}
				} catch (NotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				in.close();
				hdt.close();
			}

			outputDecompress.close();
			

			// Show basic stats
			System.out.println("- DeCompression Time: " + sw.stopAndShow());

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Main execution performing a compression and decompression using deflate by blocks
	 * 
	 * @throws ParserException
	 */
	public void compressAndDecompressFull() throws ParserException {

		try {
			HDT hdt = HDTManager.generateHDT(rdfInput, // Input RDF File ->file containing above
					"file://", // Base URI -> null
					RDFNotation.guess(rdfInput), // Input Type
					new HDTSpecification(), // HDT Options
					null // Progress Listener
					);
			StopWatch sw = new StopWatch();

			// WRITE compressed file
			// OutputStream output = new BufferedOutputStream(new FileOutputStream(streamOutput));
			// DeflaterOutputStream deflOutput = new DeflaterOutputStream(output);

			hdt.saveToHDT(streamOutput, null);

			// output.close();
			hdt.close();

			// Show basic stats
			System.out.println("- Compression Time: " + sw.stopAndShow());

			System.out.println("\n Testing Decompression");

			// Test decompression
			sw.reset();
			// READ
			InputStream in = new BufferedInputStream(new FileInputStream(streamOutput));
			PrintStream outputDecompress = new PrintStream("salidaDecompress.txt", "UTF-8");

			hdt = HDTManager.loadHDT(in, null);

			try {
				IteratorTripleString it = hdt.search("", "", "");
				StringBuilder build = new StringBuilder(1024);
				while (it.hasNext()) {
					TripleString triple = it.next();
					build.delete(0, build.length());
					triple.dumpNtriple(build);
					outputDecompress.print(build);
				}
			} catch (NotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			outputDecompress.close();
			in.close();
			hdt.close();

			// Show basic stats
			System.out.println("- DeCompression Time: " + sw.stopAndShow());

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		testHDTFile testDeflate = new testHDTFile();
		JCommander com = new JCommander(testDeflate, args);
		com.setProgramName("testDeflate");

		if (testDeflate.parameters.size() == 1) {
			System.err.println("No input file specified, reading from standard input.");
			testDeflate.rdfInput = "-";
			testDeflate.streamOutput = testDeflate.parameters.get(0);

		} else if (testDeflate.parameters.size() == 2) {
			testDeflate.rdfInput = testDeflate.parameters.get(0);
			testDeflate.streamOutput = testDeflate.parameters.get(1);

		} else {
			com.usage();
			System.exit(1);
		}

		System.out.println("Converting '" + testDeflate.rdfInput + "' to Stream File'" + testDeflate.streamOutput + "'");

		try {
			if (!testDeflate.oblivious) { // parse the file triple by triple
				System.out.println("- Processing File by triples");
				testDeflate.compressAndDecompressTripleString();
			} else { // parse the file by blocks
				System.out.println("- Processing File by buffer size: " + testDeflate.buffersize);
				testDeflate.compressAndDecompressFull();
			}
		} catch (ParserException e) {
			e.printStackTrace();
		}
		System.out.println("Bye!");
		System.exit(0);
	}

	/**
	 * Class implementing a RDFCallback to process each triple read
	 * 
	 * @author javi
	 * 
	 */
	class TripleAppender implements RDFCallback {

		private long numTriples = 0;
		private long size = 0;
		ArrayList<TripleString> triples = new ArrayList<TripleString>();
		private int blocks = 1;

		public void processTriple(TripleString triple, long pos) {
			TripleString newTriple = new TripleString(triple);
			triples.add(newTriple);
			this.numTriples++;
			if (this.numTriples % buffersize == 0) {
				//System.out.println("saving with numtriples:"+this.numTriples+" and buffersize:"+buffersize);
				//System.out.println("triples:"+this.triples.size());
				IteratorTripleStringArray it = new IteratorTripleStringArray(triples);
				try {
					HDT hdt = HDTManager.generateHDT(it, "file://", new HDTSpecification(), null);
					hdt.saveToHDT(streamOutput + blocks, null);
					hdt.close();
					blocks++;
				} catch (IOException | ParserException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				triples = new ArrayList<TripleString>();
			}

		}

		public long getNumTriples() {
			return numTriples;
		}

		public long getSize() {
			return size;
		}
		public void end(){
			IteratorTripleStringArray it = new IteratorTripleStringArray(triples);
			try {
				HDT hdt = HDTManager.generateHDT(it, "file://", new HDTSpecification(), null);
				hdt.saveToHDT(streamOutput + blocks, null);
				hdt.close();
				blocks++;
			} catch (IOException | ParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	};
}
