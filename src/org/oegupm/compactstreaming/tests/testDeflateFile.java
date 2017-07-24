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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserCallback.RDFCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StopWatch;
import org.rdfhdt.hdt.util.string.UnicodeEscape;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

/**
 * Class to convert an RDF file to a Compressed file using deflate
 * 
 * @author javi
 * 
 */
public class testDeflateFile {

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

	@Parameter(names = "-buffer", description = "Number of bytes for the compression buffer (only valid with -oblivious)")
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
			DeflaterOutputStream deflOutput = new DeflaterOutputStream(output);
			this.outData = new DataOutputStream(deflOutput);

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

			//
			// int i; while((i=fin.read())!=-1){ outData.write((byte)i); outData.flush(); }
			//

			deflOutput.finish();
			output.close();
			// Show basic stats
			System.out.println("- Compression Time: " + sw.stopAndShow());

			System.out.println("\n Testing Decompression");

			// Test decompression
			sw.reset();
			// READ
			InputStream in = new BufferedInputStream(new FileInputStream(streamOutput));

			Inflater inflater = new Inflater();

			InflaterInputStream inf = new InflaterInputStream(in, inflater);
			DataInputStream inData = new DataInputStream(inf);
			OutputStream outputDecompress = new FileOutputStream("salidaDecompress.txt");

			String line = null;
			try {
				TripleString triple = new TripleString();
				while (true) {
					// System.out.println("available:"+inData.available());
					// while ((line = inData.readUTF()) != null) {
					line = inData.readUTF();

					// This may fails in files such as Dbpedia with unescaped characters e.g. "\\\n  launch pad"@en .
					// use this instead by now as an approach
					// outputDecompress.write(line.getBytes("UTF-8"));
					// outputDecompress.write("\n".getBytes("UTF-8"));
					triple.read(UnicodeEscape.escapeString(line));
					outputDecompress.write(triple.asNtriple().toString().getBytes("UTF-8"));

				}
			} catch (EOFException e) {
				inData.close();
			}

			inflater.end();
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
			StopWatch sw = new StopWatch();

			// READ raw file
			InputStream fin = new FileInputStream(rdfInput);

			// WRITE compressed file
			OutputStream output = new BufferedOutputStream(new FileOutputStream(streamOutput));
			DeflaterOutputStream deflOutput = new DeflaterOutputStream(output);

			byte[] buffer = new byte[buffersize];
			int len;
			while ((len = fin.read(buffer)) > 0) {
				deflOutput.write(buffer, 0, len);
			}

			fin.close();
			deflOutput.finish();
			output.close();

			// Show basic stats
			System.out.println("- Compression Time: " + sw.stopAndShow());

			System.out.println("\n Testing Decompression");

			// Test decompression
			sw.reset();
			// READ
			InputStream in = new BufferedInputStream(new FileInputStream(streamOutput));

			Inflater inflater = new Inflater();

			InflaterInputStream inf = new InflaterInputStream(in, inflater);
			DataInputStream inData = new DataInputStream(inf);
			OutputStream outputDecompress = new FileOutputStream("salidaDecompress.txt");

			buffer = new byte[buffersize];

			TripleString triple = new TripleString();
			String[] lines = null;
			if (decompressOnly) { // just write the file without parsing each triple
				while ((len = inData.read(buffer)) > 0) {
					outputDecompress.write(buffer, 0, len);
				}
			} else { // parse triples (by blocks) and write the file in Ntriples containing these triples
				int lastline = 0;
				String buffString = "";
				String remaining = "";
				while ((len = inData.read(buffer)) > 0) {
					buffString = remaining + new String(buffer, 0, len, Charset.forName("UTF-8"));
					lastline = buffString.lastIndexOf('\n');

					remaining = buffString.substring(lastline + 1);
					if (lastline != -1) {
						buffString = buffString.substring(0, lastline);

						lines = buffString.split("\n");

						for (int i = 0; i < lines.length; i++) {
							triple.read(lines[i]);
						}
					}
					outputDecompress.write(buffer, 0, len);
				}
			}

			inflater.end();
			inData.close();
			outputDecompress.close();

			// Show basic stats
			System.out.println("- DeCompression Time: " + sw.stopAndShow());

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		testDeflateFile testDeflate = new testDeflateFile();
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

		public void processTriple(TripleString triple, long pos) {

			if (triple.getObject().length() < 64 * 1024) { // max:64KB
				try {
					outData.writeUTF(triple.toString());

					// outData.flush();
					this.numTriples++;
					this.size += triple.getSubject().length() + triple.getPredicate().length() + triple.getObject().length() + 4; // Spaces
																																	// and
																																	// final
																																	// dot
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				System.err.println("Skip long literal: encoded string too long: " + triple.getObject().length() + " bytes");
			}
		}

		public long getNumTriples() {
			return numTriples;
		}

		public long getSize() {
			return size;
		}
	};
}
