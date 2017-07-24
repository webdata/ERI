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
package org.oegupm.compactstreaming.tools;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.oegupm.compactstreaming.RDF2CompressedStreaming;
import org.oegupm.compactstreaming.dictionary.PrefixDictionary;
import org.oegupm.compactstreaming.impl.CompressedStreamingImporter;
import org.oegupm.compactstreaming.options.CSSpecification;
import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.util.StopWatch;
import org.rdfhdt.hdt.util.crc.CRC16;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.io.IOUtil;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

/**
 * Class to convert an RDF file to a Compressed Streaming file
 * 
 * @author javi
 * 
 */
public class RDF2StreamFile {

	public String rdfInput = null;
	public String streamOutput = null;
	OutputStream out;
	PrefixDictionary prefixes;
	HashMap<String, Boolean> predicateDiscrete;// ,predicateUniq;

	/* START> Description of parameters */
	@Parameter(description = "<input RDF> <outputRDF_Comp>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-rdftype", description = "Type of RDF Input (ntriples, nquad, n3, turtle, rdfxml)")
	public String rdfType = null;

	@Parameter(names = "-base", description = "Base URI for the dataset")
	public String baseURI = null;

	@Parameter(names = "-config", description = "Config file for the conversion")
	public String configFile = null;

	@Parameter(names = "-prefixes", description = "Prefixes file for the conversion")
	public String prefixesFile = null;

	@Parameter(names = "-discrete", description = "File of discrete predicates for the conversion")
	public String discreteFile = null;

	@Parameter(names = "-uniq", description = "File of predicates whose objects are mostly unrepeated")
	public String uniqFile = null;

	@Parameter(names = "-block", description = "Number of Triples per Block")
	public int blocksize = 0;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet = false;

	/* END> Description of parameters */

	/**
	 * Main execution of the conversion
	 * 
	 * @throws ParserException
	 * @throws IOException
	 */
	public void execute() throws ParserException, IOException {

		if (baseURI == null) {
			baseURI = "file://" + rdfInput;
		}

		CSSpecification spec;
		if (configFile != null) {
			spec = new CSSpecification(configFile);
		} else {
			spec = new CSSpecification();
		}

		prefixes = new PrefixDictionary();
		if (prefixesFile != null) {
			prefixes.load(prefixesFile);
		}

		predicateDiscrete = new HashMap<String, Boolean>();
		if (discreteFile != null) {
			loadConfigPredicates(discreteFile, predicateDiscrete);
		}

		/*
		 * predicateUniq = new HashMap<String, Boolean>(); if (uniqFile != null) { loadConfigPredicates(uniqFile,predicateUniq); }
		 */

		RDFNotation notation = null;
		if (rdfType != null) {
			try {
				notation = RDFNotation.parse(rdfType);
			} catch (IllegalArgumentException e) {
				System.out.println("Notation " + rdfType + " not recognised.");
			}
		}

		if (notation == null) { // guess notation by the filename
			try {
				notation = RDFNotation.guess(rdfInput);
			} catch (IllegalArgumentException e) {
				System.out.println("Could not guess notation for " + rdfInput + " Trying NTriples");
				notation = RDFNotation.NTRIPLES;
			}
		}
		StopWatch sw = new StopWatch(); // start timer

		this.out = new BufferedOutputStream(new FileOutputStream(streamOutput));

		printMagicHeader(); // print magic header in file
		printPrefixes(); // print prefixes in file
		printConfigPredicates("$DSC", predicateDiscrete); // print discrete predicates in file
		// printConfigPredicates("$UNQ",predicateUniq); // print uniq predicates in file

		CompressedStreamingImporter importer = new CompressedStreamingImporter(out); // set up importer

		// RDF2CompressedStreaming cs = importer.loadFromRDF(rdfInput, baseURI, notation, spec, prefixes, predicateDiscrete,predicateUniq);
		// // lunch importer
		RDF2CompressedStreaming cs = importer.loadFromRDF(rdfInput, baseURI, notation, spec, prefixes, predicateDiscrete); // lunch importer
		out.close();

		if (!quiet) { // Show basic stats
			System.out.println("- Conversion Time: " + sw.stopAndShow());
			System.out.println("- Number of Triples: " + cs.getNumTriples());
			System.out.println("- Number of Blocks: " + cs.getNumBlocks());
			System.out.println("- Configuration: " + cs.printConfiguration());
		}

	}

	/**
	 * Load config predicates from a file
	 * 
	 * @param filename
	 *            File with config predicates
	 * @throws IOException
	 */
	public void loadConfigPredicates(String filename, Map<String, Boolean> struct) throws IOException {

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(filename));
			String line;

			while ((line = reader.readLine()) != null) {
				struct.put(line, true);
			}
		} catch (FileNotFoundException e) {

		} finally {
			if (reader != null)
				reader.close();
		}

	}

	public void printMagicHeader() {
		CRCOutputStream outCRC = new CRCOutputStream(this.out, new CRC16());

		try {
			IOUtil.writeString(outCRC, "$CST"); // Write Magic Cookie
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void printPrefixes() {
		if (prefixes.size() > 0) {
			CRCOutputStream outCRC = new CRCOutputStream(this.out, new CRC16());

			try {
				IOUtil.writeString(outCRC, "$PFX"); // Cookie for the prefix information

				outCRC = new CRCOutputStream(out, new CRC8());
				VByte.encode(outCRC, prefixes.size());
				outCRC.writeCRC();
				DataOutputStream outData;

				outCRC.setCRC(new CRC32());
				outData = new DataOutputStream(outCRC);
				Iterator<String> prefs = prefixes.getKeyIteratorByID();

				while (prefs.hasNext()) {
					outData.writeUTF(prefs.next());
				}

				outCRC.writeCRC();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Print config predicates
	 * 
	 * @param cookie
	 * @param struct
	 */
	public void printConfigPredicates(String cookie, Map<String, Boolean> struct) {

		if (struct.size() > 0) {
			CRCOutputStream outCRC = new CRCOutputStream(this.out, new CRC16());

			try {
				IOUtil.writeString(outCRC, cookie); // Cookie for the config predicate information

				outCRC = new CRCOutputStream(out, new CRC8());
				VByte.encode(outCRC, struct.size());
				outCRC.writeCRC();
				DataOutputStream outData;

				outCRC.setCRC(new CRC32());
				outData = new DataOutputStream(outCRC);
				Iterator<String> preds = struct.keySet().iterator();

				while (preds.hasNext()) {
					outData.writeUTF(preds.next());
				}

				outCRC.writeCRC();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Throwable {
		RDF2StreamFile rdf2streamfile = new RDF2StreamFile();
		JCommander com = new JCommander(rdf2streamfile, args);
		com.setProgramName("file2stream");

		if (rdf2streamfile.parameters.size() == 1) {
			System.err.println("No input file specified, reading from standard input.");
			rdf2streamfile.rdfInput = "-";
			rdf2streamfile.streamOutput = rdf2streamfile.parameters.get(0);

		} else if (rdf2streamfile.parameters.size() == 2) {
			rdf2streamfile.rdfInput = rdf2streamfile.parameters.get(0);
			rdf2streamfile.streamOutput = rdf2streamfile.parameters.get(1);

		} else {
			com.usage();
			System.exit(1);
		}

		System.out.println("Converting '" + rdf2streamfile.rdfInput + "' to Stream File'" + rdf2streamfile.streamOutput + "'");

		rdf2streamfile.execute();
		System.out.println("Bye!");
		System.exit(0);
	}

}
