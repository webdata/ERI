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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.oegupm.compactstreaming.CSVocabulary;
import org.oegupm.compactstreaming.CompressedStreaming2RDF;
import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.exceptions.CRCException;
import org.rdfhdt.hdt.util.crc.CRC16;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCInputStream;
import org.rdfhdt.hdt.util.io.IOUtil;

/**
 * Class to export from Compressed Streaming
 * 
 * @author javi
 * 
 */
public class CompressedStreaming2RDFExporterFactory {

	InputStream in;
	private ArrayList<String> prefixes;
	private Boolean store_subj_dictionary = CSVocabulary.DEFAULT_STORE_SUBJ_DICTIONARY;
	private Boolean disable_consistent_predicates = CSVocabulary.DEFAULT_CONSISTENT_PREDICATES;
	private Boolean store_obj_dictionary = CSVocabulary.DEFAULT_STORE_OBJ_DICTIONARY;

	public CompressedStreaming2RDF loadFromFile(String filename, OutputStream out) throws IOException {

		try {
			in = new BufferedInputStream(new FileInputStream(filename));
		} catch (FileNotFoundException e) {
			System.err.println("Error: '" + filename + "' file not found!");
			e.printStackTrace();
		}
		// read magic number
		CRCInputStream inCRC = new CRCInputStream(in, new CRC16());

		// Cookie
		String keyword = IOUtil.readChars(in, 4);
		if (!keyword.equals("$CST")) {
			inCRC.close();
			throw new IOException("Non-Compressed Stream Format");
		}

		// read block or END Cookie
		keyword = IOUtil.readChars(in, 4);

		// read prefixes
		DataInputStream inData = new DataInputStream(null);
		if (keyword.equals("$PFX")) {
			// READ Prefixes HEADER
			inCRC = new CRCInputStream(in, new CRC8());
			int numstrings = (int) VByte.decode(inCRC);

			if (!inCRC.readCRCAndCheck()) {
				inData.close();
				throw new CRCException("CRC Error while reading Prefix Header.");

			}

			if (numstrings > 0) {

				// READ Prefixes
				inCRC.setCRC(new CRC32());
				inData = new DataInputStream(inCRC);
				String pref;
				prefixes = new ArrayList<String>(numstrings + 1); // initialize
				prefixes.add(0, "");
				for (int i = 0; i < numstrings; i++) {

					pref = inData.readUTF();
					// System.out.println("prefix read: " + pref);
					prefixes.add(i + 1, pref);
				}

				if (!inCRC.readCRCAndCheck()) {
					throw new CRCException("CRC Error while reading new prefixes.");
				}

			}
			keyword = IOUtil.readChars(in, 4);
		}

		Map<String, Boolean> tempDiscrete = new HashMap<String, Boolean>();
		// read discrete predicates
		if (keyword.equals("$DSC")) {
			loadConfigPredicate(tempDiscrete);
			keyword = IOUtil.readChars(in, 4);
		}
		/*
		 * Map<String, Boolean> tempUniq = new HashMap<String, Boolean>();
		 * 
		 * // read uniq predicates if (keyword.equals("$UNQ")) { loadConfigPredicate(tempUniq); keyword = IOUtil.readChars(in, 4); }
		 */
		// read configuration if present
		if (keyword.equals("$CNF")) {
			inCRC.setCRC(new CRC32());
			inData = new DataInputStream(inCRC);
			inCRC = new CRCInputStream(in, new CRC8());
			int numstrings = (int) VByte.decode(inCRC);
			if (!inCRC.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading Prefix Header.");
			}

			if (numstrings > 0) {

				for (int i = 0; i < numstrings; i++) {
					String item = inData.readUTF();
					int pos = item.indexOf('=');
					if (pos != -1) {
						String property = item.substring(0, pos);
						String value = item.substring(pos + 1);
						if (property.equals(CSVocabulary.STORE_SUBJ_DICTIONARY))
							store_subj_dictionary = Boolean.parseBoolean(value);
						else if (property.equals(CSVocabulary.DISABLE_CONSISTENT_PREDICATES))
							disable_consistent_predicates = Boolean.parseBoolean(value);
						else if (property.equals(CSVocabulary.STORE_OBJ_DICTIONARY))
							store_obj_dictionary = Boolean.parseBoolean(value);
					}
				}

			}
			keyword = IOUtil.readChars(in, 4);
		}

		if (store_obj_dictionary == false) {
			CompressedStreaming2RDF exporter = new CompressedStreamingExporterNoDictionary(keyword, tempDiscrete, prefixes,
					store_subj_dictionary, disable_consistent_predicates);
			exporter.loadFromCompressedStreaming(in, out);
			return exporter;
		} else {
			CompressedStreaming2RDF exporter = new CompressedStreamingExporter(keyword, tempDiscrete, prefixes, store_subj_dictionary,
					disable_consistent_predicates);
			exporter.loadFromCompressedStreaming(in, out);
			return exporter;
		}
	}

	/**
	 * Auxiliary method to load a config predicate (e.g. discrete predicates or uniq predicates)
	 * 
	 * @param struct
	 * @throws IOException
	 */
	private void loadConfigPredicate(Map<String, Boolean> struct) throws IOException {

		CRCInputStream inCRC = new CRCInputStream(in, new CRC8());
		int numstrings = (int) VByte.decode(inCRC);
		if (!inCRC.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Prefix Header.");
		}

		if (numstrings > 0) {

			// READ Prefixes
			inCRC.setCRC(new CRC32());
			DataInputStream inData = new DataInputStream(inCRC);
			String pred;

			for (int i = 0; i < numstrings; i++) {

				pred = inData.readUTF();
				struct.put(pred, true);
			}

			if (!inCRC.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading new prefixes.");
			}

		}
	}

}
