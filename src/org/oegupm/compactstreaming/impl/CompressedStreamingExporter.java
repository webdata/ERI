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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.oegupm.compactstreaming.CSVocabulary;
import org.oegupm.compactstreaming.CompressedStreaming2RDF;
import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.compact.sequence.Sequence;
import org.rdfhdt.hdt.compact.sequence.SequenceLog64;
import org.rdfhdt.hdt.exceptions.CRCException;
import org.rdfhdt.hdt.triples.TripleString;
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
public class CompressedStreamingExporter implements CompressedStreaming2RDF {

	/**
	 * Auxiliary class to keep the structure of a molecule
	 * 
	 * @author javi
	 */
	class MoleculeStructure {
		public ArrayList<Integer> listPreds; // predicates and repetitions of each predicate
		public ArrayList<String> objectsDiscrete; // objects of the discrete predicates
	};

	public OutputStream out;
	InputStream in;

	private Map<Integer, String> subjects;
	// one dictionary per predicate
	private ArrayList<HashMap<Integer, String>> objects;
	// uncomment to create one global dictionary of objects
	// private LRUDictionary<String,Integer> objects;

	private Map<Integer, MoleculeStructure> structures;

	private ArrayList<String> predicates;
	private ArrayList<Boolean> predicateLiterals; // mark which predicate stores literals
	private ArrayList<Boolean> predicateDiscrete; // mark which predicate is discrete
	// private ArrayList<Boolean> predicateUniq; // mark which predicate is uniq

	private ArrayList<String> predicatesTags;

	private ArrayList<String> newTermsURIBNode;
	private ArrayList<String> newTermsLiteral;
	int offsetNewTermURIBNode = 0; // allows incremental read of NewTerms URIs or Bnodes;
	int offsetNewTermLiteral = 0; // allows incremental read of NewTerms Literals

	Map<Integer, BitSet> predicatesNewTermMarker;

	private Map<Integer, Integer> offsetforPredicate; // count the offset when reading

	private ArrayList<String> prefixes;

	// PREDICATE CHANNELS
	Map<Integer, Sequence> objectsofPredicate;

	private long numTriples = 0;
	private long numBlocks = 0;

	private Boolean store_subj_dictionary = CSVocabulary.DEFAULT_STORE_SUBJ_DICTIONARY;
	private Boolean store_obj_dictionary = true;
	// private Boolean store_obj_dictionary = CSVocabulary.DEFAULT_STORE_OBJ_DICTIONARY;
	private Boolean disable_consistent_predicates = CSVocabulary.DEFAULT_CONSISTENT_PREDICATES;
	private SequenceLog64 sqStructs; // Use Sequence and SequenceFactory to be more generic, use log64 directly to speed up performance.
	private SequenceLog64 sqPreds;
	private SequenceLog64 sqSubjects;
	private SequenceLog64 sqPrefixes;
	private SequenceLog64 newTermURIBNodePrefixes;
	private String keyword;
	private Map<String, Boolean> tempDiscrete;

	public CompressedStreamingExporter(String keyword, Map<String, Boolean> tempDiscrete, ArrayList<String> prefixes,
			Boolean store_subj_dictionary, Boolean disable_consistent_predicates) {
		this.keyword = keyword;
		this.tempDiscrete = tempDiscrete;
		this.prefixes = prefixes;
		this.store_subj_dictionary = store_subj_dictionary;
		this.disable_consistent_predicates = disable_consistent_predicates;
		subjects = new HashMap<Integer, String>();
		objects = new ArrayList<HashMap<Integer, String>>();
		structures = new HashMap<Integer, MoleculeStructure>();
		predicates = new ArrayList<String>(100); // initialize to 100 predicates (it resizes on demand)
		predicates.add(""); // initialize ID=0 to ""
		newTermsURIBNode = new ArrayList<String>();
		newTermsLiteral = new ArrayList<String>();
		predicatesNewTermMarker = new HashMap<Integer, BitSet>();
		objectsofPredicate = new HashMap<Integer, Sequence>();
		offsetforPredicate = new HashMap<Integer, Integer>();

		predicateLiterals = new ArrayList<Boolean>(); // mark which predicate stores literals
		predicateLiterals.add(false); // add ID=0 to false
		predicateDiscrete = new ArrayList<Boolean>(); // mark which predicate is discrete
		predicateDiscrete.add(false); // add ID=0 to false
		// predicateUniq = new ArrayList<Boolean>(); // mark which predicate is uniq
		// predicateUniq.add(false); // add ID=0 to false
		predicatesTags = new ArrayList<String>();

	}

	/**
	 * Load a file in Compressed Streaming format and export the information to Ntriples
	 * 
	 * @param filename
	 *            The file in Compressed Streaming format
	 * @param out
	 *            the output stream to export the information to Ntriples
	 * @throws IOException
	 */
	public void loadFromCompressedStreaming(InputStream input, OutputStream out) throws IOException {
		this.out = out;
		this.in = input;

		/* INITIALIZE AUXILIAR STRUCTURES */
		int numstrings = 0, numNewPreds = 0, numSubjects = 0, predID = 0, numPreds = 0, currPredID = 0, currObjectID = 0, numPredsinList = 0;
		String pred = "", subj = "", newTerm = "", currSubject = "", currPred = "", currObject = "", currTag = "";
		long bytes = 0;
		BitSet literalMarkerBitset = new BitSet();

		sqStructs = new SequenceLog64();
		ArrayList<String> currObjectsDiscrete = new ArrayList<String>();
		byte[] structMarker, subjMarker;
		sqPreds = new SequenceLog64();
		BitSet structMarkerBitset, subjMarkerBitset;
		sqSubjects = new SequenceLog64();
		sqPrefixes = new SequenceLog64();
		ArrayList<String> currentSubj;
		Inflater inflater;
		InflaterInputStream inf;
		newTermURIBNodePrefixes = new SequenceLog64();
		MoleculeStructure currStruct;

		Integer currOffsetPred = 0;
		Boolean isNewStruct = false, isLiteral = false;

		TripleString outString = new TripleString();
		DataInputStream inData = new DataInputStream(null);
		/* FOR ALL BLOCKS... */
		while (keyword.equals("$BLK")) {
			numBlocks++;

			/* START> INITIALIZE DATA STRUCTURES */
			newTermsURIBNode = new ArrayList<String>();
			newTermsLiteral = new ArrayList<String>();
			predicatesNewTermMarker = new HashMap<Integer, BitSet>();
			objectsofPredicate = new HashMap<Integer, Sequence>();
			offsetforPredicate = new HashMap<Integer, Integer>();
			offsetNewTermURIBNode = 0;
			offsetNewTermLiteral = 0;
			/* END> INITIALIZE DATA STRUCTURES */

			/**** READ STRUCTURAL CHANELS *****/

			/* START> READ NEW PREDICATES */
			CRCInputStream inCRC = new CRCInputStream(in, new CRC8());
			numNewPreds = (int) VByte.decode(inCRC);

			if (!inCRC.readCRCAndCheck()) {
				inData.close();
				throw new CRCException("CRC Error while reading NewPredicate Header.");
			}

			if (numNewPreds > 0) { // READ NEW PREDICATES

				inCRC.setCRC(new CRC32());
				inData = new DataInputStream(inCRC);

				for (int i = 0; i < numNewPreds; i++) {

					pred = inData.readUTF(); // read predicate
					predicates.add(pred); // add predicate to list of predicates
					HashMap<Integer, String> newObjects = new HashMap<Integer, String>();
					objects.add(newObjects); // add dictionary of objects

					// check and save if the predicate is discrete
					if (tempDiscrete.get(pred) != null) {
						predicateDiscrete.add(true);
					} else {
						predicateDiscrete.add(false);
					}
					// check and save if the predicate is uniq
					/*
					 * if (tempUniq.get(pred) != null) { predicateUniq.add(true); } else { predicateUniq.add(false); }
					 */
				}

				if (!inCRC.readCRCAndCheck()) {
					inData.close();
					throw new CRCException("CRC Error while reading NewPredicates.");
				}

				// read the marker stating which predicates are related with literals
				inCRC = new CRCInputStream(in, new CRC8());
				numstrings = (int) VByte.decode(inCRC);
				bytes = VByte.decode(inCRC);
				if (!inCRC.readCRCAndCheck()) {
					inData.close();
					throw new CRCException("CRC Error while reading Predicate Literals header.");
				}
				// literalMarkerBitset = new BitSet();

				if (numstrings > 0) { // some predicates are related with literals
					inCRC.setCRC(new CRC32());
					byte[] literalMarker = IOUtil.readBuffer(inCRC, (int) bytes, null); // read marker
					if (!inCRC.readCRCAndCheck()) {
						inData.close();
						throw new CRCException("CRC Error while reading New predicates.");
					}
					literalMarkerBitset = BitSet.valueOf(literalMarker);

					for (int i = 0; i < numNewPreds; i++) {
						// convert the Bitset marker to the type ArrayList<Boolean> stored in predicateLiterals
						if (i < literalMarkerBitset.length()) {
							predicateLiterals.add(literalMarkerBitset.get(i));
						} else {
							predicateLiterals.add(false);
						}

					}
					// read the new tags in the literal predicates, if present
					inCRC = new CRCInputStream(in, new CRC8());
					numstrings = (int) VByte.decode(inCRC);

					if (!inCRC.readCRCAndCheck()) {
						inData.close();
						throw new CRCException("CRC Error while reading New Structures Header.");
					}

					inCRC.setCRC(new CRC32());
					inData = new DataInputStream(inCRC);
					for (int i = 0; i < numstrings; i++) { // read and store new tags
						predicatesTags.add(inData.readUTF());
					}
					if (!inCRC.readCRCAndCheck()) {
						inData.close();
						throw new CRCException("CRC Error while reading New Structures.");
					}
				} else { // the new predicates are not related with literals
					for (int i = 0; i < numNewPreds; i++) { // mark this predicate as false and initialize tags to void
						predicateLiterals.add(false);
						predicatesTags.add("");
					}
				}

			}
			/* END> READ NEW PREDICATES */

			/* START> READ STRUCTURES */

			// read IDs of structures
			// Use SequenceFactory to be more generic, use log64 directly to speed up performance.
			sqStructs = new SequenceLog64();
			sqStructs.load(in, null); // Read blocks from input, they have their own CRC check.

			// read marker of new structures
			inCRC = new CRCInputStream(in, new CRC8());
			numstrings = (int) VByte.decode(inCRC); // num of structures
			// System.out.println("NumStructures:" + numstrings);
			bytes = VByte.decode(inCRC);
			if (!inCRC.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading New Structures Header.");
			}
			structMarkerBitset = new BitSet();

			if (numstrings > 0) { // there are new structures
				inCRC.setCRC(new CRC32());
				structMarker = IOUtil.readBuffer(inCRC, (int) bytes, null);
				if (!inCRC.readCRCAndCheck()) {
					throw new CRCException("CRC Error while reading New Structures.");
				}

				structMarkerBitset = BitSet.valueOf(structMarker); // load bitset marking with true the new structures

				// Read new structures
				// 1.- Read lists of predicates and repetitions
				sqPreds.load(in, null); // Read blocks from input, they have their own CRC check.

				// 2.- Read discrete objects if present
				inCRC = new CRCInputStream(in, new CRC8());
				numstrings = (int) VByte.decode(inCRC);

				if (!inCRC.readCRCAndCheck()) {
					throw new CRCException("CRC Error while reading New Structures Header.");
				}
				if (numstrings > 0) { // there are discrete objects

					// read new discrete objects
					// Prepare decompression
					in.mark(512000); // mark the initial position before decompression (max: 500 MB)

					inflater = new Inflater();
					inf = new InflaterInputStream(in, inflater);
					inData = new DataInputStream(inf);
					currObjectsDiscrete = new ArrayList<String>();
					for (int i = 0; i < numstrings; i++) {
						currObjectsDiscrete.add(inData.readUTF());
					}
					if (inflater.getRemaining() != 0) {
						// there are more bytes to read (metadata)
						in.reset(); // reset input to the marked initial position
						in.skip(inflater.getBytesRead()); // skip the number of bytes read

					} else {
						// no remaining data
						inData.read(); // read end of stream
						in.reset(); // reset input to the marked initial position
						in.skip(inflater.getBytesRead()); // skip the number of bytes read

					}

					inflater.reset();
					inflater.end();
				}
			}
			/* END> READ STRUCTURES */

			/* START> READ SUBJECTS (if they are stored) */

			subjMarkerBitset = new BitSet(); // initialize marker of new subjects

			if (store_subj_dictionary) { // subject IDs are stored

				sqSubjects.load(in, null); // Read subjects IDs from input, they have their own CRC check.

				// read new subject marker
				inCRC = new CRCInputStream(in, new CRC8());
				numstrings = (int) VByte.decode(inCRC); // num subjects
				bytes = VByte.decode(inCRC);

				if (!inCRC.readCRCAndCheck()) {
					throw new CRCException("CRC Error while reading Subjects Markers.");
				}

				if (numstrings > 0) { // test if there are new subject marker to read
					inCRC.setCRC(new CRC32());
					subjMarker = IOUtil.readBuffer(inCRC, (int) bytes, null);
					if (!inCRC.readCRCAndCheck()) {
						throw new CRCException("CRC Error while reading New Structures.");
					}

					subjMarkerBitset = BitSet.valueOf(subjMarker);
				}
			}

			// read new subject header
			inCRC = new CRCInputStream(in, new CRC8());
			numSubjects = (int) VByte.decode(inCRC);

			if (!inCRC.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading New Subject Header.");
			}

			if (numSubjects > 0) { // new subjects
				inCRC.setCRC(new CRC32());
				sqPrefixes.load(in, null); // Read prefixes from input, they have their own CRC check.
				inData = new DataInputStream(inCRC); // Prepare inData to read new subjects from input
			}

			// save the current subjects and update the dictionary
			currentSubj = new ArrayList<String>((int) sqStructs.getNumberOfElements());
			int indexNewTerms = 0;

			// Prepare decompression
			in.mark(512000); // mark the initial position before decompression (max: 500 MB)

			inflater = new Inflater();
			inf = new InflaterInputStream(in, inflater);
			inData = new DataInputStream(inf);

			if (!store_subj_dictionary) {
				for (int i = 0; i < numSubjects; i++) {
					subj = inData.readUTF(); // read new subject

					if ((int) sqPrefixes.get(indexNewTerms) != 0) { // add prefix if present
						subj = prefixes.get((int) sqPrefixes.get(indexNewTerms)) + subj;

					}
					indexNewTerms++;
					currentSubj.add(subj); // store subject as current subject
				}
			} else { // store subjects in the dictionary of subjects
				for (int i = 0; i < sqSubjects.getNumberOfElements(); i++) {

					if (subjMarkerBitset.length() > i && subjMarkerBitset.get(i)) {
						// new subject-> read new subject from input stream

						subj = inData.readUTF(); // read new subject
						if (prefixes.size() > 0)
							if ((int) sqPrefixes.get(indexNewTerms) != 0) { // add prefix if present
								subj = prefixes.get((int) sqPrefixes.get(indexNewTerms)) + subj;

							}
						indexNewTerms++;

						subjects.put((int) sqSubjects.get(i), subj); // update dictionary of subjects
						currentSubj.add(subj); // store subject as current subject

					} else {
						// old structure-> get subject from hash of IDs2string

						currentSubj.add(subjects.get((int) sqSubjects.get(i)));
					}
				}
			}

			if (numSubjects > 0) {
				if (inflater.getRemaining() != 0) {
					// there are more bytes to read (metadata)
					in.reset(); // reset input to the marked initial position
					in.skip(inflater.getBytesRead()); // skip the number of bytes read

				} else {
					// no remaining data
					inData.read(); // read end of stream
					in.reset(); // reset input to the marked initial position
					in.skip(inflater.getBytesRead()); // skip the number of bytes read

				}

				inflater.reset();
				inflater.end();

			}

			/* END> READ SUBJECTS (if they are stored) */

			/**** PREDICATE CHANELS *****/

			/* START> READ NEW TERMS URIs or BNodes */
			inCRC = new CRCInputStream(in, new CRC8());
			numstrings = (int) VByte.decode(inCRC); // number of new URIs or BNodes
			if (!inCRC.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading NewTERMS Header.");
			}

			if (numstrings > 0) { // there are new terms URIs or BNodes

				newTermURIBNodePrefixes.load(in, null); // Read terms from input, they have their own CRC check.
				inCRC.setCRC(new CRC32());

				// Prepare decompression
				in.mark(512000); // mark the initial position before decompression (max: 500 MB)
				inflater = new Inflater();
				inf = new InflaterInputStream(in, inflater);
				inData = new DataInputStream(inf);

				for (int i = 0; i < numstrings; i++) {
					newTerm = inData.readUTF(); // read new term
					newTermsURIBNode.add(newTerm); // add to list of new terms uris or bnodes
				}

				if (inflater.getRemaining() != 0) {
					// there are more bytes to read (metadata)
					in.reset(); // reset input to the marked initial position
					in.skip(inflater.getBytesRead()); // skip the number of bytes read
				} else {
					inData.read(); // read end of stream
					in.reset(); // reset input to the marked initial position
					in.skip(inflater.getBytesRead()); // skip the number of bytes read

				}

				inflater.reset();
				inflater.end();

			}
			/* END> READ NEW TERMS URIS */

			/* START> READ NEW TERMS LITERALS */

			inCRC = new CRCInputStream(in, new CRC8());
			numstrings = (int) VByte.decode(inCRC); // number of new literals

			if (!inCRC.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading NewTERMS Header.");
			}
			if (numstrings > 0) { // there are new literals
				inCRC.setCRC(new CRC32());

				// Prepare decompression
				in.mark(512000); // mark the initial position before decompression (max: 500 MB)
				inflater = new Inflater();
				inf = new InflaterInputStream(in, inflater);
				inData = new DataInputStream(inf);

				for (int i = 0; i < numstrings; i++) {
					newTerm = inData.readUTF(); // read new term
					newTermsLiteral.add(newTerm); // add to the list of new literales
				}

				if (inflater.getRemaining() != 0) {
					// there are more bytes to read (metadata)
					in.reset(); // reset input to the marked initial position
					in.skip(inflater.getBytesRead()); // skip the number of bytes read
				} else {
					inData.read(); // read end of stream
					in.reset(); // reset input to the marked initial position
					in.skip(inflater.getBytesRead()); // skip the number of bytes read
				}

				inflater.reset();
				inflater.end();
			}
			/* END> READ NEW TERMS LITERALS */

			/* START> READ PREDICATES */
			inCRC = new CRCInputStream(in, new CRC8());
			numPreds = (int) VByte.decode(inCRC); // number of different predicates in the block

			if (!inCRC.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading NewTERMS Header.");
			}

			inCRC.setCRC(new CRC32());
			if (store_obj_dictionary) {
				for (int i = 0; i < numPreds; i++) { // for each predicate in the block

					predID = (int) VByte.decode(inCRC); // Read predicate ID
					offsetforPredicate.put(predID, 0); // initialize offset for reading

					if (!inCRC.readCRCAndCheck()) {
						throw new CRCException("CRC Error while reading Predicate ID.");
					}
					// if (!predicateUniq.get(predID)) { // skip if the predicate is uniq
					// read predicate marker for new terms
					inCRC = new CRCInputStream(in, new CRC8());
					numstrings = (int) VByte.decode(inCRC);
					bytes = VByte.decode(inCRC);
					if (!inCRC.readCRCAndCheck()) {
						throw new CRCException("CRC Error while reading Predicates Markers.");
					}
					BitSet predMarkerBitset = new BitSet(); // initialize in any case
					if (numstrings > 0) { // test if there are markers of new terms in this predicate
						inCRC.setCRC(new CRC32());
						byte[] predMarker = IOUtil.readBuffer(inCRC, (int) bytes, null);
						if (!inCRC.readCRCAndCheck()) {
							throw new CRCException("CRC Error while reading New Structures.");
						}

						predMarkerBitset = BitSet.valueOf(predMarker);
					}
					predicatesNewTermMarker.put(predID, predMarkerBitset);

					// read objects
					Sequence objects = new SequenceLog64();

					objects.load(in, null); // Read objects from input, they have their own CRC check.
					objectsofPredicate.put(predID, objects);
				}
				// }
			}

			/* END> READ PREDICATES */

			/* START> PARSE ELEMENTS */

			int offsetDiscreteOldStructs = 0;
			int offsetDiscreteNewStructs = 0;
			int offsetListPred = 0;

			for (int i = 0; i < sqStructs.getNumberOfElements(); i++) { // for each structure (= for each molecule)

				if (structMarkerBitset.length() <= i || !structMarkerBitset.get(i)) {
					// old structure-> get from hash of IDs2string

					isNewStruct = false;
					currStruct = structures.get((int) sqStructs.get(i));
					offsetDiscreteOldStructs = 0;

				} else {
					// new structure-> read new structure from input stream

					isNewStruct = true;
					currStruct = new MoleculeStructure();
					currStruct.listPreds = new ArrayList<Integer>();
					numPredsinList = (int) sqPreds.get(offsetListPred++); // add number of different predicates stored in the first position

					for (int j = 0; j < numPredsinList; j++) {
						// read predicate ID
						currStruct.listPreds.add((int) sqPreds.get(offsetListPred++));
						// read repetitions of predicate
						currStruct.listPreds.add((int) sqPreds.get(offsetListPred++));
					}

					currStruct.objectsDiscrete = new ArrayList<String>();
				}

				currSubject = currentSubj.get(i);

				int numRepeatedPredicates = 0;

				for (int j = 0; j < currStruct.listPreds.size(); j++) { // iterate trough the number of different predicates

					currPredID = currStruct.listPreds.get(j++);
					currPred = predicates.get(currPredID); // get predicate from dictionary
					isLiteral = predicateLiterals.get(currPredID);
					Boolean thisStore_obj_dictionary = store_obj_dictionary;
					// if (predicateUniq.get(currPredID))
					// thisStore_obj_dictionary=false; //skip the objects of this predicate

					numRepeatedPredicates = currStruct.listPreds.get(j); // get number of repetitions of this predicate

					for (int k = 0; k < numRepeatedPredicates; k++) { // for all the repetitions of the current predicate

						outString.setSubject(currSubject); // set subject
						outString.setPredicate(currPred); // set predicate

						// DECODE OBJECT
						if (store_obj_dictionary)
							currOffsetPred = offsetforPredicate.get(currPredID);

						if (predicateDiscrete.get(currPredID) == false) {
							if (thisStore_obj_dictionary)
								currObjectID = (int) objectsofPredicate.get(currPredID).get(currOffsetPred);

							if (!thisStore_obj_dictionary
									|| ((predicatesNewTermMarker.get(currPredID).length() > currOffsetPred) && (predicatesNewTermMarker
											.get(currPredID).get(currOffsetPred)))) { // object is a new Term
								// retrieve new Term
								StringBuilder str = new StringBuilder();

								if (isLiteral) { // the object is a literal
									currObject = newTermsLiteral.get(offsetNewTermLiteral);
									// check and append the tags if present
									if (!disable_consistent_predicates) {
										currTag = predicatesTags.get(currPredID - 1);
										str.append('"').append(currObject);
										if (currTag.length() > 0 && currTag.charAt(currTag.length() - 1) == '"') {
											str.append(currTag);
										} else {
											str.append('"').append(currTag);
										}
									} else {
										str.append(currObject);
									}

									offsetNewTermLiteral++;

								} else { // object is not a literal

									currObject = newTermsURIBNode.get(offsetNewTermURIBNode);
									// check and append the prefix of URIs or BNodes if present

									if (prefixes.size() > 0) // if there are potential prefixes
										str.append(prefixes.get((int) newTermURIBNodePrefixes.get(offsetNewTermURIBNode)));
									str.append(currObject);

									offsetNewTermURIBNode++;
								}
								currObject = str.toString();
								if (thisStore_obj_dictionary)
									objects.get(currPredID - 1).put(currObjectID, currObject); // insert to dictionary of objects

							} else { // object is an old Term, retrieve from the dictionary of objects for this predicate
								currObject = objects.get(currPredID - 1).get(currObjectID);
							}

							offsetforPredicate.put(currPredID, currOffsetPred + 1); // update offset for this predicate

						} else { // predicate is discrete

							if (!isNewStruct) { // retrieve the object from an old structure
								currObject = currStruct.objectsDiscrete.get(offsetDiscreteOldStructs++);
							} else { // retrieve the object from a new structure
								currObject = currObjectsDiscrete.get(offsetDiscreteNewStructs++);
								currStruct.objectsDiscrete.add(currObject); // update the object in the structure
							}
						}
						outString.setObject(currObject); // set object
						out.write((outString.asNtriple()).toString().getBytes(Charset.forName("UTF-8"))); // print triple string
						numTriples++;

					}

				}
				if (isNewStruct) { // if it was a new structure, update the dictionary of structures
					structures.put((int) sqStructs.get(i), currStruct); // insert new Structure with the given ID

				}
			}
			/* END> PARSE ELEMENTS */

			keyword = IOUtil.readChars(in, 4); // read new keyword (pointing to new block or end of processing)
		}

	}

	/**
	 * @return Number of Triples exported
	 */
	public long getNumTriples() {
		return numTriples;
	}

	/**
	 * @return Number of blocks read
	 */
	public long getNumBlocks() {
		return numBlocks;
	}

}
