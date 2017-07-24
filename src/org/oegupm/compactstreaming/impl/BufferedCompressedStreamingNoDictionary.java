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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.lang3.StringUtils;
import org.oegupm.compactstreaming.RDF2CompressedStreaming;
import org.oegupm.compactstreaming.dictionary.LRUDictionary;
import org.oegupm.compactstreaming.dictionary.PrefixDictionary;
import org.oegupm.compactstreaming.exceptions.BlockException;
import org.oegupm.compactstreaming.exceptions.DictionaryException;
import org.oegupm.compactstreaming.options.CSSpecification;
import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.crc.CRC16;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.oegupm.compactstreaming.CSVocabulary;

/**
 * Implementation of an importer from RDF to Compressed Streaming based on a Buffer to build blocks of a fixed size
 * 
 * @author javi
 * 
 */

public class BufferedCompressedStreamingNoDictionary implements RDF2CompressedStreaming {

	private long originalFileSize;
	private long ntTriplesSize;

	private LRUDictionary<String, Integer> subjects;

	private LRUDictionary<String, Integer> structures;

	private Map<String, Integer> predicates;

	ArrayList<String> literalTagsByPredicate; // store the literal tags of the objects related with predicates.
	ArrayList<Integer> sizeofTags; // saves size of tags to speed up processing
	BitSet predicateLiterals; // mark which predicate stores literals

	private long numTriples = 0;
	private long numBlocks = 0;

	BlockNoDictionary currentBlock;

	private Map<String, ArrayList<TripleString>> subjectsToProcess;

	ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(5);

	private OutputStream out;
	private PrefixDictionary prefixes;
	private Map<String, Boolean> predicateDiscrete;

	// TODO Complete to add predicates without dictionary
	// private Map<String, Boolean> predicateUniq = new HashMap<String, Boolean>();
	// private HashMap<String, Integer> CountRepetitions = new HashMap<String, Integer>();

	int max_size_dictionary;
	int block_size;
	boolean store_subj_dictionary;
	boolean store_obj_dictionary;
	boolean disable_consistent_predicates;
	int offset_tag;

	boolean usePrefixes = false;

	public BufferedCompressedStreamingNoDictionary(OutputStream out, CSSpecification spec, PrefixDictionary prefixes,
			Map<String, Boolean> predicateDiscrete) {

		/* TEST */
		// predicateNoDictObjects.put("http://www.w3.org/2000/01/rdf-schema#label",true);

		// Import prefixes
		this.prefixes = prefixes;

		if (this.prefixes.size() > 0)
			usePrefixes = true;

		// Import predicateDiscrete
		this.predicateDiscrete = predicateDiscrete;

		// Import predicateUniq
		// this.predicateUniq = predicateUniq;

		// load max size dictionary
		String specMaxSizeDictionary = spec.get(CSVocabulary.SPEC_MAX_SIZE_DICTIONARY);
		max_size_dictionary = CSVocabulary.DEFAULT_MAX_SIZE_DICTIONARY;
		if (specMaxSizeDictionary != null && !"".equals(specMaxSizeDictionary)) {
			try {
				max_size_dictionary = Integer.parseInt(specMaxSizeDictionary);
			} catch (NumberFormatException e) {
				max_size_dictionary = CSVocabulary.DEFAULT_MAX_SIZE_DICTIONARY;
			}
		}
		// load block size if present
		String specBlockSize = spec.get(CSVocabulary.SPEC_BLOCK_SIZE);
		block_size = CSVocabulary.DEFAULT_BLOCK_SIZE;
		if (specBlockSize != null && !"".equals(specBlockSize)) {
			try {
				block_size = Integer.parseInt(specBlockSize);
			} catch (NumberFormatException e) {
				block_size = CSVocabulary.DEFAULT_BLOCK_SIZE;
			}
		}

		// load if the dictionary of subjects must be stored
		String specStoreDictionary = spec.get(CSVocabulary.STORE_SUBJ_DICTIONARY);

		store_subj_dictionary = CSVocabulary.DEFAULT_STORE_SUBJ_DICTIONARY;
		if (specStoreDictionary != null && !"".equals(specStoreDictionary)) {

			store_subj_dictionary = Boolean.parseBoolean(specStoreDictionary);

		}

		// load if the dictionary of objects must be stored
		specStoreDictionary = spec.get(CSVocabulary.STORE_OBJ_DICTIONARY);

		store_obj_dictionary = false;

		// load if the predicates are not related with the same types and/or tags
		String specDisableLiteralTags = spec.get(CSVocabulary.DISABLE_CONSISTENT_PREDICATES);

		disable_consistent_predicates = CSVocabulary.DEFAULT_CONSISTENT_PREDICATES;
		if (specDisableLiteralTags != null && !"".equals(specDisableLiteralTags)) {

			disable_consistent_predicates = Boolean.parseBoolean(specDisableLiteralTags);

		}

		offset_tag = 1; // auxiliary offset by default for the last quote "
		if (disable_consistent_predicates)
			offset_tag = 0;

		if (store_subj_dictionary)
			subjects = new LRUDictionary<String, Integer>(max_size_dictionary);

		predicates = new HashMap<String, Integer>();
		// Consider ConcurrentHashMap<String, Integer>(); for parallel creation
		literalTagsByPredicate = new ArrayList<String>();
		sizeofTags = new ArrayList<Integer>();
		predicateLiterals = new BitSet();

		structures = new LRUDictionary<String, Integer>(max_size_dictionary);

		currentBlock = new BlockNoDictionary(store_subj_dictionary);

		subjectsToProcess = new ConcurrentHashMap<String, ArrayList<TripleString>>();

		this.out = out;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.oegupm.compactstreaming.impl.RDF2CompressedStreaming#insert(org.oegupm .compactstreaming.rdf.TripleString)
	 */
	@Override
	public void insert(TripleString triple) throws DictionaryException, BlockException, UTFDataFormatException {

		if ((numTriples % this.block_size) == 0) {
			if (numTriples != 0)
				try {
					Iterator<String> subjs = subjectsToProcess.keySet().iterator();
					while (subjs.hasNext()) {
						String currSubj = subjs.next();
						processMolecule(subjectsToProcess.get(currSubj));

					}
					currentBlock.save(out);

					numBlocks++;
					subjectsToProcess.clear();
				} catch (IOException e) {
					System.err.println("IO exception saving Block");
					e.printStackTrace();
				}

			currentBlock = new BlockNoDictionary(store_subj_dictionary);
		}

		if (triple.getObject().length() < 64 * 1024) { // max:64KB
			if (subjectsToProcess.containsKey(triple.getSubject().toString())) {
				TripleString newTriple = new TripleString(triple);

				subjectsToProcess.get(triple.getSubject().toString()).add(newTriple); // New Subject added to pool

			} else {
				ArrayList<TripleString> newTriples = new ArrayList<TripleString>();
				TripleString newTriple = new TripleString(triple);
				newTriples.add(newTriple);
				subjectsToProcess.put(newTriple.getSubject().toString(), newTriples); // New Subject added to pool

			}

			numTriples++;
		} else {
			throw new UTFDataFormatException("encoded string too long: " + triple.getObject().length() + " bytes");
		}

	}

	/**
	 * Process a molecule, i.e., set of triples around a common element (typically a subject)
	 * 
	 * @param elements
	 */
	public void processMolecule(ArrayList<TripleString> elements) {
		int tempIDSubject = 0;
		int tempIDPredicate = 0;
		int tempIDObject = 0;
		int tempIDStructure = 0;

		String subject = (String) elements.get(0).getSubject(); // could also be passed by argument
		MoleculeNoDictionary mol = new MoleculeNoDictionary();

		if (!store_subj_dictionary) {
			// check prefix and replace it if present
			Entry<String, Integer> entry = prefixes.getEntryShared(subject);
			if (entry != null) { // put only suffix
				mol.newSubject_prefix = entry.getValue();
				mol.newSubject = subject.substring(entry.getKey().length());
			} else {
				mol.newSubject_prefix = 0;
				mol.newSubject = subject;
			}
		} else { // store subject dictionary
			// Convert Subject-String to Subject-ID
			if (subjects.containsKey(subject)) {
				tempIDSubject = subjects.get(subject);
			} else {
				try {
					tempIDSubject = subjects.insert(subject);
				} catch (DictionaryException e) {
					System.err.println("LRU Dictionary exception");
					e.printStackTrace();
				}
				if (usePrefixes) {
					// check prefix and replace it if present
					Entry<String, Integer> entry = prefixes.getEntryShared(subject);

					if (entry != null) { // put only suffix
						mol.newSubject_prefix = entry.getValue();
						mol.newSubject = subject.substring(entry.getKey().length());
					} else {
						mol.newSubject_prefix = 0;
						mol.newSubject = subject;

					}
				} else {
					mol.newSubject_prefix = -1;
				}
			}
			// set molecule subject
			mol.subject = tempIDSubject;
		}

		String predicateStructure = "";
		String objectsDiscrete = "";
		Integer currPredicate = -1;

		Boolean isDiscretePredicate = false;
		String obj = "";
		for (int i = 0; i < elements.size(); i++) {

			obj = elements.get(i).getObject().toString();
			isDiscretePredicate = predicateDiscrete.get(elements.get(i).getPredicate().toString());
			if (predicates.containsKey(elements.get(i).getPredicate().toString())) {
				tempIDPredicate = predicates.get(elements.get(i).getPredicate().toString());
				// check dictionary of objects for this predicate

				// check discrete predicate
				if (isDiscretePredicate == null) {

					try {

						// erase object suffix
						obj = elements.get(i).getObject().toString();

						if (predicateLiterals.get(tempIDPredicate - 1)) {
							// offset_tag=1 by default or it is =0 if disabled_consistent_predicates=true
							obj = obj.substring(offset_tag, obj.length() - sizeofTags.get(tempIDPredicate - 1) - offset_tag);

							mol.addnewObjectLiteral(tempIDPredicate, tempIDObject, obj);
						} else {
							if (usePrefixes) {
								// replace predicate of URI
								Integer newPreffixURI = 0;
								// check prefix
								Entry<String, Integer> entry = prefixes.getEntryShared(obj);

								if (entry != null) { // put only suffix
									newPreffixURI = entry.getValue();
									obj = obj.substring(entry.getKey().length());
								}
								mol.addnewObjectURIBNode(tempIDPredicate, tempIDObject, obj, newPreffixURI);
							} else {
								mol.addnewObjectURIBNode(tempIDPredicate, tempIDObject, obj);
							}
						}

					} catch (BlockException e) {
						System.err.println("Block exception");
						e.printStackTrace();
					}

				}

			} else {
				// insert new Predicate and create LRUDictionary for it
				tempIDPredicate = predicates.size() + 1;

				predicates.put(elements.get(i).getPredicate().toString(), predicates.size() + 1);

				// store the literal tag if present

				String tag = "";
				Boolean isliteral = false;
				Boolean detectedType = false;

				// check discrete predicate
				if (isDiscretePredicate == null) {

					if (!disable_consistent_predicates) {

						if (obj.charAt(0) == '"') {
							isliteral = true;

							int pos_end = obj.lastIndexOf('"'); // tag after last '"'
							if (obj.length() > (pos_end + 1)) {
								tag = obj.substring(pos_end + 1);
								obj = obj.substring(1, pos_end); // replace obj erasing tag

							} else {
								obj = obj.substring(1, pos_end); // replace obj erasing '"'
								// trying to infer embedded tag before last '"'
								int pos_tag = obj.indexOf("^^");
								if (pos_tag > 0) {
									tag = obj.substring(pos_tag) + '"'; // we maintain the last quote to distinguish the inference
									obj = obj.substring(0, pos_tag); // replace obj erasing quotes '"'

									detectedType = true;

								}

							}
						}

					} else {
						isliteral = true; // consider all literals to avoid change much code
					}

					predicateLiterals.set(tempIDPredicate - 1, isliteral);
					literalTagsByPredicate.add(tag); // set tag TODO it could be not needed, the sizeofTags is used instead.
					if (detectedType == false) {
						sizeofTags.add(tag.length());

					} else {
						sizeofTags.add(tag.length() - 1); // speed up processing for replacing, -1 to erase last quote character
					}

					try {

						mol.addNewPredicate(tempIDPredicate, elements.get(i).getPredicate().toString(), isliteral, tag);

						if (isliteral) {
							mol.addnewObjectLiteral(tempIDPredicate, tempIDObject, obj);
						} else {
							// check prefix
							Entry<String, Integer> entry = prefixes.getEntryShared(obj);
							Integer newPreffixURI = 0;
							if (entry != null) { // put only suffix
								newPreffixURI = entry.getValue();
								obj = obj.substring(entry.getKey().length());
							}
							mol.addnewObjectURIBNode(tempIDPredicate, tempIDObject, obj, newPreffixURI);
						}

					} catch (BlockException e) {
						System.err.println("Block exception");
						e.printStackTrace();
					}
				} else {
					// insert void properties: a void dictionary of objects and no tags

					sizeofTags.add(0);
					isliteral = (obj.charAt(0) == '"');
					mol.addNewPredicate(tempIDPredicate, elements.get(i).getPredicate().toString(), isliteral, "");
				}

			}
			if (currPredicate != tempIDPredicate) {
				if (currPredicate == -1) // first
					predicateStructure += tempIDPredicate;
				else
					predicateStructure += ";" + tempIDPredicate;
			} else {

				predicateStructure += ",";

			}
			// check discrete predicate and append objects
			if (isDiscretePredicate != null) {
				objectsDiscrete = objectsDiscrete + "$" + obj;
			}
			currPredicate = tempIDPredicate;

		}
		predicateStructure = predicateStructure + objectsDiscrete;
		// Insert structure
		if (structures.containsKey(predicateStructure)) {
			tempIDStructure = structures.get(predicateStructure);

			mol.addStructure(tempIDStructure); // add structure of the molecule
		} else {
			try {
				tempIDStructure = structures.insert(predicateStructure);

			} catch (DictionaryException e) {
				System.err.println("LRU Dictionary exception");
				e.printStackTrace();
			}

			// store new structure in compact way (list of predicate-ID and its
			// repetitions and objects discrete
			ArrayList<Integer> listPredsinStructure = new ArrayList<Integer>(5);
			ArrayList<String> allObjectsDiscrete = null;
			if (objectsDiscrete != "") {
				allObjectsDiscrete = new ArrayList<String>(Arrays.asList(objectsDiscrete.split("\\$")));
				allObjectsDiscrete.remove(0); // erase first void element

				predicateStructure = predicateStructure.substring(0, predicateStructure.length() - objectsDiscrete.length());
			}
			String[] partsPredicates = predicateStructure.split(";");
			String currPred = "";
			Integer currPredID = 0;
			for (int j = 0; j < partsPredicates.length; j++) {
				// count if predicate is repeated several times
				int numRepeatedPredicates = StringUtils.countMatches(partsPredicates[j], ",");
				currPred = partsPredicates[j];

				if (numRepeatedPredicates > 0) {
					currPred = partsPredicates[j].substring(0, partsPredicates[j].indexOf(','));
				}
				currPredID = Integer.decode(currPred);
				// store predicate ID and number of repetitions
				listPredsinStructure.add(currPredID);
				listPredsinStructure.add(numRepeatedPredicates + 1); // +1 to count the initial triple (without ',').
			}

			mol.addNewStructure(tempIDStructure, listPredsinStructure, allObjectsDiscrete); // add new string structure
		}

		currentBlock.processMolecule(mol);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.oegupm.compactstreaming.RDF2CompressedStreaming#startProcessing()
	 */
	@Override
	public void startProcessing() {
		// initial tasks

		// WRITE IF SOME CONF!=DEFAULT

		try {
			CRCOutputStream outCRC = new CRCOutputStream(this.out, new CRC16());
			IOUtil.writeString(outCRC, "$CNF");

			outCRC.setCRC(new CRC8());
			VByte.encode(outCRC, 3); // write length of properties to write(to date just 3)
			outCRC.writeCRC();

			outCRC.setCRC(new CRC32());
			DataOutputStream outData = new DataOutputStream(outCRC);

			outData.writeUTF(CSVocabulary.STORE_SUBJ_DICTIONARY + "=" + store_subj_dictionary);

			outData.writeUTF(CSVocabulary.STORE_OBJ_DICTIONARY + "=" + store_obj_dictionary);

			outData.writeUTF(CSVocabulary.DISABLE_CONSISTENT_PREDICATES + "=" + disable_consistent_predicates);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.oegupm.compactstreaming.impl.RDF2CompressedStreaming#endProcessing()
	 */
	@Override
	public void endProcessing() {
		if (subjectsToProcess.size() > 0) {
			try {
				Iterator<String> subjs = subjectsToProcess.keySet().iterator();
				while (subjs.hasNext()) {

					processMolecule(subjectsToProcess.get(subjs.next()));

				}
				currentBlock.save(out);
				numBlocks++;
				subjectsToProcess.clear();
			} catch (IOException e) {
				System.err.println("IO exception saving Block");
				e.printStackTrace();
			}

		}
		/* MARK END OF STREAM */
		CRCOutputStream outCRC = new CRCOutputStream(out, new CRC16()); // Cookie

		try {
			IOUtil.writeString(outCRC, "$END");
		} catch (IOException e) {
			e.printStackTrace();
		}

		// test print repetitions
		/*
		 * System.out.println("Repeated objects:" + CountRepetitions.size()); Iterator<String> it = CountRepetitions.keySet().iterator();
		 * while (it.hasNext()) { String pred = it.next(); System.out.println("Pred: " + pred + "; repeated objects: " +
		 * CountRepetitions.get(pred)); }
		 */
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.oegupm.compactstreaming.impl.RDF2CompressedStreaming#getOriginalFileSize ()
	 */
	@Override
	public long getOriginalFileSize() {
		return originalFileSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.oegupm.compactstreaming.impl.RDF2CompressedStreaming#setOriginalFileSize (long)
	 */
	@Override
	public void setOriginalFileSize(long originalFileSize) {
		this.originalFileSize = originalFileSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.oegupm.compactstreaming.impl.RDF2CompressedStreaming#getNtTriplesSize ()
	 */
	@Override
	public long getNtTriplesSize() {
		return ntTriplesSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.oegupm.compactstreaming.impl.RDF2CompressedStreaming#setNtTriplesSize (long)
	 */
	@Override
	public void setNtTriplesSize(long ntTriplesSize) {
		this.ntTriplesSize = ntTriplesSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.oegupm.compactstreaming.impl.RDF2CompressedStreaming#getNumTriples()
	 */
	@Override
	public long getNumTriples() {
		return numTriples;
	}

	public long getNumBlocks() {
		return numBlocks;
	}

	@Override
	public String printConfiguration() {
		return CSVocabulary.SPEC_BLOCK_SIZE + ":" + this.block_size + ", " + CSVocabulary.SPEC_MAX_SIZE_DICTIONARY + ":"
				+ this.max_size_dictionary + ", " + CSVocabulary.STORE_SUBJ_DICTIONARY + ":" + this.store_subj_dictionary + ", "
				+ CSVocabulary.STORE_OBJ_DICTIONARY + ":" + this.store_obj_dictionary + ", " + CSVocabulary.DISABLE_CONSISTENT_PREDICATES
				+ ":" + this.disable_consistent_predicates;
	}

}
