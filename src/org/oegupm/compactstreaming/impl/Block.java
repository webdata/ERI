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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.DeflaterOutputStream;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.compact.sequence.SequenceLog64;
import org.rdfhdt.hdt.util.crc.CRC16;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.io.IOUtil;

/**
 * Block of compressed triples
 * 
 * @author javi
 * 
 */
public class Block {

	// STRUCTURAL CHANNELS
	protected ArrayList<Integer> subjectIDs;
	protected ArrayList<String> newSubjects;
	protected ArrayList<Integer> newSubjects_prefixes;
	protected int numSubjects;
	protected BitSet newSubjectsMarker;

	protected ArrayList<Integer> structureIDs;
	protected ArrayList<Integer> newStructure_listPreds;
	protected ArrayList<String> newStructure_allObjectsDiscrete;

	protected BitSet newStructuresMarker;

	protected ArrayList<String> newPredicates;
	protected BitSet predicateLiterals; // mark which predicate stores literals
	protected ArrayList<String> newPredicatesTags;
	// FIX ADD THE STRUCTURE OF THE MOLECULE

	// PREDICATE CHANNELS
	protected Map<Integer, ArrayList<Integer>> predicates; // FIXME when considering objects out of dictionary, this should be
															// ArrayList<String> too
	protected Map<Integer, BitSet> predicatesNewTermMarker;
	protected ArrayList<String> newTermsURIBNode;
	protected ArrayList<Integer> newTermsURIBNode_prefixes;
	protected ArrayList<String> newTermsLiteral;

	protected int totalstructs = 0;
	protected int totalnewPredicates = 0;
	protected Map<Integer, Integer> totalObjsperPredicate; // count markers

	// FIXME: maxBlockSize is not used now, probably it helps optimizing the initialization of structures
	protected Boolean store_subj_dictionary = false;

	public Block(Boolean store_subject_IDs) {
		this();
		this.store_subj_dictionary = store_subject_IDs;
		subjectIDs = new ArrayList<Integer>();
	}

	public Block() {

		newSubjects = new ArrayList<String>();
		newSubjects_prefixes = new ArrayList<Integer>();
		newPredicates = new ArrayList<String>();
		structureIDs = new ArrayList<Integer>();

		newStructure_listPreds = new ArrayList<Integer>();
		newStructure_allObjectsDiscrete = new ArrayList<String>();

		predicates = new HashMap<Integer, ArrayList<Integer>>();
		newTermsURIBNode = new ArrayList<String>();
		newTermsURIBNode_prefixes = new ArrayList<Integer>();
		newTermsLiteral = new ArrayList<String>();

		newSubjectsMarker = new BitSet();
		predicatesNewTermMarker = new HashMap<Integer, BitSet>();
		newStructuresMarker = new BitSet();

		numSubjects = 0;
		totalObjsperPredicate = new HashMap<Integer, Integer>();
	}

	/**
	 * Process Molecule in Block
	 * 
	 * @param mol
	 *            Molecule
	 */
	public synchronized void processMolecule(Molecule mol) {

		/* START> ADD SUBJECT */

		if (!store_subj_dictionary) { // add always new subject
			newSubjects.add(mol.newSubject);
			if (mol.newSubject_prefix != -1)
				newSubjects_prefixes.add(mol.newSubject_prefix);
		} else { // subjecT based on IDs
			if (mol.newSubject != null) { // new subject
				newSubjects.add(mol.newSubject);
				if (mol.newSubject_prefix != -1)
					newSubjects_prefixes.add(mol.newSubject_prefix);
				newSubjectsMarker.set(numSubjects, true);
			} else { // old subject
				newSubjectsMarker.set(numSubjects, false); // set new subject to false

			}
			subjectIDs.add(mol.subject);
		}
		numSubjects++;
		/* END> ADD SUBJECT */

		/* START> ADD STRUCTURE ID AND NEW STRUCTURES */
		structureIDs.add(mol.structureID);

		if (mol.newStructure_listPreds != null) {
			// add number of predicates (useful for decompression)
			newStructure_listPreds.add(mol.newStructure_listPreds.size() / 2); // div 2 to discount the number of repetitions included in
																				// the list
			newStructure_listPreds.addAll(mol.newStructure_listPreds);
			if (mol.newStructure_allObjectsDiscrete != null)
				newStructure_allObjectsDiscrete.addAll(mol.newStructure_allObjectsDiscrete);
		}

		if (mol.isNewStructure)
			newStructuresMarker.set(totalstructs, mol.isNewStructure);

		totalstructs++;

		/* END> ADD STRUCTURE ID AND NEW STRUCTURES */

		/* START> ADD NEW PREDICATES */
		if (mol.newPredicates != null) {
			newPredicates.addAll(mol.newPredicates);
			if (mol.newPredicatesTags != null) {
				if (newPredicatesTags == null) {
					newPredicatesTags = new ArrayList<String>();
				}
				newPredicatesTags.addAll(mol.newPredicatesTags);
				if (predicateLiterals == null) {
					predicateLiterals = new BitSet();
				}

				for (int i = 0; i < mol.newPredicates.size(); i++) {
					if (mol.predicateLiterals.length() > i) {
						predicateLiterals.set(totalnewPredicates, mol.predicateLiterals.get(i));
					}
					totalnewPredicates++;
				}

			}

		}
		/* END> ADD NEW PREDICATES */

		/* START> ADD NEW TERMS IN PREDICATES */

		newTermsURIBNode.addAll(mol.newTermsURIBNodes);
		newTermsURIBNode_prefixes.addAll(mol.prefixesURIs);
		newTermsLiteral.addAll(mol.newTermsLiterals);
		/* END> ADD NEW TERMS IN PREDICATES */

		/* START> ADD OBJECTS IN PREDICATES */
		Iterator<Entry<Integer, ArrayList<Integer>>> it = mol.predicates.entrySet().iterator();
		Map.Entry<Integer, ArrayList<Integer>> pairs;
		while (it.hasNext()) {
			pairs = it.next();
			if (predicates.containsKey(pairs.getKey())) {
				predicates.get(pairs.getKey()).addAll(pairs.getValue());
			} else { // add new Predicate

				predicates.put(pairs.getKey(), pairs.getValue());
				BitSet newTermMarker = new BitSet();
				predicatesNewTermMarker.put(pairs.getKey(), newTermMarker);
				totalObjsperPredicate.put(pairs.getKey(), 0);

			}

			// Add predicate markers
			for (int i = 0; i < pairs.getValue().size(); i++) { // mark all objects
				if (mol.predicatesNewTermMarker.get(pairs.getKey()).length() > i) {
					predicatesNewTermMarker.get(pairs.getKey()).set(totalObjsperPredicate.get(pairs.getKey()),
							mol.predicatesNewTermMarker.get(pairs.getKey()).get(i));
					// update total objs per predicate
					// FIXME (it could be faster with an array)
				}
				// update total number of objects per predicate
				totalObjsperPredicate.put(pairs.getKey(), totalObjsperPredicate.get(pairs.getKey()) + 1);
			}
		}
		/* END> ADD OBJECTS IN PREDICATES */

	}

	/**
	 * Save Block to ouputstream
	 * 
	 * @param output
	 * @throws IOException
	 */
	public void save(OutputStream output) throws IOException {

		CRCOutputStream outCRC = new CRCOutputStream(output, new CRC16());

		IOUtil.writeString(outCRC, "$BLK"); // Cookie

		/**** STRUCTURAL CHANELS *****/

		// WRITE NEW PREDICATES
		outCRC = new CRCOutputStream(output, new CRC8());
		VByte.encode(outCRC, newPredicates.size());
		outCRC.writeCRC();
		DataOutputStream outData;
		if (newPredicates.size() > 0) {
			outCRC.setCRC(new CRC32());
			outData = new DataOutputStream(outCRC);
			for (int i = 0; i < newPredicates.size(); i++) {
				outData.writeUTF(newPredicates.get(i));
			}
			// Uncomment this to write a consecutive sequence of bytes
			// byte[] newPredsByte =
			// newPredicates.toString().getBytes(Charset.forName("UTF-8"));
			// IOUtil.writeBuffer(outCRC, newPredsByte, 0, newPredsByte.length);
			outCRC.writeCRC();

			// Print if predicates are literals, and their tags
			byte[] predicateLiteralsByte = predicateLiterals.toByteArray();

			outCRC = new CRCOutputStream(output, new CRC8());
			VByte.encode(outCRC, predicateLiterals.length());

			VByte.encode(outCRC, predicateLiteralsByte.length);
			outCRC.writeCRC();
			if (predicateLiterals.length() > 0) { // there are new predicates
				outCRC.setCRC(new CRC32());
				IOUtil.writeBuffer(outCRC, predicateLiteralsByte, 0, predicateLiteralsByte.length, null);
				outCRC.writeCRC();

				// WRITE TAGS

				outCRC = new CRCOutputStream(output, new CRC8());
				VByte.encode(outCRC, newPredicatesTags.size());

				outCRC.writeCRC();
				if (newPredicatesTags.size() > 0) {
					outCRC.setCRC(new CRC32());
					outData = new DataOutputStream(outCRC);
					for (int i = 0; i < newPredicatesTags.size(); i++) {
						outData.writeUTF(newPredicatesTags.get(i));
					}
					outCRC.writeCRC();
				}
			}

		}

		// WRITE STRUCTURE

		// Use SequenceFactory to be more generic, use log64 directly to speed up performance.
		// Sequence sq = SequenceFactory.createStream(CSVocabulary.SEQ_TYPE_LOG);
		SequenceLog64 sq = new SequenceLog64();
		sq.addIntegers(structureIDs);

		sq.save(output, null); // they have their own CRC check.
		sq.close();

		// WRITE NEW STRUCTURES MARKER
		byte[] newStructMarkersByte = newStructuresMarker.toByteArray();

		outCRC = new CRCOutputStream(output, new CRC8());
		VByte.encode(outCRC, newStructuresMarker.length());
		VByte.encode(outCRC, newStructMarkersByte.length);
		outCRC.writeCRC();
		if (newStructuresMarker.length() > 0) { // test if there are new structures
			outCRC.setCRC(new CRC32());
			IOUtil.writeBuffer(outCRC, newStructMarkersByte, 0, newStructMarkersByte.length, null);
			outCRC.writeCRC();

			// WRITE NEW STRUCTURES

			// write list of predicates and repetitions
			sq = new SequenceLog64();
			sq.addIntegers(newStructure_listPreds);

			sq.save(output, null); // they have their own CRC check.
			sq.close();

			// write objects discrete if present
			outCRC = new CRCOutputStream(output, new CRC8());
			VByte.encode(outCRC, newStructure_allObjectsDiscrete.size());

			outCRC.writeCRC();
			if (newStructure_allObjectsDiscrete.size() > 0) {
				DeflaterOutputStream deflOutput = new DeflaterOutputStream(output);
				outData = new DataOutputStream(deflOutput);
				for (int i = 0; i < newStructure_allObjectsDiscrete.size(); i++)
					outData.writeUTF(newStructure_allObjectsDiscrete.get(i));
				// Uncomment this to write a consecutive sequence of bytes
				// byte[] newStructByte = newStructures.toString().getBytes(
				// Charset.forName("UTF-8"));
				// IOUtil.writeBuffer(outCRC, newStructByte, 0,
				// newStructByte.length);
				// outCRC.writeCRC();
				deflOutput.flush();
				deflOutput.finish();

			}
		}
		// WRITE SUBJECTS
		// Use SequenceFactory to be more generic, use log64 directly to speed up performance.
		// Sequence sqSubjs = SequenceFactory.createStream(CSVocabulary.SEQ_TYPE_LOG);
		if (store_subj_dictionary) {
			SequenceLog64 sqSubjs = new SequenceLog64();
			sqSubjs.addIntegers(subjectIDs);

			sqSubjs.save(output, null); // they have their own CRC check
			sqSubjs.close();

			// WRITE NEW SUBJECTS MARKER
			byte[] newSubjMarkersByte = newSubjectsMarker.toByteArray();
			outCRC = new CRCOutputStream(output, new CRC8());
			VByte.encode(outCRC, newSubjectsMarker.length());
			VByte.encode(outCRC, newSubjMarkersByte.length);
			outCRC.writeCRC();
			if (newSubjectsMarker.length() > 0) {
				outCRC.setCRC(new CRC32());
				IOUtil.writeBuffer(outCRC, newSubjMarkersByte, 0, newSubjMarkersByte.length, null);
				outCRC.writeCRC();

			}
		}
		// WRITE NEW SUBJECTS
		outCRC = new CRCOutputStream(output, new CRC8());
		VByte.encode(outCRC, newSubjects.size());

		outCRC.writeCRC();
		if (newSubjects.size() > 0) {
			outCRC.setCRC(new CRC32());
			// WRITE prefixes
			// Use SequenceFactory to be more generic, use log64 directly to speed up performance.
			// Sequence sqSubjs = SequenceFactory.createStream(CSVocabulary.SEQ_TYPE_LOG);
			SequenceLog64 sqSubjs_prefixes = new SequenceLog64();
			sqSubjs_prefixes.addIntegers(newSubjects_prefixes);

			sqSubjs_prefixes.save(output, null); // they have their own CRC
			sqSubjs_prefixes.close(); // check

			// COMPRESSION

			// TODO Potential improvement: use CheckedOutputStream

			DeflaterOutputStream deflOutput = new DeflaterOutputStream(output);
			outData = new DataOutputStream(deflOutput);

			for (int i = 0; i < newSubjects.size(); i++) {
				outData.writeUTF(newSubjects.get(i));
			}

			deflOutput.flush();
			deflOutput.finish();
		}

		/**** PREDICATE CHANELS *****/
		// WRITE NEW TERMS

		// WRITE NEW TERMS URIS or BNODES
		outCRC = new CRCOutputStream(output, new CRC8());
		VByte.encode(outCRC, newTermsURIBNode.size());
		outCRC.writeCRC();

		if (newTermsURIBNode.size() > 0) {

			// WRITE prefixes

			SequenceLog64 sqNewTerm_prefixes = new SequenceLog64();
			sqNewTerm_prefixes.addIntegers(newTermsURIBNode_prefixes);

			sqNewTerm_prefixes.save(output, null); // they have their own CRC check
			sqNewTerm_prefixes.close();
			outCRC.setCRC(new CRC32());

			// COMPRESSION

			// TODO Potential improvement: use CheckedOutputStream

			DeflaterOutputStream deflOutput = new DeflaterOutputStream(output);
			outData = new DataOutputStream(deflOutput);

			for (int i = 0; i < newTermsURIBNode.size(); i++) {
				outData.writeUTF(newTermsURIBNode.get(i));
			}

			deflOutput.flush();
			deflOutput.finish();

		}
		// WRITE NEW TERMS LITERALS
		outCRC = new CRCOutputStream(output, new CRC8());
		VByte.encode(outCRC, newTermsLiteral.size());

		outCRC.writeCRC();
		if (newTermsLiteral.size() > 0) {
			outCRC.setCRC(new CRC32());

			// COMPRESSION

			// TODO Potential improvement: use CheckedOutputStream
			DeflaterOutputStream deflOutput = new DeflaterOutputStream(output);
			outData = new DataOutputStream(deflOutput);

			for (int i = 0; i < newTermsLiteral.size(); i++) {
				outData.writeUTF(newTermsLiteral.get(i));
			}

			deflOutput.flush();
			deflOutput.finish();
		}

		// Iterate predicates
		Iterator<Integer> predsIt = predicates.keySet().iterator();
		Integer tempPred;

		// save the number of predicates (in order to iterate through the channels
		outCRC = new CRCOutputStream(output, new CRC8());
		VByte.encode(outCRC, predicates.size());
		// VByte.encode(outCRC, newTermsByte.length);
		outCRC.writeCRC();

		outCRC.setCRC(new CRC32());

		while (predsIt.hasNext()) {
			tempPred = predsIt.next();
			// save predicateID
			VByte.encode(outCRC, tempPred);
			outCRC.writeCRC();
			// save newTerms marker
			byte[] newPredMarkersByte = predicatesNewTermMarker.get(tempPred).toByteArray();
			outCRC = new CRCOutputStream(output, new CRC8());
			VByte.encode(outCRC, predicatesNewTermMarker.get(tempPred).length());
			VByte.encode(outCRC, newPredMarkersByte.length);
			outCRC.writeCRC();
			if (predicatesNewTermMarker.get(tempPred).length() > 0) {
				outCRC.setCRC(new CRC32());
				IOUtil.writeBuffer(outCRC, newPredMarkersByte, 0, newPredMarkersByte.length, null);
				outCRC.writeCRC();
			}
			// save objects

			SequenceLog64 sqPred = new SequenceLog64();
			sqPred.addIntegers(predicates.get(tempPred));

			sqPred.save(output, null); // they have their own CRC check
			sqPred.close();
		}
	}

	public String toString() {
		String ret = "NumResources: " + subjectIDs.size() + "\n";
		for (int i = 0; i < subjectIDs.size(); i++) {
			ret = ret + subjectIDs.get(i) + "\n";
		}
		return ret;
	}

	/**
	 * @return Number of resources (subjects) in the block
	 */
	public int length() {
		return subjectIDs.size();
	}

}
