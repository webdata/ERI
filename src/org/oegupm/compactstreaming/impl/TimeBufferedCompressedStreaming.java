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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.oegupm.compactstreaming.RDF2CompressedStreaming;
import org.oegupm.compactstreaming.dictionary.Dictionary;
import org.oegupm.compactstreaming.dictionary.LRUDictionary;
import org.oegupm.compactstreaming.exceptions.BlockException;
import org.oegupm.compactstreaming.exceptions.DictionaryException;
import org.oegupm.compactstreaming.options.CSSpecification;
import org.rdfhdt.hdt.triples.TripleString;


/**
 * This class is under development...
 * 
 * @author javi
 *
 */
public class TimeBufferedCompressedStreaming implements RDF2CompressedStreaming {

	class WorkerThread implements Runnable {

		String subject;

		public WorkerThread(String subject) {
			this.subject = subject;
		}

		@Override
		public void run() {
			System.out.println("Worker subject:" + subject);
			// do processing
			ArrayList<TripleString> elements = subjectsToProcess.get(subject);
			// uncomment to show triples in the molecule
			// for (int i=0;i<elements.size();i++)
			// System.out.println("Worker Triples:"+elements.get(i));
			processMolecule(elements);
			subjectsToProcess.remove(subject);
		}

		public void processMolecule(ArrayList<TripleString> elements) {

			System.out.println("Processing Elements in worker");
			Molecule mol = new Molecule();
			// Convert Subject-String to Subject-ID
			if (subjects.containsKey(subject)) {
				tempIDSubject = subjects.get(subject);
				// /currentBlock.addSubject(tempIDSubject); //add subject of the
				// molecule

			} else {
				try {
					tempIDSubject = subjects.insert(subject);
				} catch (DictionaryException e) {
					System.err.println("LRU Dictionary exception");
					e.printStackTrace();
				}
				mol.newSubject = subject;
				// //currentBlock.addNewSubject(tempIDSubject, subject); //add
				// new string subject
			}
			// set molecule subject
			mol.subject = tempIDSubject;

			String predicateStructure = "";
			Integer currPredicate = -1;
			System.out.println("Molecule size:" + elements.size());
			for (int i = 0; i < elements.size(); i++) {

				if (predicates.containsKey(elements.get(i).getPredicate().toString())) {
					tempIDPredicate = predicates.get(elements.get(i).getPredicate().toString());
					// check dictionary of objects for this predicate
					// FIX: Only if it is a predicate of URIs --> how do we
					// check this?
					// FIX: Extend also to repeated literals? --> how do we
					// check this?
					tempDictionary = objects.get(predicates.get(elements.get(i).getPredicate()) - 1);
					if (tempDictionary.containsKey(elements.get(i).getObject().toString())) {
						tempIDObject = tempDictionary.get(elements.get(i).getObject().toString());
						try {
							// //currentBlock.addObject(tempIDPredicate,
							// tempIDObject);
							mol.addObject(tempIDPredicate, tempIDObject);
						} catch (BlockException e) {
							System.err.println("Block exception");
							e.printStackTrace();
						}
					} else {
						try {
							tempIDObject = tempDictionary.insert(elements.get(i).getObject().toString());
							// //currentBlock.addnewObject(tempIDPredicate,
							// // tempIDObject,
							// elements.get(i).getObject().toString());
							mol.addnewObjectURIBNode(tempIDPredicate, tempIDObject, elements.get(i).getObject().toString(), 0); // FIXME add
																																// prefixes
						} catch (DictionaryException e) {
							System.err.println("LRU Dictionary exception");
							e.printStackTrace();
						} catch (BlockException e) {
							System.err.println("Block exception");
							e.printStackTrace();
						}

					}

				} else {
					// insert new Predicate and create LRUDictionary for it
					tempIDPredicate = predicates.size() + 1;
					predicates.put(elements.get(i).getPredicate().toString(), predicates.size() + 1);
					LRUDictionary<String, Integer> newObjects = new LRUDictionary<String, Integer>(MAX_SIZE_DICTIONARY);
					try {
						tempIDObject = newObjects.insert(elements.get(i).getObject().toString());
						objects.add(newObjects);

						// // currentBlock.addNewPredicate(tempIDPredicate,
						// elements.get(i)
						// // .getPredicate().toString());
						// // currentBlock.addnewObject(tempIDPredicate,
						// tempIDObject,
						// // elements.get(i).getObject().toString());
						mol.addNewPredicate(tempIDPredicate, elements.get(i).getPredicate().toString(), false, "");// FIXME repair with new
																													// molecule of literals
						mol.addnewObjectURIBNode(tempIDPredicate, tempIDObject, elements.get(i).getObject().toString(), 0); // FIXME add
																															// prefixes
					} catch (DictionaryException e) {
						System.err.println("LRU Dictionary exception");
						e.printStackTrace();
					} catch (BlockException e) {
						System.err.println("Block exception");
						e.printStackTrace();
					}

				}
				if (currPredicate != tempIDPredicate) {
					predicateStructure += ";" + tempIDPredicate;
				} else {
					predicateStructure += ",";

				}
				currPredicate = tempIDPredicate;

			}
			// Insert structure
			if (structures.containsKey(predicateStructure)) {
				tempIDStructure = structures.get(predicateStructure);

				// //currentBlock.addStructure(tempIDStructure); //add structure
				// of the molecule
				mol.addStructure(tempIDStructure); // add structure of the
													// molecule
			} else {
				try {
					tempIDStructure = structures.insert(predicateStructure);
				} catch (DictionaryException e) {
					System.err.println("LRU Dictionary exception");
					e.printStackTrace();
				}
				// //currentBlock.addNewStructure(tempIDStructure,
				// predicateStructure); //add new string structure

				// store new structure in compact way (list of predicate-ID and its repetitions and objects discrete
				ArrayList<Integer> listPredsinStructure = new ArrayList<Integer>(5);

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

				mol.addNewStructure(tempIDStructure, listPredsinStructure, null); // add
				// new
				// string
				// structure

			}

			currentBlock.processMolecule(mol);

			/*
			 * TEST System.out.print("Triple-ID Inserted: "); System.out.print(subjects.get(triple.getSubject())+" ");
			 * System.out.print(predicates.get(triple.getPredicate())+" "); System
			 * .out.println(objects.get(predicates.get(triple.getPredicate( ))-1).get(triple.getObject()));
			 */
			System.out.print("Temp-IDs: ");
			System.out.println(tempIDSubject + " " + tempIDPredicate + " " + tempIDObject);
			System.out.println();

		}

	}

	public static final int MAX_SIZE_DICTIONARY = 2; // Max number of elements
														// per dictionary before
														// LRU replacement
	public static final int BLOCK_SIZE = 4; // Number of triples in each Block
	public static final int SECONDS_CHECK_MOLECULE = 1;

	/*
	 * We could limit the number of triples in each molecule
	 */
	// public static final int MAX_TRIPLES_IN_MOLECULE = 10;
	// private int sizeCurrentMolecule; //num of triples

	private long originalFileSize;
	private long ntTriplesSize;
	private long numTriples;

	private LRUDictionary<String, Integer> subjects;
	// one dictionary per predicate
	private ArrayList<LRUDictionary<String, Integer>> objects;
	// uncomment to create one global dictionary of objects
	// private LRUDictionary<String,Integer> objects;

	private LRUDictionary<String, Integer> structures;

	private Map<String, Integer> predicates;

	private int tempIDSubject = 0;
	private int tempIDPredicate = 0;
	private int tempIDObject = 0;
	private int tempIDStructure = 0;

	private int countTriples = 0;

	Block currentBlock;
	private LRUDictionary<String, Integer> tempDictionary;

	private Map<String, ArrayList<TripleString>> subjectsToProcess;

	ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(5);

	private OutputStream out;

	public TimeBufferedCompressedStreaming(OutputStream out, CSSpecification spec, Dictionary preffix,
			Map<String, Boolean> predicateDiscrete) {
		subjects = new LRUDictionary<String, Integer>(MAX_SIZE_DICTIONARY);
		predicates = new ConcurrentHashMap<String, Integer>();
		objects = new ArrayList<LRUDictionary<String, Integer>>(MAX_SIZE_DICTIONARY);
		structures = new LRUDictionary<String, Integer>(MAX_SIZE_DICTIONARY);

		currentBlock = new Block();

		subjectsToProcess = new ConcurrentHashMap<String, ArrayList<TripleString>>();

		this.out = out;
	}

	public void insert(TripleString triple) throws DictionaryException, BlockException {

		if ((countTriples % BLOCK_SIZE) == 0) {
			if (countTriples != 0)
				try {
					currentBlock.save(out);
				} catch (IOException e) {
					System.err.println("IO exception saving Block");
					e.printStackTrace();
				} // FIX Needed operation, for instance save
					// to disk

			currentBlock = new Block();
		}

		/*
		 * if (subjects.containsKey(triple.getSubject().toString())){ tempIDSubject = subjects.get(triple.getSubject().toString());
		 * //currentBlock.addSubject(tempIDSubject); } else { tempIDSubject = subjects.insert(triple.getSubject().toString());
		 * //currentBlock.addNewSubject(tempIDSubject, triple.getSubject().toString()); }
		 */

		if (subjectsToProcess.containsKey(triple.getSubject().toString())) {
			subjectsToProcess.get(triple.getSubject().toString()).add(triple);
			System.out.println("Triple added to pool:" + triple.getSubject() + " " + triple.getPredicate() + " " + triple.getObject());
		} else {
			ArrayList<TripleString> newTriples = new ArrayList<TripleString>();
			newTriples.add(triple);
			subjectsToProcess.put(triple.getSubject().toString(), newTriples);
			WorkerThread worker = new WorkerThread(triple.getSubject().toString());
			scheduledThreadPool.schedule(worker, SECONDS_CHECK_MOLECULE, TimeUnit.SECONDS);
			System.out.println("New Subject added to pool:" + triple.getSubject() + " " + triple.getPredicate() + " " + triple.getObject());
		}

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			System.err.println("Thread Interrupted");
			e.printStackTrace();
		}
		countTriples++;

	}

	public void endProcessing() {
		if (currentBlock.length() > 0) {
			try {
				currentBlock.save(out);
			} catch (IOException e) {
				System.err.println("IO exception saving Block");
				e.printStackTrace();
			}
		}
	}

	public long getOriginalFileSize() {
		return originalFileSize;
	}

	public void setOriginalFileSize(long originalFileSize) {
		this.originalFileSize = originalFileSize;
	}

	public long getNtTriplesSize() {
		return ntTriplesSize;
	}

	public void setNtTriplesSize(long ntTriplesSize) {
		this.ntTriplesSize = ntTriplesSize;
	}

	public long getNumTriples() {
		return numTriples;
	}

	public void setNumTriples(long numTriples) {
		this.numTriples = numTriples;
	}

	@Override
	public String printConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startProcessing() {
		// TODO Auto-generated method stub

	}

	@Override
	public long getNumBlocks() {
		// TODO Auto-generated method stub
		return 0;
	}
}
