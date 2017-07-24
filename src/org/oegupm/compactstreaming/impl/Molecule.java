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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.oegupm.compactstreaming.exceptions.BlockException;

/**
 * Molecule of triples sharing the same subject
 * 
 * @author javi
 * 
 */
public class Molecule {

	// STRUCTURAL CHANNELS
	protected Integer subject;
	protected Integer newSubject_prefix = 0;
	protected String newSubject = null;
	protected ArrayList<String> newPredicates;
	protected BitSet predicateLiterals; // mark which predicate stores literals
	protected ArrayList<String> newPredicatesTags;
	// protected ArrayList<Integer> structureIDs;
	protected Integer structureID;
	protected Boolean isNewStructure = false;

	protected ArrayList<Integer> newStructure_listPreds;
	protected ArrayList<String> newStructure_allObjectsDiscrete;

	// PREDICATE CHANNELS
	protected Map<Integer, ArrayList<Integer>> predicates; // FIXME when considering objects out of dictionary, this could be
															// ArrayList<String> too
	protected Map<Integer, BitSet> predicatesNewTermMarker;
	protected ArrayList<String> newTermsURIBNodes;
	protected ArrayList<String> newTermsLiterals;
	protected ArrayList<Integer> prefixesURIs;

	protected Map<Integer, Integer> totalObjsperPredicate; // to count markers

	/**
	 * Main constructor
	 */
	public Molecule() {
		predicates = new HashMap<Integer, ArrayList<Integer>>();
		predicatesNewTermMarker = new HashMap<Integer, BitSet>();

		newTermsURIBNodes = new ArrayList<String>();
		newTermsLiterals = new ArrayList<String>();
		prefixesURIs = new ArrayList<Integer>();
		totalObjsperPredicate = new HashMap<Integer, Integer>();

	}

	/**
	 * Add the structure of the molecule, which is new
	 * 
	 * @param structure
	 * @param listPredsinStructure
	 * @param allObjectsDiscrete
	 */
	public void addNewStructure(Integer structure, ArrayList<Integer> listPredsinStructure, ArrayList<String> allObjectsDiscrete) {
		newStructure_listPreds = new ArrayList<Integer>(listPredsinStructure);
		if (allObjectsDiscrete != null)
			newStructure_allObjectsDiscrete = new ArrayList<String>(allObjectsDiscrete);
		structureID = structure;
		isNewStructure = true;
	}

	public void addStructure(Integer structure) {
		structureID = structure;
	}

	public void addNewPredicate(Integer predicate, String newPredicate, Boolean isLiteral, String tag) {
		if (newPredicates == null) {
			newPredicates = new ArrayList<String>();
		}
		newPredicates.add(newPredicate);
		ArrayList<Integer> newObjects = new ArrayList<Integer>();
		predicates.put(predicate, newObjects);
		BitSet newTermMarker = new BitSet();
		predicatesNewTermMarker.put(predicate, newTermMarker);
		totalObjsperPredicate.put(predicate, 0);
		if (predicateLiterals == null) {
			predicateLiterals = new BitSet();
		}
		predicateLiterals.set(newPredicates.size() - 1, isLiteral);
		if (newPredicatesTags == null) {
			newPredicatesTags = new ArrayList<String>();
		}
		newPredicatesTags.add(tag);
	}

	public void addNewPredicateInconsistent(Integer predicate, String newPredicate) {
		if (newPredicates == null) {
			newPredicates = new ArrayList<String>();
		}
		newPredicates.add(newPredicate);
		ArrayList<Integer> newObjects = new ArrayList<Integer>();
		predicates.put(predicate, newObjects);
		BitSet newTermMarker = new BitSet();
		predicatesNewTermMarker.put(predicate, newTermMarker);
		totalObjsperPredicate.put(predicate, 0);

	}

	public void addObject(Integer predicate, Integer object) throws BlockException {
		addObject(predicate, object, false);
	}

	public void addObject(Integer predicate, Integer object, boolean isNew) throws BlockException {

		if (predicates.containsKey(predicate)) {

			predicates.get(predicate).add(object);
			predicatesNewTermMarker.get(predicate).set(totalObjsperPredicate.get(predicate), isNew);

			totalObjsperPredicate.put(predicate, totalObjsperPredicate.get(predicate) + 1);
		} else {

			// add new Predicate
			ArrayList<Integer> newObjects = new ArrayList<Integer>();
			newObjects.add(object);

			predicates.put(predicate, newObjects);

			BitSet newTermMarker = new BitSet();
			newTermMarker.set(0, isNew);
			predicatesNewTermMarker.put(predicate, newTermMarker);
			totalObjsperPredicate.put(predicate, 1);

		}

	}

	public void addnewObjectURIBNode(Integer predicate, Integer object, String newObject, Integer preffixURI) throws BlockException {
		addObject(predicate, object, true);
		prefixesURIs.add(preffixURI);
		newTermsURIBNodes.add(newObject);
	}

	/*
	 * Add without prefix
	 */
	public void addnewObjectURIBNode(Integer predicate, Integer object, String newObject) throws BlockException {
		addObject(predicate, object, true);
		newTermsURIBNodes.add(newObject);
	}

	public void addnewObjectLiteral(Integer predicate, Integer object, String newObject) throws BlockException {
		addObject(predicate, object, true);
		newTermsLiterals.add(newObject);
	}

}
