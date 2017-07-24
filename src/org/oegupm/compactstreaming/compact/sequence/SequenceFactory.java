/*
 * Copyright (C) 2014 
 *  - Ontology Engineering Group (OEG), http://www.oeg-upm.net/
 *  - Javier D. Fernandez, <jdfernandez@fi.upm.es>
 * 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Lesser Public License as published by
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
package org.oegupm.compactstreaming.compact.sequence;

import java.io.IOException;
import java.io.InputStream;

import org.oegupm.compactstreaming.CSVocabulary;
import org.oegupm.compactstreaming.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.compact.sequence.Sequence;
import org.rdfhdt.hdt.compact.sequence.SequenceInt32;
import org.rdfhdt.hdt.compact.sequence.SequenceLog64;

/**
 * Factory of Sequences of numbers
 * 
 * @author javi
 * 
 */
public class SequenceFactory {
	public static final byte TYPE_SEQLOG = 1;
	public static final byte TYPE_SEQ32 = 2;
	public static final byte TYPE_SEQ64 = 3;

	public static Sequence createStream(String name) {
		if (name == null) {
			return new SequenceLog64();
		} else if (name.equals(CSVocabulary.SEQ_TYPE_INT32)) {
			return new SequenceInt32();
		} else if (name.equals(CSVocabulary.SEQ_TYPE_LOG)) {
			return new SequenceLog64();
		}
		return new SequenceLog64();
	}

	public static Sequence createStream(InputStream input) throws IOException {
		input.mark(1);
		int type = input.read();
		input.reset();
		switch (type) {
		case TYPE_SEQLOG:
			return new SequenceLog64();
		case TYPE_SEQ32:
			return new SequenceInt32();
		case TYPE_SEQ64:
			return new SequenceLog64();
		}
		throw new IllegalFormatException("Implementation not found for Sequence with code " + type);
	}

}
