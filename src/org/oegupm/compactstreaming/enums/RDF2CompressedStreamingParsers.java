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
package org.oegupm.compactstreaming.enums;

/**
 * Enumeration of the different parsers to get RDF2CompressedStreaming
 * 
 */
public enum RDF2CompressedStreamingParsers {
	/**
	 * Based on Consuming BlockSize each loop
	 * 
	 */
	Buffered,
	/**
	 * Based on Consuming BlockSize each loop and without dictionary of objects
	 * 
	 */
	BufferedNoDict,
	/**
	 * Based on Consuming T-seconds each loop
	 * 
	 */
	Time

}