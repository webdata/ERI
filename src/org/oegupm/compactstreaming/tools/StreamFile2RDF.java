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

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.oegupm.compactstreaming.CompressedStreaming2RDF;
import org.oegupm.compactstreaming.impl.CompressedStreaming2RDFExporterFactory;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.util.StopWatch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

/**
 * Class to parse a Compressed Streaming file and export to an RDF file in Ntriples
 * 
 * @author javi
 * 
 */
public class StreamFile2RDF {

	public String InputStream = null;
	public String rdfOutput = null;

	/* START> Description of parameters */
	@Parameter(description = "<input RDF_Comp> <outputRDF>")
	public List<String> parameters = Lists.newArrayList();

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

		StopWatch sw = new StopWatch();
		PrintStream out = null;
		if (rdfOutput.equals("stdout")) {
			out = System.out;
		} else {
			out = new PrintStream(rdfOutput, "UTF-8");
		}

		CompressedStreaming2RDFExporterFactory exporterFactory = new CompressedStreaming2RDFExporterFactory();
		CompressedStreaming2RDF exporter = exporterFactory.loadFromFile(InputStream, out); // launch exporter

		out.close();
		if (!quiet) { // Show basic stats

			System.out.println("- Conversion Time: " + sw.stopAndShow());
			System.out.println("- Number of Triples: " + exporter.getNumTriples());
			System.out.println("- Number of Blocks: " + exporter.getNumBlocks());
		}

	}

	public static void main(String[] args) throws Throwable {
		StreamFile2RDF streamfile2rdf = new StreamFile2RDF();
		JCommander com = new JCommander(streamfile2rdf, args);
		com.setProgramName("stream2file");

		if (streamfile2rdf.parameters.size() == 1) {
			System.err.println("No output file specified, writing to standard output.");
			streamfile2rdf.rdfOutput = "stdout";
			streamfile2rdf.InputStream = streamfile2rdf.parameters.get(0);

		} else if (streamfile2rdf.parameters.size() == 2) {
			streamfile2rdf.InputStream = streamfile2rdf.parameters.get(0);
			streamfile2rdf.rdfOutput = streamfile2rdf.parameters.get(1);

		} else {
			com.usage();
			System.exit(1);
		}

		System.out.println("Converting '" + streamfile2rdf.InputStream + "' to Stream File'" + streamfile2rdf.rdfOutput + "'");

		streamfile2rdf.execute();
		System.out.println("Bye!");
		System.exit(0);
	}

}
