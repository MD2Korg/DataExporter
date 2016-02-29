/*
 * Copyright (c) 2016, The University of Memphis, MD2K Center
 * - Timothy Hnat <twhnat@memphis.edu>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import org.apache.commons.cli.*;
import org.md2k.dataexporter.DataExport;

import java.util.List;

/**
 * Main class to run DataExport
 */
public class Main {
    /**
     * Main method
     * @param args options to configure execution
     */
    public static void main(String[] args) {

        Options options = new Options();

        Option databaseFile = new Option("d", "database", true, "mCerebrum SQLite Database File");
        databaseFile.setRequired(true);
        databaseFile.setArgName("file");
        options.addOption(databaseFile);

        Option jsonFlag = new Option("j", "json", false, "enable JSON file output");
        options.addOption(jsonFlag);

        Option csvFlag = new Option("c", "csv", false, "enable CSV file output");
        options.addOption(csvFlag);

        Option publish = new Option("p", "publish", true, "configure publishing to webservice");
        publish.setArgName("URL");
        options.addOption(publish);

        Option help = new Option("h", "help", false, "print this message" );
        options.addOption(help);

        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            if(line.hasOption("help")) {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "java -jar dataexporter.jar", options, true);
            } else {

                DataExport de = new DataExport(line.getOptionValue("database"));

                List<Integer> ids = de.getIDs();
                for (Integer id : ids) {
                    System.out.println("Exporting data stream: " + id);
                    if (line.hasOption("publish")) {
                        if (de.publishGzipJSONData(line.getOptionValue("publish"), id)) {
                            System.out.println("Success");
                        } else {
                            System.out.println("Failure");
                        }
                    }
                    if (line.hasOption("csv")) {
                        de.writeCSVDataFile(id);
                    }
                    if (line.hasOption("json")) {
                        de.writeJSONDataFile(id);
                    }
                }
            }
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "java -jar dataexporter.jar", options, true);

        }

    }
}