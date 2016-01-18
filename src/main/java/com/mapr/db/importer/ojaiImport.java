package com.mapr.db.importer;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.HelpFormatter;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ojaiImport {

    private static final Logger log = Logger.getLogger(ojaiImport.class.getName());
    private String[] args = null;
    private Options options = new Options();

    public static void main( String[] args ) {
        CommandLineParser parser = new BasicParser();
        CommandLine line = null;

        ojaiImport i = new ojaiImport(parser, line, args);
    }

    public ojaiImport(CommandLineParser parser, CommandLine line, String[] args) {

        try {
            line = parser.parse(createOptions(), args);
        } catch (ParseException exp ) {
            System.out.println( "Unexpected exception:" + exp.getMessage() );
        }

        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ojaiimport", options);
            System.exit(0);
        }
        if (line.hasOption("file")) {
            System.out.println("Input file: " + line.getOptionValue("file"));
        }
        if (line.hasOption("key")) {
            System.out.println("'_id' Element: " + line.getOptionValue("key"));
        }

        streamParser p = new streamParser(line);
        System.exit(0);
    }

    private Options createOptions() {
        options.addOption( "f", "file", true, "Use this file as input instead of standard in" );
        options.addOption( "k", "key", true, "JSON element to use as the '_id' (rowkey)" );
        options.addOption( "h", "help", false, "This message" );

        return options;
    }

}
