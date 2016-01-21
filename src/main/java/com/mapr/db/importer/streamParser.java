package com.mapr.db.importer;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.ojai.DocumentBuilder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.mapr.db.MapRDB;

public class streamParser {

    private int depth = 0;

    public streamParser(CommandLine line) {
        try {

            JsonFactory jfactory = new JsonFactory();
            JsonParser jParser = null;

            if (line.hasOption("f")) {
                jParser = jfactory.createJsonParser(new File(line.getOptionValue("file")));
            } else {
                jParser = jfactory.createJsonParser(System.in);
            }

            DocumentBuilder b = MapRDB.newDocumentBuilder();
            //boolean inArray = false;

            while (jParser.nextToken() != null) {
                String fieldName = jParser.getCurrentName();

                switch (jParser.getCurrentToken()) {
                case END_ARRAY:
                    depth--;
                    b.endArray();
                    // inArray = false; // incorrect to assume that inArray is false
                    break;
                case END_OBJECT:
                    b.endMap();
                    depth--;
                    if (depth == 0) {
                        System.out.println(b.getDocument().asJsonString());
                    }
                    break;
                case START_ARRAY:
                    if (fieldName == null) {
                        b.addNewArray();
                    } else {
                        b.putNewArray(fieldName);
                    }
                    depth++;
                    //inArray = true;
                    break;
                case START_OBJECT:
                    if (fieldName == null) {
                        b.addNewMap();
                    } else {
                        b.putNewMap(fieldName);
                    }
                    depth++;
                    break;

// Not sure about these guys
                case FIELD_NAME:
                case NOT_AVAILABLE:
                case VALUE_EMBEDDED_OBJECT:
                    break;
// These actually add things to the arr or object
                case VALUE_NULL:
                    if (fieldName != null) {
                        b.putNull(fieldName);
                    } else {
                        b.addNull();
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                  if (fieldName != null) {
                        b.put(fieldName, jParser.getDoubleValue());
                    } else {
                        b.add(jParser.getDoubleValue());
                    }
                    break;
                case VALUE_NUMBER_INT:
                    if (fieldName != null) {
                        b.put(fieldName, jParser.getLongValue());
                    } else {
                        b.add(jParser.getLongValue());
                    }
                    break;
                case VALUE_STRING:
                    if (fieldName != null) {
                        b.put(fieldName, jParser.getText());
                    } else {
                        b.add(jParser.getText());
                    }
                    break;
                case VALUE_FALSE:
                case VALUE_TRUE:
                    if (fieldName != null) {
                        b.put(fieldName, jParser.getBooleanValue());
                    } else {
                        b.add(jParser.getBooleanValue());
                    }
                    break;
                }
                System.out.println("["+depth+"]   " + jParser.getCurrentToken().toString() + ": "+ fieldName); // display mkyong

            }
            jParser.close();

        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
// JsonToken.END_ARRAY
// JsonToken.END_OBJECT
// JsonToken.FIELD_NAME
// JsonToken.NOT_AVAILABLE
// JsonToken.START_ARRAY
// JsonToken.START_OBJECT
// JsonToken.VALUE_EMBEDDED_OBJECT
// JsonToken.VALUE_FALSE
// JsonToken.VALUE_NULL
// JsonToken.VALUE_NUMBER_FLOAT
// JsonToken.VALUE_NUMBER_INT
// JsonToken.VALUE_STRING
// JsonToken.VALUE_TRUE
