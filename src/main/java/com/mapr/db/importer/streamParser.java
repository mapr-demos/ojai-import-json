package com.mapr.db.importer;



import java.io.File;
import java.io.IOException;
import java.util.Vector;
import org.apache.commons.cli.CommandLine;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;

import com.mapr.db.Condition;
import com.mapr.db.MapRDB;
import com.mapr.db.DBDocument;
import com.mapr.db.ojai.DBDocumentBuilder;
import com.mapr.db.Mutation;
import com.mapr.db.Table;
import com.mapr.db.exceptions.DocumentExistsException;
import org.ojai.DocumentStream;
import org.ojai.DocumentBuilder;

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
            boolean inArray = false;

            while (jParser.nextToken() != null) {

                switch (jParser.getCurrentToken()) {
                case END_ARRAY:
                    depth--;
                    b.endArray();
                    inArray = false;
                    break;
                case END_OBJECT:
                    depth--;
                    if (depth == 0) {
                        System.out.println(b.getDocument().asJsonString());
                    }
                    b = MapRDB.newDocumentBuilder();
                    break;
                case START_ARRAY:
                    depth++;
                    b.addNewArray();
                    inArray = true;
                    break;
                case START_OBJECT:
                    depth++;
                    break;

// Not sure about these guys
                case FIELD_NAME:
                case NOT_AVAILABLE:
                case VALUE_EMBEDDED_OBJECT:
                    break;
// These actually add things to the arr or object
                case VALUE_NULL:
                    if (!inArray) {
                        b.putNull(jParser.getCurrentName());
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                    if (!inArray) {
                        b.put(jParser.getCurrentName(), jParser.getFloatValue());
                    } else {
                        b.add(jParser.getFloatValue());
                    }
                    break;
                case VALUE_NUMBER_INT:
                    if (!inArray) {
                        b.put(jParser.getCurrentName(), jParser.getIntValue());
                    } else {
                        b.add(jParser.getIntValue());
                    }
                    break;
                case VALUE_STRING:
                    if (!inArray) {
                        b.put(jParser.getCurrentName(), jParser.getText());
                    } else {
                        b.add(jParser.getText());
                    }
                    break;
                case VALUE_FALSE:
                case VALUE_TRUE:
                    if (!inArray) {
                        b.put(jParser.getCurrentName(), jParser.getBooleanValue());
                    } else {
                        b.add(jParser.getBooleanValue());
                    }
                    break;
                }
                String fieldname = jParser.getCurrentName();
                System.out.println("["+depth+"]   " + jParser.getCurrentToken().toString() + ": "+ fieldname); // display mkyong

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
