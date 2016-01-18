package com.mapr.db.importer;



import java.io.File;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;

public class streamParser {

    public streamParser(CommandLine line) {
        try {

            JsonFactory jfactory = new JsonFactory();

            /*** read from file ***/
            JsonParser jParser = jfactory.createJsonParser(new File("c:\\user.json"));

            // loop until token equal to "}"
            while (jParser.nextToken() != JsonToken.END_OBJECT) {

                String fieldname = jParser.getCurrentName();
                if ("name".equals(fieldname)) {

                    // current token is "name",
                    // move to next, which is "name"'s value
                    jParser.nextToken();
                    System.out.println(jParser.getText()); // display mkyong

                }

                if ("age".equals(fieldname)) {

                    // current token is "age",
                    // move to next, which is "name"'s value
                    jParser.nextToken();
                    System.out.println(jParser.getIntValue()); // display 29

                }

                if ("messages".equals(fieldname)) {

                    jParser.nextToken(); // current token is "[", move next

                    // messages is array, loop until token equal to "]"
                    while (jParser.nextToken() != JsonToken.END_ARRAY) {

                        // display msg1, msg2, msg3
                        System.out.println(jParser.getText());

                    }

                }

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
