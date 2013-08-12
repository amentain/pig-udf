package com.xeenon.pig.tools;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: xeenon
 * Date: 08.08.13
 * Time: 16:41
 */
public class JsonLogValidator {
    private File log;

    public JsonLogValidator(String fileName) throws IOException {
        log = new File(fileName);
        if (!log.isFile()) {
            throw new IOException("File not found: "+ fileName);
        }
    }

    public void validate() throws IOException {
        FileReader fileReader = new FileReader(log);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        Long line = 0l;
        String json;
        while ((json = bufferedReader.readLine()) != null) {
            line++;
            if (!validateLine(json)) {
                System.err.println("on line "+ line +":: "+ json);
            }
        }

        bufferedReader.close();
        fileReader.close();
    }

    private boolean validateLine(String json) {

        boolean valid = false;
        try {
            final JsonParser parser = new ObjectMapper().getJsonFactory()
                    .createJsonParser(json);
            while (parser.nextToken() != null) {
            }
            valid = true;
        } catch (JsonParseException jpe) {
            jpe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return valid;
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1)
            return;

        for (String fileName : args) {
            JsonLogValidator validator = new JsonLogValidator(fileName);
            validator.validate();
        }
    }
}
