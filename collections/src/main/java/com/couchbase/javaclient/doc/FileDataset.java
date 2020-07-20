package com.couchbase.javaclient.doc;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.javaclient.doc.DocTemplate;
import com.github.javafaker.Faker;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

abstract public class FileDataset {

    protected void readFile (String path, int numOps, List<JsonObject> records) {
        JSONParser jsonParser = new JSONParser();

        int i = 1;
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {

            String line;

            while ((line = br.readLine()) != null) {

                if (line.isEmpty()) break;
                try {
                    Object obj = jsonParser.parse(line);
                    JSONObject json = (JSONObject) obj;
                    parseAndStoreJsonObject(json);
                } catch (Exception e) {
                    count++;
                }
                if (records.size() >= numOps) {
                    return;
                }
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        i = 0;
        while (records.size() < numOps) {
            records.add(records.get(i++));
        }
        System.out.println("Counter " + count);

    }

    abstract protected void parseAndStoreJsonObject(JSONObject simpleJson);

}
