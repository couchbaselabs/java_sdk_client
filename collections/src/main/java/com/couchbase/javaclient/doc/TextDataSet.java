package com.couchbase.javaclient.doc;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TextDataSet implements DocTemplate {

    private List<JsonObject> records = new ArrayList<>();
    private String dataSetName;

    TextDataSet(DocSpec docSpec) {
        this.dataSetName = docSpec.get_template();
        readFile(docSpec.getDataFile(), docSpec.get_num_ops());
    }

    protected void parseAndStoreJsonObject(org.json.simple.JSONObject simpleJson){
        String strSimpleJson = simpleJson.toJSONString();
        com.couchbase.client.java.json.JsonObject cbJson = com.couchbase.client.java.json.JsonObject.fromJson(strSimpleJson);
        cbJson.put("type", dataSetName);
        records.add(cbJson);
    }

    @Override
    public JsonObject createJsonObject(Faker faker, int docsize, int id) {
        return records.get(id-1);
    }

    @Override
    public JsonObject updateJsonObject(JsonObject obj, List<String> fieldsToUpdate) {
        return obj;
    }

    private void readFile (String path, int numOps) {
        JSONParser jsonParser = new JSONParser();

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
                if (records.size() > numOps) {
                    return;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        int i = 0;
        while (records.size() < numOps) {
            records.add(records.get(i++));
        }
        System.out.println("Counter " + count);

    }

}
