package com.couchbase.javaclient.doc;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.List;

public class Napa extends FileDataset implements DocTemplate {

    private List<JsonObject> records = new ArrayList<>();

    Napa(String path, int numOps) {
        readFile(path, numOps, records);
    }

    protected void parseAndStoreJsonObject(org.json.simple.JSONObject simpleJson){
        String strSimpleJson = simpleJson.toJSONString();
        com.couchbase.client.java.json.JsonObject cbJson = com.couchbase.client.java.json.JsonObject.fromJson(strSimpleJson);
        cbJson.put("type", "napa");
        records.add(cbJson);
    }

    @Override
    public JsonObject createJsonObject(Faker faker, int docsize, int id) {
        return records.get(id-1);
    }

    @Override
    public Object updateJsonObject(JsonObject obj, List<String> fieldsToUpdate) {
        return obj;
    }
}
