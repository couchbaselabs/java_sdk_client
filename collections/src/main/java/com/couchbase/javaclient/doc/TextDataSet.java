package com.couchbase.javaclient.doc;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TextDataSet implements DocTemplate {

    private final static Logger log = Logger.getLogger(TextDataSet.class);
    private List<JsonObject> records = new ArrayList<>();
    private String dataSetName;

    TextDataSet(DocSpec docSpec) {
        this.dataSetName = docSpec.get_template();
        readFile(docSpec.getDataFile(), docSpec.get_num_ops()+docSpec.get_startSeqNum());
    }

    protected void parseAndStoreJsonObject(org.json.simple.JSONObject simpleJson){
        String strSimpleJson = simpleJson.toJSONString();
        com.couchbase.client.java.json.JsonObject cbJson = com.couchbase.client.java.json.JsonObject.fromJson(strSimpleJson);
        cbJson.put("type", dataSetName);
        records.add(cbJson);
    }

    @Override
    public JsonObject createJsonObject(Faker faker, int docsize, int id) {
        // DocCreate generates keys with Flux.range(startSeqNum, num_docs), so id
        // is the absolute sequence number and starts at startSeqNum - 0 for a
        // default load. readFile() sizes records to num_ops + startSeqNum exactly
        // so that id indexes it directly; subtracting one asked for
        // records.get(-1) on the very first document of every run, and returned
        // the wrong record for every document after it.
        if (id < 0 || id >= records.size()) {
            throw new IllegalStateException("No record for doc id " + id
                    + ": dataset '" + dataSetName + "' holds " + records.size()
                    + " records. Check the data file has at least "
                    + "num_ops + startSeqNum parseable lines.");
        }
        return records.get(id);
    }

    @Override
    public JsonObject updateJsonObject(Faker faker, JsonObject obj, List<String> fieldsToUpdate) {
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
            log.error(e);
        }
        if (records.isEmpty()) {
            // Nothing parsed - the padding loop below would itself throw on
            // records.get(0). Say why instead.
            log.error("No records parsed from " + path + " (" + count
                    + " unparseable lines); cannot pad to " + numOps);
            return;
        }
        int i = 0;
        while (records.size() < numOps) {
            records.add(records.get(i++));
        }
        log.info("Counter " + count);
    }

}
