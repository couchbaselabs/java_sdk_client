package com.couchbase.javaclient.doc;

import java.util.List;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

public interface DocTemplate {

	JsonObject createJsonObject(Faker faker, int docsize, int id);
	JsonObject updateJsonObject(JsonObject obj, List<String> fieldsToUpdate);

}