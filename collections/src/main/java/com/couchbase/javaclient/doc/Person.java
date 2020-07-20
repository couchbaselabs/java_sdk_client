package com.couchbase.javaclient.doc;

import java.util.List;
import java.util.Random;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;

public class Person implements DocTemplate{
	JsonObject jsonObject = JsonObject.create();
	Random random = new Random();

	public JsonObject createJsonObject(Faker faker, int docsize, int id) {
		jsonObject.put("firstName", faker.name().firstName());
		jsonObject.put("lastName", faker.name().lastName());
		jsonObject.put("title", faker.name().title());
		jsonObject.put("suffix", faker.name().suffix());
		jsonObject.put("streetAddress", faker.address().streetAddress());
		jsonObject.put("city", faker.address().city());
		jsonObject.put("country", faker.address().country());
		jsonObject.put("age", random.nextInt(70));
		int count = 0;
		do {
			count = count + 1;
			jsonObject.put("filler" + count, faker.lorem().words(docsize / 10));
		} while (jsonObject.toString().length() < docsize);
		return jsonObject;
	}

	@Override
	public Object updateJsonObject(JsonObject obj, List<String> fieldsToUpdate) {
		return obj;
	}

}
