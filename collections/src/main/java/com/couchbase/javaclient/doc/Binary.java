package com.couchbase.javaclient.doc;

import com.couchbase.client.java.json.JsonObject;
import com.github.javafaker.Faker;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class Binary{
    public static byte[] createBinaryObject(Faker faker, int docsize){
    	return (faker.lorem().characters(100, docsize)).getBytes(StandardCharsets.UTF_8);
    }

}
