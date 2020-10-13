package com.couchbase.javaclient.reactive;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.HttpClients;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class ElasticSync {

    public static final String filePrefix = "/tmp/es_bulk_";

    public static void sync(String elasticInstanceIP, String elasticPort, String elasticLogin, String elasticPassword, File file, int retryCount) {
        System.out.println("Started Elastic sync..");

        HttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost("http://" + elasticInstanceIP + ":" + elasticPort + "/_bulk");
        // auth header
        String token = elasticLogin + ":" + elasticPassword;
        httpPost.addHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8)));
        httpPost.setEntity(new FileEntity(file.getAbsoluteFile()));

        //Execute and get the response
        HttpResponse response;
        try {
            response = httpclient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode < 200 || statusCode > 299) {
                final String error = response.getStatusLine().getReasonPhrase();
                // TODO: add error to logs
                System.err.println("Elastic sync failed with code [" + statusCode + "] and error " + error);
                if (retryCount != 0) {
                    sync(elasticInstanceIP, elasticPort, elasticLogin, elasticPassword, file, retryCount - 1);
                } else {
                    System.exit(1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Completed Elastic sync..");
    }

    public static String createElasticObject(String dataset, String id, String operation) {
        return "{\"" + operation + "\": {" +
                "\"_index\": \"es_index\"," +
                "\"_type\": \"" + dataset + "\"," +
                "\"_id\": \"" + id + "\"" +
                "}}\n";
    }
}
