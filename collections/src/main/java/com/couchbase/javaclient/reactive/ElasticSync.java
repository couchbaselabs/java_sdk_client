package com.couchbase.javaclient.reactive;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public final class ElasticSync {

    private final static Logger log = Logger.getLogger(ElasticSync.class);
    public static final String filePrefix = "/tmp/es_bulk_";

    public static void syncFiles(String elasticInstanceIP, String elasticPort, String elasticLogin, String elasticPassword, List<File> files, int retryCount) {
        log.info("Started Elastic sync... Need to sync [" + files.size() + "] files.");
        int i = 1;
        for (File file: files) {
            log.info("Starts sync file number: [" + i + "]");
            sync(elasticInstanceIP, elasticPort, elasticLogin, elasticPassword, file, retryCount);
            i++;
        }
        log.info("Completed Elastic sync..");
    }

    private static void sync(String elasticInstanceIP, String elasticPort, String elasticLogin, String elasticPassword, File file, int retryCount) {
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
                log.error("Elastic sync failed with code [" + statusCode + "] and error " + error);
                if (retryCount != 0) {
                    sync(elasticInstanceIP, elasticPort, elasticLogin, elasticPassword, file, retryCount - 1);
                } else {
                    System.exit(1);
                }
            }
        } catch (IOException e) {
            log.error(e);
            log.info("Elastic sync failed..");
            System.exit(1);
        }
    }

    public static String createElasticObject(String dataset, String id, String operation) {
        return "{\"" + operation + "\": {" +
                "\"_index\": \"es_index\"," +
                "\"_type\": \"" + dataset + "\"," +
                "\"_id\": \"" + id + "\"" +
                "}}\n";
    }
}
