package com.couchbase.javaclient.queryutils;
import java.util.*;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.javaclient.ConnectionFactory;
import com.couchbase.javaclient.utils.FileUtils;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryResult;

public class QueryRunner implements Callable<String> {

    private final static Logger log = Logger.getLogger(QueryRunner.class);
    private static String query;
    private static Cluster cluster;

    public QueryRunner(String _query, Cluster _cluster) {
        query = _query;
        cluster = _cluster;
    }

    public void runQueryOnBucket() {
        List result_list = new ArrayList();
        try {
            final QueryResult result = cluster.query(query,
                    queryOptions().metrics(true));

            for (JsonObject row : result.rowsAsObject()) {
                result_list.add(row);
            }
            System.out.println(result_list);
            System.out.println("Total Number of results: " + result_list.size());
            System.out.println("Reported execution time: " + result.metaData().metrics().get().executionTime());
            log.info(result_list);
        } catch (CouchbaseException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public String call() throws Error {
            log.info("Run Query on Buckets");
            runQueryOnBucket();
        return null;
    }
}