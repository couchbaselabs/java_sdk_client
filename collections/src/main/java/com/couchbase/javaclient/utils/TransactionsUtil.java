package com.couchbase.javaclient.utils;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfigBuilder;

import java.time.Duration;

public class TransactionsUtil {
    public final static String CONTENT_NAME= "content";
    public static JsonObject initial = JsonObject.create().put(CONTENT_NAME, "initial");
    public static final JsonObject updated = JsonObject.create().put(CONTENT_NAME, "updated");

    public static Transactions getDefaultTransactionsFactory(Cluster cluster){
        TransactionConfigBuilder builder = TransactionConfigBuilder.create()
                .durabilityLevel(TransactionDurabilityLevel.MAJORITY)
                .expirationTime(Duration.ofSeconds(36000))
                .cleanupWindow(Duration.ofSeconds(360));

        Transactions transactions = Transactions.create(cluster, builder);

        return transactions;
    }
}
