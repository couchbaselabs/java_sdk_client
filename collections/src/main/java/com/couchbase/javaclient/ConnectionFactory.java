package com.couchbase.javaclient;

import java.time.Duration;

//import com.couchbase.client.core.env.Logger;
import java.util.logging.Level; 
import java.util.logging.Logger;
import java.util.logging.Handler;
import java.util.logging.ConsoleHandler;

import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

public class ConnectionFactory {

	ClusterEnvironment environment;
	Cluster cluster;
	Bucket bucket;
	Collection collection;

	public ConnectionFactory(String clusterName, String username, String password, String bucketName, String scopeName,
			String collectionName) {
		this.setCluster(connectCluster(clusterName, username, password));
		this.setBucket(connectBucket(cluster, bucketName));
		this.setCollection(connectCollection(bucket, scopeName, collectionName));
	}

	private Bucket connectBucket(Cluster cluster, String bucketName) {
		try {
			bucket = cluster.bucket(bucketName);
			bucket.waitUntilReady(Duration.ofSeconds(30));
		} catch (Exception ex) {
			this.handleException("Cannot connect to bucket " + bucketName + "\n" + ex);
		}
		return bucket;
	}

	private Cluster connectCluster(String clusterName, String username, String password) {
		Logger logger = Logger.getLogger("com.couchbase.client");
        logger.setLevel(Level.SEVERE);
        for(Handler h : logger.getParent().getHandlers()) {
            if(h instanceof ConsoleHandler){
                h.setLevel(Level.SEVERE);
            }
        }
		try {
			environment = ClusterEnvironment.builder()
					.loggerConfig(LoggerConfig.disableSlf4J(true))
					.build();
			environment.eventBus().subscribe(event -> {
				if (event.severity() == Event.Severity.ERROR) {
					System.out.println("Hit unrecoverable error..exiting \n" + event);
					System.exit(1);
				}
			});
			cluster = Cluster.connect(clusterName,
					ClusterOptions.clusterOptions(username, password).environment(environment));
		} catch (Exception ex) {
			this.handleException("Cannot connect to cluster" + clusterName + "\n" + ex);
		}
		return cluster;
	}

	private Collection connectCollection(Bucket bucket, String scopeName, String collectionName) {
		try {
			if (collectionName.equalsIgnoreCase("default")) {
				return bucket.defaultCollection();
			}
			if (scopeName != null) {
				return bucket.scope(scopeName).collection(collectionName);
			}
		} catch (Exception ex) {
			this.handleException(
					"Cannot connect to collection " + bucket + '.' + scopeName + '.' + collectionName + "\n" + ex);
		}
		return bucket.collection(collectionName);
	}

	public void close() {
		if (cluster != null) {
			cluster.disconnect();
		}
		if (environment != null) {
			environment.shutdown();
		}
	}

	public Bucket getBucket() {
		return bucket;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public Collection getCollection() {
		return collection;
	}

	public void setBucket(Bucket bucket) {
		this.bucket = bucket;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public void setCollection(Collection collection) {
		this.collection = collection;
	}

	public void handleException(String msg) {
		System.out.println(msg);
		this.close();
		System.exit(1);
	}
}
