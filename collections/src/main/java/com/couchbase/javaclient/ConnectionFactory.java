package com.couchbase.javaclient;

import java.time.Duration;

import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.env.TimeoutConfig;
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
			System.out.println("Cannot connect to bucket " + bucketName + "\n" + ex);
		}
		return bucket;
	}

	private Cluster connectCluster(String clusterName, String username, String password) {
		try {
			//ClusterEnvironment env = ClusterEnvironment.builder()
			//.ioConfig(IoConfig.captureTraffic(ServiceType.VIEWS)).build();
			environment = ClusterEnvironment.builder()
					.compressionConfig(CompressionConfig
					        .enable(true))
					.loggerConfig(LoggerConfig.fallbackToConsole(false).disableSlf4J(true))
					//.timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofSeconds(5)))
					.build();
//			cluster = Cluster.connect(clusterName,
//					ClusterOptions.clusterOptions(username, password).environment(environment));
			cluster = Cluster.connect(clusterName,
					ClusterOptions.clusterOptions(username, password).environment(environment));
		} catch (Exception ex) {
			System.out.println("Cannot connect to cluster " + clusterName + "\n" + ex);
		}
		return cluster;
	}

	private Collection connectCollection(Bucket bucket, String scopeName, String collectionName) {
		try {
			if(collectionName.equalsIgnoreCase("default")) {
				return bucket.defaultCollection();
			}
			if(scopeName != null) {
				return bucket.scope(scopeName).collection(collectionName);
			}			
		} catch (Exception ex) {
			System.out.println(
					"Cannot connect to collection " + bucket + '.' + scopeName + '.' + collectionName + "\n" + ex);
		}
		return bucket.collection(collectionName);
	}
	
	public void close() {
		cluster.disconnect();
		environment.shutdown();
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
}
