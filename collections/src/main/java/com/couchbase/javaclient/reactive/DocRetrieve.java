package com.couchbase.javaclient.reactive;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.javaclient.doc.DocSpec;
import com.couchbase.javaclient.doc.Person;

import org.apache.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class DocRetrieve implements Callable<String> {

	private final static Logger log = Logger.getLogger(DocRetrieve.class);

	private DocSpec ds;
	private Bucket bucket;
	private Collection collection;
	private static int num_docs = 0;
	private boolean done = false;

	public DocRetrieve(DocSpec _ds, Bucket _bucket) {
		ds = _ds;
		bucket = _bucket;
	}

	public DocRetrieve(DocSpec _ds, Collection _collection) {
		ds = _ds;
		collection = _collection;
	}

	@Override
	public String call() throws Exception {
		if (collection != null) {
			log.info("Retrieve collection " + collection.bucketName() + "." + collection.scopeName() + "."
					+ collection.name());
			printCollection(ds, collection);
		} else {
			log.info("Retrieve bucket collections");
			printBucketCollections(ds, bucket);
		}
		done = true;
		return num_docs + " DOCS PRESENT!";
	}

	public static void printBucketCollections(DocSpec ds, Bucket bucket) {
		List<Collection> bucketCollections = new ArrayList<>();
		List<ScopeSpec> bucketScopes = bucket.collections().getAllScopes();
		for (ScopeSpec scope : bucketScopes) {
			for (CollectionSpec scopeCollection : scope.collections()) {
				Collection collection = bucket.scope(scope.name()).collection(scopeCollection.name());
				if (collection != null) {
					bucketCollections.add(collection);
				}
			}
		}
		log.info("Collections in " + bucket + ' ' + bucketCollections);
		bucketCollections.parallelStream().forEach(c -> printCollection(ds, c));
	}

	public static void printCollection(DocSpec ds, Collection collection) {
		ReactiveCollection rcollection = collection.reactive();
		int created_docs = (int) (ds.get_num_ops() * ((float) ds.get_percent_create() / 100));
		int deleted_docs = (int) (ds.get_num_ops() * ((float) ds.get_percent_delete() / 100));
		int expected_docs = created_docs - deleted_docs;

		log.info("deleted docs " + deleted_docs);
		log.info("expected docs " + expected_docs);

		List<String> docsToDeleteList = new ArrayList<>();
		String key = null;
		try {
			for (int id = ds.get_startSeqNum(); id < ds.get_startSeqNum() + num_docs; id++) {
				key = ds.get_prefix() + id + ds.get_suffix();
				GetResult found = collection.get(key);
				docsToDeleteList.add(key);
			}
		} catch (Exception e) {
			log.error(key + " not found. Skipping delete");
		}
		Flux<String> docsToDelete = Flux.fromIterable(docsToDeleteList);
		List<String> docsToFetchList = new ArrayList<>();

		for (int id = ds.get_startSeqNum() + deleted_docs; id <= created_docs; id++) {
			key = ds.get_prefix() + id + ds.get_suffix();
			docsToFetchList.add(key);
		}
		if (ds.get_shuffle_docs()) {
			java.util.Collections.shuffle(docsToFetchList);
		}

		List<GetResult> actual_docs = Flux.fromIterable(docsToFetchList).flatMap(id -> rcollection.get(id).retry(20))
				.buffer(1000)
				// Block until last value, complete or timeout expiry
				.blockLast(Duration.ofMinutes(10));

		log.info(expected_docs + " keys expected, " + actual_docs.size() + " keys present in collection");
	}

	public boolean isDone() {
		return done;
	}
}