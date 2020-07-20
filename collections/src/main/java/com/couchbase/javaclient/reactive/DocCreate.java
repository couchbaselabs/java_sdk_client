package com.couchbase.javaclient.reactive;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.javaclient.doc.*;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;

public class DocCreate implements Callable<String> {
	private DocSpec ds;
	private Bucket bucket;
	private String dataFile = null;
	private Collection collection;
	private static int num_docs = 0;

	public DocCreate(DocSpec _ds, Bucket _bucket, String dataFile) {
		ds = _ds;
		bucket = _bucket;
		this.dataFile = dataFile;
	}

	public DocCreate(DocSpec _ds, Collection _collection, String dataFile) {
		ds = _ds;
		collection = _collection;
		this.dataFile = dataFile;
	}

	public void upsertBucketCollections(DocSpec _ds, Bucket _bucket) {
		ds = _ds;
		bucket = _bucket;
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
		bucketCollections.parallelStream().forEach(c -> upsert(ds, c));
	}

	public void upsertCollection(DocSpec _ds, Collection _collection) {
		ds = _ds;
		collection = _collection;
		upsert(ds, collection);
	}

	public void upsert(DocSpec ds, Collection collection) {
		ReactiveCollection rcollection = collection.reactive();
		num_docs = (int) (ds.get_num_ops() * ((float) ds.get_percent_create() / 100));
		Flux<String> docsToUpsert = Flux.range(ds.get_startSeqNum(), num_docs)
				.map(id -> (ds.get_prefix() + id + ds.get_suffix()));
		DocTemplate docTemplate = DocTemplateFactory.getDocTemplate(ds.get_template(), this.dataFile, ds.get_num_ops());
		System.out.println("Started upsert..");
		AtomicInteger id = new AtomicInteger(0);
		try {
			docsToUpsert.publishOn(Schedulers.elastic())
					.flatMap(key -> rcollection.upsert(key, docTemplate.createJsonObject(ds.faker, ds.get_size(), id.getAndIncrement()),
							upsertOptions().expiry(Duration.ofSeconds(ds.get_expiry()))))
					// Num retries, first backoff, max backoff
					.retryBackoff(10, Duration.ofMillis(1000), Duration.ofMillis(10000))
					// Block until last value, complete or timeout expiry
					.blockLast(Duration.ofMinutes(10));
		} catch (Exception err) {
			err.printStackTrace();
		}
		System.out.println("Completed upsert");
	}

	@Override
	public String call() throws Exception {
		if (ds.get_percent_create() == 0) {
			return "";
		}
		if (collection != null) {
			upsertCollection(ds, collection);
		} else {
			upsertBucketCollections(ds, bucket);
		}
		return num_docs + " DOCS CREATED!";
	}
}