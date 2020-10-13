package com.couchbase.javaclient.reactive;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.javaclient.doc.*;
import com.couchbase.javaclient.utils.FileUtils;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;

public class DocCreate implements Callable<String> {
	private DocSpec ds;
	private Bucket bucket;
	private Collection collection;
	private static int num_docs = 0;
	private Map<String, String> elasticMap = new HashMap<>();

	public DocCreate(DocSpec _ds, Bucket _bucket) {
		ds = _ds;
		bucket = _bucket;
	}

	public DocCreate(DocSpec _ds, Collection _collection) {
		ds = _ds;
		collection = _collection;
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
		if(ds.get_shuffle_docs()){
			List<String> docs = docsToUpsert.collectList().block();
			java.util.Collections.shuffle(docs);
			docsToUpsert = Flux.fromIterable(docs);
		}
		DocTemplate docTemplate = DocTemplateFactory.getDocTemplate(ds);
		System.out.println("Started upsert..");

		try {
			docsToUpsert.publishOn(Schedulers.elastic())
					.flatMap(key -> rcollection.upsert(key, getObject(key, docTemplate, elasticMap),
							upsertOptions().expiry(Duration.ofSeconds(ds.get_expiry()))))
					.log()
					// Num retries, first backoff, max backoff
					.retryBackoff(10, Duration.ofMillis(1000), Duration.ofMillis(10000))
					// Block until last value, complete or timeout expiry
					.blockLast(Duration.ofMinutes(10));
		} catch (Exception err) {
			err.printStackTrace();
		}
		System.out.println("Completed upsert");
	}

	private JsonObject getObject(String key, DocTemplate docTemplate, Map<String, String> elasticMap) {
		JsonObject obj = docTemplate.createJsonObject(ds.faker, ds.get_size(), extractId(key));
		elasticMap.put(key, obj.toString());
		return obj;
	}

	@Override
	public String call() throws Error {
		if (collection != null) {
			upsertCollection(ds, collection);
		} else {
			upsertBucketCollections(ds, bucket);
		}
		if (ds.isElasticSync() && !elasticMap.isEmpty()) {
			File elasticFile = FileUtils.writeForElastic(elasticMap, ds.get_template(), "create");
			ElasticSync.sync(ds.getElasticIP(), ds.getElasticPort(), ds.getElasticLogin(), ds.getElasticPassword(), elasticFile, 5);
		}
		return num_docs + " DOCS CREATED!";
	}

	private int extractId(String key) {
		return Integer.parseInt(key.replace(ds.get_prefix(), "").replace(ds.get_suffix(), ""));
	}
}