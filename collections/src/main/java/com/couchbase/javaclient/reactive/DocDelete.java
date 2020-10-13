package com.couchbase.javaclient.reactive;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.javaclient.doc.DocSpec;

import com.couchbase.javaclient.utils.FileUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DocDelete implements Callable<String> {
	private DocSpec ds;
	private Bucket bucket;
	private Collection collection;
	private static int num_docs = 0;
	private boolean done = false;
	private Map<String, String> elasticMap = new HashMap<>();

	public DocDelete(DocSpec _ds, Bucket _bucket) {
		ds = _ds;
		bucket = _bucket;
	}

	public DocDelete(DocSpec _ds, Collection _collection) {
		ds = _ds;
		collection = _collection;
	}

	@Override
	public String call() throws Exception {
		if (collection != null) {
			deleteCollection(ds, collection);
		} else {
			deleteBucketCollections(ds, bucket);
		}
		// delete from elastic
		if (ds.isElasticSync() && !elasticMap.isEmpty()) {
			File elasticFile = FileUtils.writeForElastic(elasticMap, ds.get_template(), "delete");
			ElasticSync.sync(ds.getElasticIP(), ds.getElasticPort(), ds.getElasticLogin(), ds.getElasticPassword(), elasticFile, 5);
		}
		done = true;
		return num_docs + " DOCS UPDATED!";
	}

	public void deleteBucketCollections(DocSpec ds, Bucket bucket) {
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
		bucketCollections.parallelStream().forEach(c -> delete(ds, c));
	}

	public void deleteCollection(DocSpec ds, Collection collection) {
		delete(ds, collection);
	}

	public void delete(DocSpec ds, Collection collection) {
		ReactiveCollection rcollection = collection.reactive();
		num_docs = (int) (ds.get_num_ops() * ((float) ds.get_percent_delete() / 100));
		Flux<String> docsToDelete = Flux.range(ds.get_startSeqNum(), num_docs)
				.map(id -> ds.get_prefix() + id + ds.get_suffix());
		if(ds.get_shuffle_docs()){
			List<String> docs = docsToDelete.collectList().block();
			java.util.Collections.shuffle(docs);
			docsToDelete = Flux.fromIterable(docs);
		}
		System.out.println("Started delete..");
		try {
			docsToDelete.publishOn(Schedulers.elastic())
					// .delayElements(Duration.ofMillis(5))
					.flatMap(id -> wrap(rcollection, id, elasticMap))
					.log()
					// Num retries, first backoff, max backoff
					.retryBackoff(10, Duration.ofMillis(100), Duration.ofMillis(1000))
					// Block until last value, complete or timeout expiry
					.blockLast(Duration.ofMinutes(10));
		} catch (Exception err) {
			err.printStackTrace();
		}

		System.out.println("Completed delete");
	}

	private Mono<MutationResult> wrap(ReactiveCollection rcollection, String id, final Map<String, String> elasticMap) {
		elasticMap.put(id, null);
		return rcollection.remove(id);
	}
}