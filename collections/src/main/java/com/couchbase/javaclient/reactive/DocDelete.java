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
import org.apache.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class DocDelete implements Callable<String> {

	private final static Logger log = Logger.getLogger(DocDelete.class);

	private static DocSpec ds;
	private static Bucket bucket;
	private static Collection collection;
	private static int nThreads; 
	private static int num_docs = 0;
	private boolean done = false;
	private Map<String, String> elasticMap = new HashMap<>();

	public DocDelete(DocSpec _ds, Bucket _bucket, int _nThreads) {
		ds = _ds;
		bucket = _bucket;
		nThreads = _nThreads;
	}

	public DocDelete(DocSpec _ds, Collection _collection, int _nThreads) {
		ds = _ds;
		collection = _collection;
		nThreads = _nThreads;
	}

	@Override
	public String call() throws Exception {
		if (collection != null) {
			log.info("Delete collection " + collection.bucketName() + "." + collection.scopeName() + "." + collection.name());
			deleteCollection(collection);
		} else {
			log.info("Delete bucket collections");
			deleteBucketCollections();
		}
		// delete from elastic
		if (ds.isElasticSync() && !elasticMap.isEmpty()) {
			List<File> elasticFiles = FileUtils.writeForElastic(elasticMap, ds.get_template(), "delete");
			ElasticSync.syncFiles(ds.getElasticIP(), ds.getElasticPort(), ds.getElasticLogin(), ds.getElasticPassword(), elasticFiles, 5);
		}
		done = true;
		return num_docs + " DOCS UPDATED!";
	}

	public void deleteBucketCollections() {
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
		bucketCollections.parallelStream().forEach(c -> delete(c));
	}

	public void deleteCollection(Collection collection) {
		delete(collection);
	}

	public void delete(Collection collection) {
		ReactiveCollection rcollection = collection.reactive();
		num_docs = (int) (ds.get_num_ops() * ((float) ds.get_percent_delete() / 100));
		Flux<String> docsToDelete = Flux.range(ds.get_startSeqNum(), num_docs)
				.map(id -> ds.get_prefix() + id + ds.get_suffix());
		if(ds.get_shuffle_docs()){
			List<String> docs = docsToDelete.collectList().block();
			java.util.Collections.shuffle(docs);
			docsToDelete = Flux.fromIterable(docs);
		}
		try {
			docsToDelete.publishOn(Schedulers
					.newBoundedElastic(nThreads, 100, "catapult-delete"))
					// .delayElements(Duration.ofMillis(5))
					.flatMap(id -> wrap(rcollection, id, elasticMap))
					// .log()
					// Num retries
					.retry(20)
					// Block until last value, complete or timeout expiry
					.blockLast(Duration.ofMinutes(10));
		} catch (Exception err) {
			log.error(err);
		}

		log.info("Completed delete");
	}

	private Mono<MutationResult> wrap(ReactiveCollection rcollection, String id, final Map<String, String> elasticMap) {
		elasticMap.put(id, null);
		return rcollection.remove(id);
	}
}