package com.couchbase.javaclient.reactive;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.javaclient.doc.*;
import com.couchbase.javaclient.utils.FileUtils;

import org.apache.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class DocCreate implements Callable<String> {

	private final static Logger log = Logger.getLogger(DocCreate.class);
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
		List<MutationResult> results;
		try {
			if ("Binary".equals(ds.get_template())) {
				results = docsToUpsert.publishOn(Schedulers.elastic())
						.flatMap(key -> rcollection.upsert(key, new Binary().createBinaryObject(ds.faker, ds.get_size()),
								upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE)
								.expiry(Duration.ofSeconds(ds.get_expiry()))))
						.buffer(1000)
						.blockLast(Duration.ofSeconds(num_docs));
			}else {
				DocTemplate docTemplate = DocTemplateFactory.getDocTemplate(ds);
				results = docsToUpsert.publishOn(Schedulers.elastic())
						.flatMap(key -> rcollection.upsert(key, getObject(key, docTemplate, elasticMap),
								upsertOptions().expiry(Duration.ofSeconds(ds.get_expiry()))))
						.buffer(1000)
						.blockLast(Duration.ofSeconds(num_docs));
			}
			// print results
			if (ds.isOutput()) {
				System.out.println("Insert results");
				FileUtils.printMutationResults(results, log);
			}
		} catch (Throwable err) {
			log.error(err);
		}
		log.info("Completed upsert");
	}

	private JsonObject getObject(String key, DocTemplate docTemplate, Map<String, String> elasticMap) {
		JsonObject obj = docTemplate.createJsonObject(ds.faker, ds.get_size(), extractId(key));
		elasticMap.put(key, obj.toString());
		return obj;
			
	}

	@Override
	public String call() throws Error {
		if (collection != null) {
			log.info("Upsert collection " + collection.bucketName() + "." + collection.scopeName() + "." + collection.name());
			upsertCollection(ds, collection);
		} else {
			log.info("Upsert bucket collections");
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