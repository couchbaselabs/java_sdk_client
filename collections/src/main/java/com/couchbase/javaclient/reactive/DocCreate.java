package com.couchbase.javaclient.reactive;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;

import com.couchbase.javaclient.doc.*;
import com.couchbase.javaclient.utils.FileUtils;

import com.couchbase.javaclient.utils.TransactionsUtil;
import com.couchbase.transactions.Transactions;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;
import java.util.logging.Level;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;

public class DocCreate implements Callable<String> {

	private final static Logger log = Loggers.getLogger(DocCreate.class);
	private static DocSpec ds;
	private static Cluster cluster;
	private static Bucket bucket;
	private static Collection collection;
	private static int nThreads; 
	private static int num_docs = 0;
	private Map<String, String> elasticMap = new HashMap<>();

	public DocCreate(DocSpec _ds, Cluster _cluster, Bucket _bucket, int _nThreads) {
		cluster= _cluster;
		ds = _ds;
		bucket = _bucket;
		nThreads = _nThreads;
	}

	public DocCreate(DocSpec _ds, Cluster _cluster, Collection _collection, int _nThreads) {
		cluster= _cluster;
		ds = _ds;
		collection = _collection;
		nThreads = _nThreads;
	}

	public void upsertBucketCollections() {
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
		bucketCollections.parallelStream().forEach(c -> upsert(c));
	}

	public void upsertCollection(Collection collection) {
		upsert(collection);
	}

	public void upsert(Collection collection) {
		ReactiveCollection rcollection = collection.reactive();
		num_docs = (int) (ds.get_num_ops() * ((float) ds.get_percent_create() / 100));
		Flux<String> docsToUpsert = Flux.range(ds.get_startSeqNum(), num_docs)
				.map(id -> (ds.get_prefix() + id + ds.get_suffix()));
		if(ds.get_shuffle_docs()){
			List<String> docs = docsToUpsert.collectList().block();
			java.util.Collections.shuffle(docs);
			docsToUpsert = Flux.fromIterable(docs);
		}
		
		try {
			if(ds.getUseTransactions()){
				log.info("Using Transactions for DocCreate");
				List<String> insertDocs = docsToUpsert.collectList().block();
				insertDocs.forEach(key -> {
					log.info("Checking if doc exists previosuly and if so, Removing doc: "+key);
					if(collection.exists(key).exists()){
						collection.remove(key);
					}
				});

				Transactions transactions =  TransactionsUtil.getDefaultTransactionsFactory(cluster);
				transactions.run(ctx->{
					insertDocs.forEach(key -> {
						ctx.insert(collection,key,TransactionsUtil.initial);
					});
				});
				transactions.close();
			}
			else if ("Binary".equals(ds.get_template())) {
					docsToUpsert.publishOn(Schedulers
							// Num threads, items in queue, thread name prefix
							.newBoundedElastic(nThreads, 100, "catapult-create"))
							.flatMap(
									key -> rcollection.upsert(key, new Binary().createBinaryObject(ds.faker, ds.get_size()),
											upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE)
													.expiry(Duration.ofSeconds(ds.get_expiry()))))
							.log("", ds.getNewLogLevel())
							.buffer(1000)
							.retry(20)
							.blockLast(Duration.ofSeconds(1000));
				} else {
					DocTemplate docTemplate = DocTemplateFactory.getDocTemplate(ds);
					docsToUpsert.publishOn(Schedulers
							.newBoundedElastic(nThreads, 100, "catapult-create"))
							.flatMap(key -> rcollection.upsert(key, getObject(key, docTemplate, elasticMap),
									upsertOptions().expiry(Duration.ofSeconds(ds.get_expiry()))))
							.log("", ds.getNewLogLevel())
							.buffer(1000)
							// Num retries
							.retry(20)
							// Block until last value, complete or timeout expiry
							.blockLast(Duration.ofSeconds(1000));
				}
		} catch (Throwable err) {
			err.printStackTrace();
			System.out.println("Error "+err.toString());
			log.error(err.toString());
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
			log.info("Upsert collection " + collection.bucketName() + "." + collection.scopeName() + "."
					+ collection.name());
			upsertCollection(collection);
		} else {
			log.info("Upsert bucket collections");
			upsertBucketCollections();
		}
		if (ds.isElasticSync() && !elasticMap.isEmpty()) {
			List<File> elasticFiles = FileUtils.writeForElastic(elasticMap, ds.get_template(), "create");
			ElasticSync.syncFiles(ds.getElasticIP(), ds.getElasticPort(), ds.getElasticLogin(),
					ds.getElasticPassword(), elasticFiles, 5);
		}
		return num_docs + " DOCS CREATED!";
	}

	private int extractId(String key) {
		return Integer.parseInt(key.replace(ds.get_prefix(), "").replace(ds.get_suffix(), ""));
	}

	private Mono<MutationResult> wrap(ReactiveCollection rcollection, String id, final Map<String, String> elasticMap) {
		elasticMap.put(id, null);
		return rcollection.remove(id);
	}
}