package com.couchbase.javaclient;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.javaclient.doc.DocSpec;
import com.couchbase.javaclient.doc.DocSpecBuilder;
import com.couchbase.javaclient.reactive.DocCreate;
import com.couchbase.javaclient.reactive.DocDelete;
import com.couchbase.javaclient.reactive.DocRetrieve;
import com.couchbase.javaclient.reactive.DocUpdate;

import com.couchbase.javaclient.utils.FileUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DocOperations {

	private final static Logger log = Logger.getLogger(DocOperations.class);

	public static void main(String[] args) {
		log.info("Input args: " + Arrays.asList(args));
		ArgumentParser parser = ArgumentParsers.newFor("Couchbase Java SDK Client For Collections").build()
				.defaultHelp(true).description("Standalone SDK Client");
		// Connection params
		parser.addArgument("-i", "--cluster").required(true).help("Couchbase cluster address");
		parser.addArgument("-u", "--username").setDefault("Administrator").help("Username of Couchbase user");
		parser.addArgument("-p", "--password").setDefault("password").help("Password of Couchbase user");
		parser.addArgument("-b", "--bucket").setDefault("default").help("Name of existing Couchbase bucket");
		parser.addArgument("-s", "--scope").setDefault("_default").help("Name of existing scope");
		parser.addArgument("-c", "--collection").setDefault("default").help("Name of existing collection");

		// Operation params
		parser.addArgument("-n", "--num_ops").type(Integer.class).setDefault(1000).help("Number of operations");
		parser.addArgument("-pc", "--percent_create").type(Integer.class).setDefault(100)
				.help("Percentage of creates out of num_ops");
		parser.addArgument("-pu", "--percent_update").type(Integer.class).setDefault(0)
				.help("Percentage of updates out of num_ops");
		parser.addArgument("-pd", "--percent_delete").type(Integer.class).setDefault(0)
				.help("Percentage of deletes out of num_ops");
		parser.addArgument("-pr", "--percent_read").type(Integer.class).setDefault(0)
				.help("Percentage of reads out of num_ops");
		parser.addArgument("-l", "--load_pattern").choices("uniform", "sparse", "dense").setDefault("uniform")
				.help("uniform, sparse, dense");
		parser.addArgument("-nt", "--num_threads").type(Integer.class).setDefault(4)
				.help("Max number of threads per operation type");
		parser.addArgument("-sd", "--shuffle_docs").type(Boolean.class).setDefault(Boolean.FALSE)
				.help("if true, shuffle docs, else operate sequentially");
		parser.addArgument("-ac", "--all_collections").type(Boolean.class).setDefault(Boolean.FALSE)
				.help("if true, all collections will be exercised");
		parser.addArgument("-lf", "--loop_forever").type(Boolean.class).setDefault(Boolean.FALSE)
				.help("if true, loop forever");
		parser.addArgument("-li", "--loop_interval").type(Integer.class).setDefault(30)
				.help("Sleep interval between loops in seconds");

		// Doc params
		parser.addArgument("-dsn", "--start_seq_num").type(Integer.class).setDefault(1)
				.help("Doc id start sequence number");
		parser.addArgument("-dpx", "--prefix").setDefault("doc_").help("Doc id prefix");
		parser.addArgument("-dsx", "--suffix").setDefault("").help("Doc id suffix");
		parser.addArgument("-dt", "--template").setDefault("Person").help("JSON document template");
		parser.addArgument("-de", "--expiry").type(Integer.class).setDefault(0).help("Document expiry in seconds");
		parser.addArgument("-ds", "--size").type(Integer.class).setDefault(500).help("Document size in bytes");
		parser.addArgument("-st", "--start").type(Integer.class).setDefault(0).help("Starting documents operations index");
		parser.addArgument("-en", "--end").type(Integer.class).setDefault(0).help("Ending documents operations index");
		parser.addArgument("-fu", "--fields_to_update").type(String.class).setDefault("")
				.help("Comma separated list of fields to update.");
		parser.addArgument("-ln", "--language").type(String.class).setDefault("en").help("Locale for wiki datased");
		parser.addArgument("-es", "--elastic_sync").type(Boolean.class).setDefault(Boolean.FALSE)
				.help("If true, then syncronize cb data with elastic bucket");
		parser.addArgument("-es_host", "--elastic_host").type(String.class).setDefault("").help("Elastic instance IP");
		parser.addArgument("-es_port", "--elastic_port").type(String.class).setDefault("")
				.help("Elastic instance port");
		parser.addArgument("-es_login", "--elastic_login").type(String.class).setDefault("")
				.help("Elastic instance user login");
		parser.addArgument("-es_password", "--elastic_password").type(String.class).setDefault("")
				.help("Elastic instance password");
		// DEBUG < INFO < WARN < ERROR < FATAL < OFF
		parser.addArgument("-log_level", "--log_level").type(String.class)
				.choices("DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF").setDefault("INFO")
				.help("Log level. Levels can be: DEBUG < INFO < WARN < ERROR < FATAL < OFF");
		parser.addArgument("-output", "--output").type(Boolean.class).setDefault(Boolean.FALSE)
				.help("Output to console of upsert results");

		try {
			Namespace ns = parser.parseArgs(args);
			run(ns);
		} catch (ArgumentParserException e) {
			log.error(e);
			parser.handleError(e);
		}
	}

	private static void run(Namespace ns) {
		String logLevel = ns.getString("log_level");
		Logger.getRootLogger().setLevel(Level.toLevel(logLevel, Level.INFO));
		
		String clusterName = ns.getString("cluster");
		String username = ns.getString("username");
		String password = ns.getString("password");
		String bucketName = ns.getString("bucket");
		String scopeName = ns.getString("scope");
		String collectionName = ns.getString("collection");

		String fieldsToUpdateStr = ns.getString("fields_to_update");
		String lang = ns.getString("language");
		String docTemplate = ns.getString("template");
		String preparedDataFile = FileUtils.getDataFilePrepared(docTemplate, lang);
		List<String> fieldsToUpdate = Arrays.asList(fieldsToUpdateStr.split(","));

		ConnectionFactory connection = new ConnectionFactory(clusterName, username, password, bucketName, scopeName,
				collectionName, Level.toLevel(logLevel, Level.INFO));
		Bucket bucket = connection.getBucket();
		Collection collection = connection.getCollection();

		DocSpec dSpec = new DocSpecBuilder().numOps(ns.getInt("num_ops")).percentCreate(ns.getInt("percent_create"))
				.percentUpdate(ns.getInt("percent_update")).percentDelete(ns.getInt("percent_delete"))
				.startSeqNum(ns.getInt("start_seq_num")).prefix(ns.getString("prefix")).suffix(ns.getString("suffix")).template(ns.getString("template"))
				.expiry(ns.getInt("expiry")).size(ns.getInt("size")).start(ns.getInt("start")).end(ns.getInt("end"))
				.dataFile(preparedDataFile).shuffleDocs(ns.getBoolean("shuffle_docs"))
				.setElasticSync(ns.getBoolean("elastic_sync")).setElasticIP(ns.getString("elastic_host"))
				.setElasticPort(ns.getString("elastic_port")).setElasticLogin(ns.getString("elastic_login"))
				.setElasticPassword(ns.getString("elastic_password")).setOutput(ns.getBoolean("output"), logLevel)
				.fieldsToUpdate(fieldsToUpdate).buildDocSpec();

		if (ns.getBoolean("loop_forever")) {
			log.info("Loop forever");
			while (true) {
				try {
					spawnTasks(dSpec, ns.getBoolean("all_collections"), bucket, collection, ns.getInt("num_threads"));
					TimeUnit.SECONDS.sleep(ns.getInt("loop_interval"));
				} catch (Exception e) {
					log.error(e);
				}
			}
		} else {
			try {
				spawnTasks(dSpec, ns.getBoolean("all_collections"), bucket, collection, ns.getInt("num_threads"));
			} catch (Exception e) {
				log.error(e);
				connection.close();
				System.exit(1);
			}
		}
		connection.close();
		System.exit(0);
	}

	private static void spawnTasks(DocSpec dSpec, Boolean all_collections, 
			Bucket bucket, Collection collection, int nThreads) {
		ForkJoinTask<String> create = null;
		ForkJoinTask<String> update = null;
		ForkJoinTask<String> delete = null;
		ForkJoinTask<String> retrieve = null;
		ForkJoinPool pool = new ForkJoinPool();

		if (all_collections) {
			create = ForkJoinTask.adapt(new DocCreate(dSpec, bucket, nThreads));
			update = ForkJoinTask.adapt(new DocUpdate(dSpec, bucket, nThreads));
			delete = ForkJoinTask.adapt(new DocDelete(dSpec, bucket, nThreads));
			retrieve = ForkJoinTask.adapt(new DocRetrieve(dSpec, bucket, nThreads));
		} else {
			create = ForkJoinTask.adapt(new DocCreate(dSpec, collection, nThreads));
			update = ForkJoinTask.adapt(new DocUpdate(dSpec, collection, nThreads));
			delete = ForkJoinTask.adapt(new DocDelete(dSpec, collection, nThreads));
			retrieve = ForkJoinTask.adapt(new DocRetrieve(dSpec, collection, nThreads));
		}
		bucket.waitUntilReady(Duration.ofSeconds(30));
		if (dSpec.get_percent_create() > 0) {
			log.info("Invoke create");
			pool.invoke(create);
		}
		if (dSpec.get_percent_update() > 0) {
			log.info("Invoke update");
			pool.invoke(update);
		}
		if (dSpec.get_percent_delete() > 0) {
			log.info("Invoke delete");
			pool.invoke(delete);
		}
		pool.invoke(retrieve);
		pool.shutdownNow();
	}

}
