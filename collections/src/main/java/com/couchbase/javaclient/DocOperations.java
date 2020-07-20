package com.couchbase.javaclient;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

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


public class DocOperations {
	public static void main(String[] args) {
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
				.help("uniform: load all collections with percent_create docs, "
						+ "sparse: load all collections with maximum of percent_create docs"
						+ "dense: load all collections with minimum of percent_create docs");

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
		parser.addArgument("-fu", "--fields_to_update").type(String.class).setDefault("").help("Comma separated list of fields to update.");
		parser.addArgument("-ac", "--all_collections").type(Boolean.class).setDefault(Boolean.FALSE).help("True: if all collections are to be exercised");
		parser.addArgument("-ln", "--language").type(String.class).setDefault("en").help("Locale for wiki datased");

		try {
			Namespace ns = parser.parseArgs(args);
			run(ns);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
		}
	}


	private static void run(Namespace ns) {
		String clusterName = ns.getString("cluster");
		String username = ns.getString("username");
		String password = ns.getString("password");
		String bucketName = ns.getString("bucket");
		String scopeName = ns.getString("scope");
		String collectionName = ns.getString("collection");
		String fieldsToUpdateStr = ns.getString("fields_to_update");

		String lang = ns.getString("language");
		final String NAPA_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/napa_dataset.txt.gz";
		final String DEWIKI_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/dewiki.txt.gz";
		final String ENWIKI_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/enwiki.txt.gz";
		final String ESWIKI_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/eswiki.txt.gz";
		final String FRWIKI_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/frwiki.txt.gz";

		final String NAPA_FILE = "napa_dataset.txt";
		final String DEWIKI_FILE = "dewiki.txt";
		final String ENWIKI_FILE = "enwiki.txt";
		final String ESWIKI_FILE = "eswiki.txt";
		final String FRWIKI_FILE = "frwiki.txt";

		String localFileName = null;

		String localFile = null;

		String docTemplate = ns.getString("template");
		if(docTemplate.equals("napa") || docTemplate.equals("wiki")){
			String localArchiveFile = "";
			String remotePath = "";
			if(docTemplate.equals("napa")){
				remotePath = NAPA_URL;
				localFile = NAPA_FILE;
			}else{
				if (lang.equals("de")){
					remotePath = DEWIKI_URL;
					localFile = DEWIKI_FILE;
				}else if(lang.equals("en")){
					remotePath = ENWIKI_URL;
					localFile = ENWIKI_FILE;
				}else if(lang.equals("es")){
					remotePath = ESWIKI_URL;
					localFile = ESWIKI_FILE;
				}else if(lang.equals("fr")){
					remotePath = FRWIKI_URL;
					localFile = FRWIKI_FILE;
				}
				localArchiveFile = localFile + ".gz";
			}
			localFileName = FileUtils.loadDataFile(localArchiveFile, remotePath, localFile, docTemplate);
		}

		List<String> fieldsToUpdate = Arrays.asList(fieldsToUpdateStr.split(","));

		ConnectionFactory connection = new ConnectionFactory(clusterName, username, password, bucketName, scopeName,
				collectionName);
		Bucket bucket = connection.getBucket();
		Collection collection = null;
		collection = connection.getCollection();

		DocSpec dSpec = new DocSpecBuilder().numOps(ns.getInt("num_ops")).percentCreate(ns.getInt("percent_create"))
				.percentUpdate(ns.getInt("percent_update")).percentDelete(ns.getInt("percent_delete"))
				.loadPattern(ns.getString("load_pattern")).startSeqNum(ns.getInt("start_seq_num"))
				.prefix(ns.getString("prefix")).suffix(ns.getString("suffix")).template(ns.getString("template"))
				.expiry(ns.getInt("expiry")).size(ns.getInt("size")).start(ns.getInt("start")).end(ns.getInt("end")) .buildDocSpec();


		ForkJoinTask<String> create = null;
		ForkJoinTask<String> update = null;
		ForkJoinTask<String> delete = null;
		ForkJoinTask<String> retrieve = null;
		ForkJoinPool pool = new ForkJoinPool();

		if(ns.getBoolean("all_collections")) {
			create = ForkJoinTask.adapt(new DocCreate(dSpec, bucket, localFileName));
			update = ForkJoinTask.adapt(new DocUpdate(dSpec, bucket, fieldsToUpdate));
			delete = ForkJoinTask.adapt(new DocDelete(dSpec, bucket));
			retrieve = ForkJoinTask.adapt(new DocRetrieve(dSpec, bucket));
		} else {
			create = ForkJoinTask.adapt(new DocCreate(dSpec, collection, localFileName));
			update = ForkJoinTask.adapt(new DocUpdate(dSpec, collection, fieldsToUpdate));
			delete = ForkJoinTask.adapt(new DocDelete(dSpec, collection));
			retrieve = ForkJoinTask.adapt(new DocRetrieve(dSpec, collection));
		}
		try {
			pool.invoke(create);
			pool.invoke(update);
			pool.invoke(delete);
			pool.invoke(retrieve);
			pool.shutdownNow();
		} catch (Exception e) {
			e.printStackTrace();
			connection.close();
			System.exit(1);
		}
		connection.close();
		System.exit(0);
	}

}
