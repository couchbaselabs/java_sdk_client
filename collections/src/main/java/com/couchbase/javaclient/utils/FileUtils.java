package com.couchbase.javaclient.utils;

import com.couchbase.javaclient.DataTransformer;
import com.couchbase.javaclient.reactive.ElasticSync;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.couchbase.javaclient.reactive.ElasticSync.createElasticObject;

public final class FileUtils {

    final static String NAPA_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/napa_dataset.txt.gz";
    final static String DEWIKI_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/dewiki.txt.gz";
    final static String ENWIKI_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/enwiki.txt.gz";
    final static String ESWIKI_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/eswiki.txt.gz";
    final static String FRWIKI_URL = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/frwiki.txt.gz";

    final static String NAPA_FILE = "napa_dataset.txt";
    final static String DEWIKI_FILE = "dewiki.txt";
    final static String ENWIKI_FILE = "enwiki.txt";
    final static String ESWIKI_FILE = "eswiki.txt";
    final static String FRWIKI_FILE = "frwiki.txt";

    public static String loadDataFile(String localPath, String remotePath, String localFile, String dataset){
        try {
            File f = new File(localPath);
            if (!f.exists()) {
                URL url = new URL(remotePath);
                ReadableByteChannel readChannel = Channels.newChannel(url.openStream());
                FileOutputStream fos = new FileOutputStream(localPath);
                FileChannel writeChannel = fos.getChannel();
                writeChannel.transferFrom(readChannel, 0, Long.MAX_VALUE);
            }
            unGunzipFile(localPath, localFile);
            return DataTransformer.pyJsonToJavaJson(localFile, "transformed-"+localFile, dataset);
        }catch(IOException ioe){
            System.out.println(ioe.getMessage());
        }
        return null;
    }

    public static void unGunzipFile(String compressedFile, String decompressedFile) {
        byte[] buffer = new byte[1024];
        try {
            FileInputStream fileIn = new FileInputStream(compressedFile);
            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);
            FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);
            int bytesRead;
            while ((bytesRead = gZIPInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
            gZIPInputStream.close();
            fileOutputStream.close();
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    public static String getDataFilePrepared(String docTemplate, String lang){
        String localFileName = null;
        String localFile = null;

        if(docTemplate.equals("napa") || docTemplate.equals("wiki")){
            String localArchiveFile = "";
            String remotePath = "";
            if(docTemplate.equals("napa")){
                remotePath = NAPA_URL;
                localFile = NAPA_FILE;
            }else{
                switch (lang) {
                    case "de":
                        remotePath = DEWIKI_URL;
                        localFile = DEWIKI_FILE;
                        break;
                    case "en":
                        remotePath = ENWIKI_URL;
                        localFile = ENWIKI_FILE;
                        break;
                    case "es":
                        remotePath = ESWIKI_URL;
                        localFile = ESWIKI_FILE;
                        break;
                    case "fr":
                        remotePath = FRWIKI_URL;
                        localFile = FRWIKI_FILE;
                        break;
                }
            }
            localArchiveFile = localFile + ".gz";
            localFileName = loadDataFile(localArchiveFile, remotePath, localFile, docTemplate);
        }
        return localFileName;
    }

    public static File writeForElastic(Map<String, String> objects, String dataset, String operation) {
        try {
            final File elasticFile = getNewFile(ElasticSync.filePrefix, operation);
            final FileWriter writer = new FileWriter(elasticFile);
            for (String id: objects.keySet()) {
                writer.write(createElasticObject(dataset, id, operation));
                if ("update".equals(operation)){
                    writer.write("{ \"doc\" : "+objects.get(id)+" }\n");
                }
                else if ("create".equals(operation)){
                    writer.write(objects.get(id) + "\n");
                }
            }
            writer.flush();
            writer.close();
            return elasticFile;
        }catch(IOException ioe){
            System.err.println("Cannot write data file for Elastic - " + ioe.getMessage());
            ioe.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    public static File getNewFile(String filePrefix, String fileType) {
        File f = new File(filePrefix + fileType + ".txt");
        if (f.exists()) {
            f.delete();
        }
        return f;
    }



}
