package com.couchbase.javaclient.utils;

import com.couchbase.javaclient.DataTransformer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.GZIPInputStream;

public final class FileUtils {

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

}
