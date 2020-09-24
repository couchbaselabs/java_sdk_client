package com.couchbase.javaclient;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

public class DataTransformer {

    private final static Logger log = Logger.getLogger(DataTransformer.class);
    private static JSONParser jsonParser = new JSONParser();

    public static String pyJsonToJavaJson(String inFile, String outFile, String dataSet) {
        PrintWriter writer = null;
        File file = new File(outFile);
        try {
            writer = new PrintWriter(file, "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            log.warn(e);
        }
        int count = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(inFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) break;

                if ("napa".equals(dataSet)) {
                    line = parseNapa(line);
                } else {
                    line = parseWiki(line);
                }

                try {
                    Object obj = jsonParser.parse(line);
                    JSONObject json = (JSONObject) obj;
                    writer.println(line);
                } catch (ParseException e) {
                    count++;
                }
            }
        } catch (IOException e) {
            log.error(e);
        }
        writer.close();
        log.info("Count error transform = " + count);
        return file.getAbsolutePath();
    }

    private static String parseWiki(String line) {
        line = line.replaceAll("\"", "\\\\\"");
        // ', u' -> ", "
        line = line.replaceAll("', u'", "\", \"");
        line = line.replaceAll("\\\\\", u'", "\", \"");
        line = line.replaceAll("\\{u'", "{\"");
        line = line.replaceAll("': u'", "\": \"");
        line = line.replaceAll("}, u'", "}, \"");
        line = line.replaceAll(", u'", ", \"");
        line = line.replaceAll("u\\\\\"", "\"");
        line = line.replaceAll("\\\\\"}", "\"}");

        // ': { -> ": {
        //
        line = line.replaceAll("': \\{", "\": \\{");
        line = line.replaceAll("': \"", "\": \"");

        line = line.replaceAll("'#text\":", "\"#text\":");
        line = line.replaceAll("'}", "\"}");
        line = line.replaceAll("animalia''\"}", "animalia'''}");
        line = line.replaceAll(": None", ": " + null);
        line = line.replaceAll("\\\\'", "'");
        // ', " -> ", "
        line = line.replaceAll("', \"", "\", \"");
        line = line.replaceAll("': null,", "\": null,");
        line = line.replaceAll("': null}", "\": null}");
        line = line.replaceAll("hurt yo\". \\(", "hurt you\\\\\". (");
        return line;
    }

    private static String parseNapa(String line) {
        line = line.replaceAll(":False,", ":false,");
        line = line.replaceAll(":True,", ":true,");
        line = line.replaceAll(":False}", ":false}");
        line = line.replaceAll(":True}", ":true}");
        return line;
    }
}

