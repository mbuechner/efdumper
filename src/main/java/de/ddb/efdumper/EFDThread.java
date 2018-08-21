/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.ddb.efdumper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Büchner <m.buechner@dnb.de>
 */
final class EFDThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(Thread.class);

    private final String url;
    private final String language;
    private final JsonGenerator jGenerator;
    private final String targetFile;
    private final String faultyFolder;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public EFDThread(String url, String language, JsonGenerator jGenerator, String targetFile, String faultyFolder) {
        this.url = url;
        this.language = language;
        this.jGenerator = jGenerator;
        this.targetFile = targetFile;
        this.faultyFolder = faultyFolder;
    }

    @Override
    public void run() {
        HttpURLConnection con = null;

        for (int i = 0; i < App.MAXTHREADRERUN; ++i) {
            try {
                con = (HttpURLConnection) new URL(url).openConnection();
                con.setRequestProperty("Accept-Language", language);

                con.connect();

                if (con.getResponseCode() != HttpURLConnection.HTTP_OK) {
                    final JsonNode node = objectMapper.readTree(con.getErrorStream());
                    final String errorText = node.get("Error").textValue();
                    if (errorText.contains("currently not supported by Entity Facts")) {
                        LOG.warn(url + ": Response: " + con.getResponseMessage() + ". " + (i + 1) + " attempt(s). " + node.toString());
                        con.disconnect();
                        break;
                    } else if (errorText.contains("NOT found in database")) {
                        LOG.error(url + ": Response: " + con.getResponseMessage() + ". " + (i + 1) + " attempt(s). " + node.toString());
                        con.disconnect();
                        break;
                    }

                    if (i >= App.MAXTHREADRERUN - 1) {
                        LOG.error(url + ": Response: " + con.getResponseMessage() + ". " + (i + 1) + " attempt(s). " + node.toString());
                        con.disconnect();
                        break; // no need to try again
                    }
                } else {
                    final JsonNode node = objectMapper.readTree(con.getInputStream());

                    synchronized (jGenerator) {
                        objectMapper.writeValue(jGenerator, node);
                        jGenerator.writeRaw('\n');
                        jGenerator.flush();
                        LOG.info(url + " has been written to " + targetFile);
                    }
                    con.disconnect();
                    break; // all went fine so we escape here
                }
            } catch (JsonMappingException | JsonParseException e) {
                LOG.error("JSON of " + url + " is malformed.", e);

                // write faulty item to hdd
                try {
                    final String faultyFilename = url.substring(url.lastIndexOf('/') + 1, url.length());
                    if (con != null) {
                        FileUtils.copyInputStreamToFile(con.getInputStream(), new File(faultyFolder + File.separator + faultyFilename + ".json"));
                        FileUtils.copyInputStreamToFile(con.getErrorStream(), new File(faultyFolder + File.separator + faultyFilename + "_httpError.json"));
                    }
                } catch (IOException ex) {
                    LOG.error("Could not write stream to hdd. :(", ex);
                }

                if (con != null) {
                    con.disconnect();
                }
                break; // no need to try again
            } catch (MalformedURLException e) {
                LOG.error("URL " + url + " is malformed.", e);
                if (con != null) {
                    con.disconnect();
                }
                break; // no need to try again
            } catch (ConnectException e) {
                if (i >= App.MAXTHREADRERUN - 1) {
                    LOG.error("For " + url + " the server did not response. That was attempt " + (i + 1) + " of " + App.MAXTHREADRERUN + ".", e);
                }
                if (con != null) {
                    con.disconnect();
                }
                break; // no need to try again
            } catch (IOException e) {
                LOG.warn("Writing data for " + url + " failed. That was attempt " + (i + 1) + " of " + App.MAXTHREADRERUN + ".", e);
                if (con != null) {
                    con.disconnect();
                }
                break; // no need to try again
            }

            try {
                Thread.sleep(App.THREADSLEEP);
            } catch (InterruptedException ex) {
                LOG.error("Thread sleep was interupted! (That shouldn't happend)", ex);
            }
        }
    }
}
