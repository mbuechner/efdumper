/* 
 * Copyright 2016-2018, Michael Büchner <m.buechner@dnb.de>
 * Deutsche Digitale Bibliothek
 * c/o Deutsche Nationalbibliothek
 * Informationsinfrastruktur
 * Adickesallee 1, D-60322 Frankfurt am Main 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ddb.efdump;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static de.ddb.efdump.EFDExecutor.MAXTHREADRERUN;
import static de.ddb.efdump.EFDExecutor.THREADSLEEP;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Michael Büchner
 */
public class EFDThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(EFDThread.class);

    private boolean done = false;
    private final int runCount; // re-run counter
    private final String url;
    private final String language;
    private final JsonGenerator jGenerator;
    private final ObjectMapper OM;

    /**
     * A worker thread to download a JSON object from Entity Facts
     *
     * @param url - URL to download from
     * @param language - Language to request
     * @param jGenerator - Dump file to save data
     * @param runCount Which run is that?
     */
    public EFDThread(String url, String language, JsonGenerator jGenerator, int runCount) {
        this.url = url;
        this.language = language;
        this.jGenerator = jGenerator;
        this.runCount = runCount;
        this.OM = new ObjectMapper();
    }

    @Override
    public void run() {
        HttpURLConnection con = null;

        try {
            con = (HttpURLConnection) new URL(url).openConnection();
            con.setRequestProperty("Accept-Language", language);
            con.connect();

            if (con.getResponseCode() != HttpURLConnection.HTTP_OK) {
                final JsonNode node = OM.readTree(con.getErrorStream());
                final String errorText = node.get("Error").textValue();
                if (errorText.contains("currently not supported by Entity Facts")) {
                    LOG.warn("{}: Response: {}. {} attempt(s). {}", url, con.getResponseMessage(), runCount, errorText);
                    done = true;
                } else if (errorText.contains("NOT found in database")) {
                    LOG.error("{}: Response: {}. {} attempt(s). {}", url, con.getResponseMessage(), runCount, errorText);
                    done = true;
                } else if (runCount >= MAXTHREADRERUN) {
                    LOG.error("{}: Response: {}. {} attempt(s). {}", url, con.getResponseMessage(), runCount, node);
                    done = true;
                } else {
                    LOG.warn("{}: Response: {}. {} attempt(s). {}", url, con.getResponseMessage(), runCount, node);
                }
            } else {
                final JsonNode node = OM.readTree(con.getInputStream());
                synchronized (jGenerator) {
                    OM.writeTree(jGenerator, node);
                    jGenerator.writeRaw('\n');
                    jGenerator.flush();
                    LOG.debug("{}: Successfully written to {} dump file", url, language);
                }
                done = true; // all went fine so we escape here
            }
        } catch (JsonMappingException | JsonParseException e) {
            if (runCount >= MAXTHREADRERUN) {
                LOG.error("{}: JSON of is malformed. {} attempt(s). {}", url, runCount, e.getLocalizedMessage());
                done = true; // no need to try again
            } else {
                LOG.warn("{}: JSON of is malformed. {} attempt(s). {}", url, runCount, e.getLocalizedMessage());
            }
        } catch (MalformedURLException e) {
            LOG.error("{}: Malformed URL.", url, e);
            done = true; // no need to try again
        } catch (ConnectException e) {
            if (runCount >= MAXTHREADRERUN) {
                LOG.error("{}: Server did not response. {} attempt(s). {}", url, runCount, e.getLocalizedMessage());
                done = true; // no need to try again
            } else {
                LOG.warn("{}: Server did not response. {} attempt(s). {}", url, runCount, e.getLocalizedMessage());
            }
        } catch (IOException e) {
            if (runCount >= MAXTHREADRERUN) {
                LOG.error("{}: Writing data failed. {} attempt(s). {}", url, runCount, e.getLocalizedMessage());
                done = true; // no need to try again
            } else {
                LOG.warn("{}: Server did not response. {} attempt(s). {}", url, runCount, e.getLocalizedMessage());
            }
        } finally {
            if (con != null) {
                con.disconnect();
            }

        }
        if (!done && runCount < MAXTHREADRERUN) {
            EFDExecutor.getExecutor().schedule(
                    new EFDThread(url, language, jGenerator, runCount + 1), THREADSLEEP, TimeUnit.SECONDS
            );
        }
    }
}
