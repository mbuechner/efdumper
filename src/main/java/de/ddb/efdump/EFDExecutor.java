/*
 * Copyright 2018 Deutsche Digitale Bibliothek.
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

import au.com.bytecode.opencsv.CSVReader;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import static java.net.HttpURLConnection.setFollowRedirects;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author buechner
 */
public class EFDExecutor {

    private final File[] GND_CSV_FILES;
    private final String OUTPUT_FILE;
    protected final static String EF_URL = "http://hub.culturegraph.org/entityfacts/{ID}";
    private final static int MAXTHREADS = 16;
    protected final static int MAXTHREADRERUN = 3;
    protected final static int THREADSLEEP = 5; // seconds

    private final static ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(MAXTHREADS);

    private static final Logger LOG = LoggerFactory.getLogger(EFDExecutor.class);

    protected static Set<String> LANGUAGES = new HashSet<String>() {
        {
            //add("en-US");
            add("de-DE");
        }
    };

    public EFDExecutor(File[] GND_CSV_FILES, String OUTPUT_FILE) throws IOException {
        this.GND_CSV_FILES = GND_CSV_FILES.clone();
        this.OUTPUT_FILE = OUTPUT_FILE;
        setFollowRedirects(true);
    }

    public void makeDump() throws IOException {

        final ScheduledExecutorService conExSe = Executors.newScheduledThreadPool(GND_CSV_FILES.length + 1); // one parser for every dump AND one for monitoring

        // add monitoring
        final Runnable queueCounter = () -> {
            final ThreadPoolExecutor tpe = (ThreadPoolExecutor) EXECUTOR;
            int active = tpe.getQueue().size() >= MAXTHREADS ? MAXTHREADS : tpe.getQueue().size();
            int inQueue = Integer.max(tpe.getQueue().size() - tpe.getPoolSize(), 0) + active;

            LOG.info("Queue size: {} running of {}", active, inQueue);
        };
        conExSe.scheduleWithFixedDelay(queueCounter, 0, 10, TimeUnit.SECONDS); // log every 10 Sek.

        final JsonFactory jfactory = new JsonFactory();
        final Map<String, String> fn = new HashMap<>();
        final Map<String, FileOutputStream> fos = new HashMap<>();
        final Map<String, JsonGenerator> jg = new HashMap<>();

        // init FileOutputStreams and JsonGenerator
        final String timestamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
        for (String language : LANGUAGES) {

            final String filename = OUTPUT_FILE.replace("{TIMESTAMP}", timestamp).replace("{LANG}", language);
            fn.put(language, filename);

            final FileOutputStream fo = new FileOutputStream(filename);
            fos.put(language, fo);

            final JsonGenerator jGenerator = jfactory.createGenerator(fo, JsonEncoding.UTF8);

            jGenerator.setPrettyPrinter(new MinimalPrettyPrinter(""));
            jGenerator.writeStartArray();
            jg.put(language, jGenerator);
            LOG.info("Language {} will be in file {}...", language, filename);
        }

        int sum = 0;

        for (File dumpFile : GND_CSV_FILES) {

            final FileReader filereader = new FileReader(dumpFile);
            final CSVReader csvReader = new CSVReader(filereader);
            String[] nextRecord;

            while ((nextRecord = csvReader.readNext()) != null) {
                if (nextRecord.length > 0) {
                    final String gndId = nextRecord[0];
                    final String url = EF_URL.replace("{ID}", gndId);
                    LANGUAGES.forEach((language) -> {
                        EXECUTOR.submit(new EFDThread(url, language, jg.get(language), 1));
                    });
                    ++sum;
                }
            }
        }
        EXECUTOR.shutdown();

        try {
            EXECUTOR.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            // nothing
        }

        conExSe.shutdown();

        try {
            conExSe.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            // nothing
        }

        // close all FileOutputStreams and JsonGenerator
        for (String language : LANGUAGES) {
            jg.get(language).writeEndArray();
            jg.get(language).flush();
            jg.get(language).close();
            fos.get(language).flush();
            fos.get(language).close();
        }

        LOG.info("Statistics: {} entities processed", sum);
    }

    /**
     * @return the executor
     */
    public static ScheduledExecutorService getExecutor() {
        return EXECUTOR;
    }

    /**
     * @param aLANGUAGES the LANGUAGES to set
     */
    public static void setLANGUAGES(Set<String> aLANGUAGES) {
        LANGUAGES = aLANGUAGES;
    }

}
