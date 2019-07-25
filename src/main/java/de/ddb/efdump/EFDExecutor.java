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
import java.nio.charset.Charset;
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

    protected final static String EF_URL = "http://hub.culturegraph.org/entityfacts/{ID}";
    protected final static int MAXTHREADRERUN = 3;
    protected final static int THREADSLEEP = 5; // seconds

    private static EFDExecutor instance;
    private ScheduledExecutorService EXECUTOR;
    private ThreadPoolExecutor TPE;
    private File[] GndCsvFiles;
    private String OutputFile;
    private int maxThreads;
    private int maxSubmittedTasks;

    private static final Logger LOG = LoggerFactory.getLogger(EFDExecutor.class);

    private static Set<String> LANGUAGES = new HashSet<String>() {
        {
            //add("en-US");
            add("de-DE");
        }
    };

    public static synchronized EFDExecutor getEFDExecutor() {
        if (EFDExecutor.instance == null) {
            EFDExecutor.instance = new EFDExecutor();
        }
        return EFDExecutor.instance;
    }

    private EFDExecutor() {
    }

    public void init(File[] GND_CSV_FILES, String OUTPUT_FILE, int maxThreads, int maxSubmittedTasks) throws IOException {
        this.GndCsvFiles = GND_CSV_FILES.clone();
        this.OutputFile = OUTPUT_FILE;
        this.maxThreads = maxThreads;
        this.maxSubmittedTasks = maxSubmittedTasks;
        setFollowRedirects(true);

        EXECUTOR = Executors.newScheduledThreadPool(maxThreads);
        TPE = (ThreadPoolExecutor) EXECUTOR;
    }

    public void makeDump() throws IOException {
        final ScheduledExecutorService conExSe = Executors.newScheduledThreadPool(GndCsvFiles.length + 1); // one parser for every dump AND one for monitoring

        // add monitoring
        final Runnable queueCounter = new Runnable() {
            @Override
            public void run() {
                final int active = TPE.getQueue().size() >= maxThreads ? maxThreads : TPE.getQueue().size();
                final int inQueue = Integer.max(TPE.getQueue().size() - TPE.getPoolSize(), 0) + active;
                final long completed = TPE.getCompletedTaskCount();
                LOG.info("Status: {} processed. {} running. {} in queue.",
                        completed,
                        active,
                        (inQueue == maxSubmittedTasks) ? inQueue + " (MAX)" : inQueue);
            }
        };
        conExSe.scheduleWithFixedDelay(queueCounter, 0, 10, TimeUnit.SECONDS); // log every 10 Sek.

        final JsonFactory jfactory = new JsonFactory();
        final Map<String, String> fn = new HashMap<>();
        final Map<String, FileOutputStream> fos = new HashMap<>();
        final Map<String, JsonGenerator> jg = new HashMap<>();

        // init FileOutputStreams and JsonGenerator
        final String timestamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
        for (String language : getLANGUAGES()) {

            final String filename = OutputFile.replace("{TIMESTAMP}", timestamp).replace("{LANG}", language);
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

        for (File dumpFile : GndCsvFiles) {

            final FileReader filereader = new FileReader(dumpFile, Charset.forName("UTF-8"));
            final CSVReader csvReader = new CSVReader(filereader);
            String[] nextRecord;

            while ((nextRecord = csvReader.readNext()) != null) {
                if (nextRecord.length > 0) {
                    final String gndId = nextRecord[0];
                    final String url = EF_URL.replace("{ID}", gndId);
                    getLANGUAGES().forEach((language) -> {
                        EXECUTOR.submit(new EFDThread(url, language, jg.get(language), 1));
                    });
                    ++sum;
                }

                // wait until submit more task (heap space problem)
                while (TPE.getQueue().size() >= maxSubmittedTasks);
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
        for (String language : getLANGUAGES()) {
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
    public ScheduledExecutorService getExecutor() {
        return EXECUTOR;
    }

    /**
     * @param aLANGUAGES the LANGUAGES to set
     */
    public static void setLANGUAGES(Set<String> aLANGUAGES) {
        LANGUAGES = aLANGUAGES;
    }

    /**
     * @return the LANGUAGES
     */
    public static Set<String> getLANGUAGES() {
        return LANGUAGES;
    }

}
