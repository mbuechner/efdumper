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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.github.jsonldjava.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import static java.net.HttpURLConnection.setFollowRedirects;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author buechner
 */
public class EFDExecutor {

    private final File[] GND_DUMPS_TTL;
    private final String OUTPUT_FILE;
    protected final static String EF_URL = "http://hub.culturegraph.org/entityfacts/{ID}";
    private final static int MAXTHREADS = 16;
    protected final static int MAXTHREADRERUN = 3;
    protected final static int THREADSLEEP = 5; // seconds

    private final static ThreadFactory WORKER_FACTORY = new ThreadFactoryBuilder()
            .setNameFormat("Downloader-%d")
            .setDaemon(true)
            .build();
    private final static ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(MAXTHREADS, WORKER_FACTORY);

    private static final Logger LOG = LoggerFactory.getLogger(EFDExecutor.class);

    protected static Set<String> LANGUAGES = new HashSet<String>() {
        {
            //add("en-US");
            add("de-DE");
        }
    };

    private final static Map<String, Integer> ALLOWED_ENTITY_TYPES = Collections.synchronizedMap(new HashMap<String, Integer>() {
        {
            put("http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson", 0); // 4,55 Mio
            //Subklassen
            put("http://d-nb.info/standards/elementset/gnd#CollectivePseudonym", 0);
            put("http://d-nb.info/standards/elementset/gnd#Gods", 0);
            put("http://d-nb.info/standards/elementset/gnd#LiteraryOrLegendaryCharacter", 0);
            put("http://d-nb.info/standards/elementset/gnd#Pseudonym", 0);
            put("http://d-nb.info/standards/elementset/gnd#RoyalOrMemberOfARoyalHouse", 0);
            put("http://d-nb.info/standards/elementset/gnd#Spirits", 0);

            put("http://d-nb.info/standards/elementset/gnd#CorporateBody", 0); // 1,38 Mio.
            //Subklassen
            put("http://d-nb.info/standards/elementset/gnd#Company", 0);
            put("http://d-nb.info/standards/elementset/gnd#FictiveCorporateBody", 0);
            put("http://d-nb.info/standards/elementset/gnd#MusicalCorporateBody", 0);
            put("http://d-nb.info/standards/elementset/gnd#OrganOfCorporateBody", 0);
            put("http://d-nb.info/standards/elementset/gnd#ProjectOrProgram", 0);
            put("http://d-nb.info/standards/elementset/gnd#ReligiousAdministrativeUnit", 0);
            put("http://d-nb.info/standards/elementset/gnd#ReligiousCorporateBody", 0);

            put("http://d-nb.info/standards/elementset/gnd#Family", 0);
            //Subklassen
            // keine

            put("http://d-nb.info/standards/elementset/gnd#PlaceOrGeographicName", 0);
            //Subklassen
            put("http://d-nb.info/standards/elementset/gnd#Company", 0);
            put("http://d-nb.info/standards/elementset/gnd#AdministrativeUnit", 0);
            put("http://d-nb.info/standards/elementset/gnd#BuildingOrMemorial", 0);
            put("http://d-nb.info/standards/elementset/gnd#Country", 0);
            put("http://d-nb.info/standards/elementset/gnd#ExtraterrestrialTerritory", 0);
            put("http://d-nb.info/standards/elementset/gnd#FictivePlace", 0);
            put("http://d-nb.info/standards/elementset/gnd#MemberState", 0);
            put("http://d-nb.info/standards/elementset/gnd#NameOfSmallGeographicUnitLyingWithinAnotherGeographicUnit", 0);
            put("http://d-nb.info/standards/elementset/gnd#NaturalGeographicUnit", 0);
            put("http://d-nb.info/standards/elementset/gnd#ReligiousTerritory", 0);
            put("http://d-nb.info/standards/elementset/gnd#TerritorialCorporateBodyOrAdministrativeUnit", 0);
            put("http://d-nb.info/standards/elementset/gnd#WayBorderOrLine", 0);
        }
    });

    private final static List<String> BEACON_HEADER = new ArrayList<String>() {
        {
            add("#FORMAT: BEACON");
            add("#PREFIX: http://d-nb.info/gnd/");
            add("#TARGET: http://d-nb.info/gnd/");
            add("#CONTACT: Gemeinsame Normdatei (GND) <gnd-info@dnb.de>");
            add("#INSTITUTION: Deutsche Nationalbibliothek");
            add("#DESCRIPTION: List of deprecated GND URIs to their valid primary GND URI");
            add("#TIMESTAMP: {DATE}");
            add("#UPDATE: monthly");
        }
    };

    public EFDExecutor(File[] GND_DUMPS_TTL, String OUTPUT_FILE) throws IOException {
        this.GND_DUMPS_TTL = GND_DUMPS_TTL.clone();
        this.OUTPUT_FILE = OUTPUT_FILE;
        setFollowRedirects(true);
    }

    public void makeDump() throws IOException {

        final ThreadFactory controllerFactory = new ThreadFactoryBuilder()
                .setNameFormat("Controller-%d")
                .setDaemon(true)
                .build();

        final ScheduledExecutorService conExSe = Executors.newScheduledThreadPool(GND_DUMPS_TTL.length + 1, controllerFactory); // one parser for every dump AND one for monitoring

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

        for (File dumpFile : GND_DUMPS_TTL) {

            final PipedRDFIterator<Triple> iter = new PipedRDFIterator<>();
            final PipedRDFStream<Triple> is = new PipedTriplesStream(iter);

            // Create a runnable for our parser thread
            final Runnable parser = new Runnable() {
                @Override
                public void run() {
                    LOG.info("Start reading {} ({} of {})...", dumpFile.getAbsolutePath(), Arrays.asList(GND_DUMPS_TTL).indexOf(dumpFile) + 1, GND_DUMPS_TTL.length);
                    try (final InputStream fis = new FileInputStream(dumpFile);
                            final GZIPInputStream gzip = new GZIPInputStream(fis)) {
                        RDFDataMgr.parse(is, gzip, Lang.TTL);
                    } catch (IOException ex) {
                        LOG.error(ex.getLocalizedMessage(), ex);
                    }
                }
            };

            // Start the parser on another thread
            conExSe.submit(parser);

            int i = 0;
            int j = 0;
            while (iter.hasNext()) {

                // Do something with each triple
                final Triple next = iter.next();
                final String gndId = next.getSubject().toString().replace("http://d-nb.info/gnd/", "");
                final String url = EF_URL.replace("{ID}", gndId);
                final String object = next.getObject().toString();

                // nur erlaubte Entitätentypen (Personen, Geografika usw.)
                if (ALLOWED_ENTITY_TYPES.containsKey(object)) {

                    j++;
                    LANGUAGES.forEach((language) -> {
                        EXECUTOR.submit(new EFDThread(url, language, object, jg.get(language), 1));
                    });
                }

                if (i++ % 1_000000 == 0) {
                    LOG.info("{} entities in {} processed, {} are accepted entity types...", i - 1, dumpFile, j);
                }
//                if (++i > 64) {
//                    break;
//                }

            }
            LOG.info("Finished processing {} entities in {} processed, {} are accepted entity types.", i - 1, dumpFile, j);
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

        int sum = 0;
        for (Entry<String, Integer> e : ALLOWED_ENTITY_TYPES.entrySet()) {
            sum = sum + e.getValue();
        }

        LOG.info("Statistics: {} entities dumped from Entity Facts", sum);
        LOG.info("Statistics: {}", ALLOWED_ENTITY_TYPES);
    }

    public void makeBeacon() throws IOException {

        final ThreadFactory conFact = new ThreadFactoryBuilder()
                .setNameFormat("Controller-%d")
                .setDaemon(true)
                .build();

        final ExecutorService conExSe = Executors.newFixedThreadPool(GND_DUMPS_TTL.length + 1, conFact); // one parser and one processor
        final String filename = OUTPUT_FILE.replace("{TIMESTAMP}", new SimpleDateFormat("yyyyMMdd").format(new Date()));
        LOG.info("Output file will be {}", filename);

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(filename), Charset.forName("UTF-8")))) {

            for (String line : BEACON_HEADER) {
                bw.write(line.replace("{DATE}", new SimpleDateFormat("yyyy-MM-dd").format(new Date())));
                bw.newLine();
            }
            bw.flush();

            // do it for every dump file
            for (File dumpFile : GND_DUMPS_TTL) {

                // add parsers
                final PipedRDFIterator<Triple> iter = new PipedRDFIterator<>();
                final PipedRDFStream<Triple> is = new PipedTriplesStream(iter);

                final Runnable processor = new Runnable() {
                    @Override
                    public void run() {

                        try (final InputStream fis = new FileInputStream(dumpFile);
                                final GZIPInputStream gzip = new GZIPInputStream(fis)) {
                            LOG.info("Start precessing {} ({} of {})...", dumpFile.getAbsolutePath(), Arrays.asList(GND_DUMPS_TTL).indexOf(dumpFile) + 1, GND_DUMPS_TTL.length);
                            RDFDataMgr.parse(is, gzip, Lang.TTL);
                        } catch (IOException ex) {
                            LOG.error(ex.getLocalizedMessage(), ex);
                        }
                    }
                };

                // Start the processor on another thread
                conExSe.submit(processor);

                int i = 0;
                int j = 0;
                String primaryId = "";
                String entityType = "";
                Set<String> variantIds = new HashSet<>();

                while (iter.hasNext()) {
                    // Do something with each triple
                    final Triple next = iter.next();

                    final String subject = next.getSubject().toString();
                    final String object = next.getObject().toString();
                    final String predicate = next.getPredicate().toString();

                    if (!primaryId.equals(subject)) {
                        if (getALLOWED_ENTITY_TYPES().containsKey(entityType)) {
                            if (!variantIds.isEmpty()) {

                                // write statistics
                                getALLOWED_ENTITY_TYPES().put(entityType, getALLOWED_ENTITY_TYPES().get(entityType) + 1);

                                for (String variantID : variantIds) {
                                    try {
                                        variantID = variantID.replace("http://d-nb.info/gnd/", "");
                                        variantID = variantID.replaceAll("\"", "");
                                        bw.write(variantID + "||" + primaryId.replace("http://d-nb.info/gnd/", ""));
                                        bw.newLine();
                                    } catch (IOException e) {
                                        LOG.error("Could not write line to {}. {}", filename, e.getLocalizedMessage());
                                    }
                                }
                            }
                            ++j;
                        }
                        primaryId = subject;
                        entityType = object;
                        variantIds = new HashSet<>();

                        if (i++ % 1_000000 == 0) {
                            LOG.info("{} entities in {} processed, {} are accepted entity types...", i - 1, dumpFile, j);
                        }
                    }

                    if (predicate.equals("http://d-nb.info/standards/elementset/dnb#deprecatedUri")) {
                        variantIds.add(object); //
                    }
                }
                LOG.info("Finished processing {} entities in {} processed, {} are accepted entity types.", i - 1, dumpFile, j);
            }

            // shutdown all threads
            conExSe.shutdown();
        }
        // wait until's done
        try {
            conExSe.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            // nothing
        }
        LOG.info("Statistics: {}", ALLOWED_ENTITY_TYPES);
    }

    /**
     * @return the executor
     */
    public static ScheduledExecutorService getExecutor() {
        return EXECUTOR;
    }

    /**
     * @return the ALLOWED_ENTITY_TYPES
     */
    public static Map<String, Integer> getALLOWED_ENTITY_TYPES() {
        return ALLOWED_ENTITY_TYPES;
    }

    /**
     * @param aLANGUAGES the LANGUAGES to set
     */
    public static void setLANGUAGES(Set<String> aLANGUAGES) {
        LANGUAGES = aLANGUAGES;
    }

}
