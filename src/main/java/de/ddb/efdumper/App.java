package de.ddb.efdumper;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GND-Dump: https://data.dnb.de/opendata/
 * @author Michael Büchner
 */

public final class App {

    protected final String GND_DUMP_TTL;
    protected final String OUTPUT_JSON_FILE;
    protected final String OUTPUT_FAULTY_FOLDER;
    protected final static String EF_URL = "http://hub.culturegraph.org/entityfacts/{ID}";

    public final static int MAXTHREADS = 16;
    public final static int MAXTHREADRERUN = 3;
    public final static int THREADSLEEP = 1500;

    public static final Set<String> LANGUAGES = new HashSet<String>() {
        {
            add("en-US");
            add("de-DE");
        }
    };

    protected final static Set<String> ALLOWED_ENTITY_TYPES = new HashSet<String>() {
        {
            add("http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson"); // 4,55 Mio
            //Subklassen
            add("http://d-nb.info/standards/elementset/gnd#CollectivePseudonym");
            add("http://d-nb.info/standards/elementset/gnd#Gods");
            add("http://d-nb.info/standards/elementset/gnd#LiteraryOrLegendaryCharacter");
            add("http://d-nb.info/standards/elementset/gnd#Pseudonym");
            add("http://d-nb.info/standards/elementset/gnd#RoyalOrMemberOfARoyalHouse");
            add("http://d-nb.info/standards/elementset/gnd#Spirits");

            add("http://d-nb.info/standards/elementset/gnd#CorporateBody"); // 1,38 Mio.
            //Subklassen
            add("http://d-nb.info/standards/elementset/gnd#Company");
            add("http://d-nb.info/standards/elementset/gnd#FictiveCorporateBody");
            add("http://d-nb.info/standards/elementset/gnd#MusicalCorporateBody");
            add("http://d-nb.info/standards/elementset/gnd#OrganOfCorporateBody");
            add("http://d-nb.info/standards/elementset/gnd#ProjectOrProgram");
            add("http://d-nb.info/standards/elementset/gnd#ReligiousAdministrativeUnit");
            add("http://d-nb.info/standards/elementset/gnd#ReligiousCorporateBody");

            add("http://d-nb.info/standards/elementset/gnd#Family");
            //Subklassen
            // keine

            add("http://d-nb.info/standards/elementset/gnd#PlaceOrGeographicName");
            //Subklassen
            add("http://d-nb.info/standards/elementset/gnd#Company");
            add("http://d-nb.info/standards/elementset/gnd#AdministrativeUnit");
            add("http://d-nb.info/standards/elementset/gnd#BuildingOrMemorial");
            add("http://d-nb.info/standards/elementset/gnd#Country");
            add("http://d-nb.info/standards/elementset/gnd#ExtraterrestrialTerritory");
            add("http://d-nb.info/standards/elementset/gnd#FictivePlace");
            add("http://d-nb.info/standards/elementset/gnd#MemberState");
            add("http://d-nb.info/standards/elementset/gnd#NameOfSmallGeographicUnitLyingWithinAnotherGeographicUnit");
            add("http://d-nb.info/standards/elementset/gnd#NaturalGeographicUnit");
            add("http://d-nb.info/standards/elementset/gnd#ReligiousTerritory");
            add("http://d-nb.info/standards/elementset/gnd#TerritorialCorporateBodyOrAdministrativeUnit");
            add("http://d-nb.info/standards/elementset/gnd#WayBorderOrLine");
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public App(String GND_DUMP_TTL, String OUTPUT_JSON_FILE, String OUTPUT_FAULTY_FOLDER) throws IOException {
        this.GND_DUMP_TTL = GND_DUMP_TTL;
        this.OUTPUT_JSON_FILE = OUTPUT_JSON_FILE;
        this.OUTPUT_FAULTY_FOLDER = OUTPUT_FAULTY_FOLDER;
        final File faulty_folder = new File(OUTPUT_FAULTY_FOLDER);
        if (!faulty_folder.mkdir()) {
            throw new IOException("Faulty folder " + faulty_folder.getAbsolutePath() + " already exists.");
        }
        HttpURLConnection.setFollowRedirects(true);
    }

    @SuppressWarnings("empty-statement")
    public void run() throws IOException {
        try (final InputStream fis = new FileInputStream(GND_DUMP_TTL);
                final GZIPInputStream gzip = new GZIPInputStream(fis);
                // final InputStreamReader isr = new InputStreamReader(gzip, Charset.forName("UTF-8"))
                ) {

            final PipedRDFIterator<Triple> iter = new PipedRDFIterator<>();
            final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);
            final ExecutorService executor = Executors.newSingleThreadExecutor();

            // Create a runnable for our parser thread
            final Runnable parser = new Runnable() {
                @Override
                public void run() {
                    RDFDataMgr.parse(inputStream, gzip, Lang.TTL);
                }
            };

            // Start the parser on another thread
            executor.submit(parser);

            final JsonFactory jfactory = new JsonFactory();
            final Map<String, String> fn = new HashMap<>();
            final Map<String, FileOutputStream> fos = new HashMap<>();
            final Map<String, JsonGenerator> jg = new HashMap<>();

            // init FileOutputStreams and JsonGenerator
            for (String language : LANGUAGES) {
                final String timestamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
                final String filename = OUTPUT_JSON_FILE.replace("{TIMESTAMP}", timestamp).replace("{LANG}", language);
                fn.put(language, filename);

                final FileOutputStream fo = new FileOutputStream(filename);
                fos.put(language, fo);

                final JsonGenerator jGenerator = jfactory.createGenerator(fo, JsonEncoding.UTF8);

                jGenerator.setPrettyPrinter(new MinimalPrettyPrinter(""));
                jGenerator.writeStartArray();
                jg.put(language, jGenerator);
                LOG.info("Language {} will be in file {}...", language, filename);
            }

//            int i = 0;
            while (iter.hasNext()) {

                // Do something with each triple
                final Triple next = iter.next();
                final String gndId = next.getSubject().toString().replace("http://d-nb.info/gnd/", "");
                final String url = EF_URL.replace("{ID}", gndId);
                final String object = next.getObject().toString();

                // nur erlaubte Entitätentypen (Personen, Geografika usw.)
                if (!ALLOWED_ENTITY_TYPES.contains(object)) {
                    continue;
                }

                // wait until's free
                while (!EFDScheduler.getInstance().canAdd(LANGUAGES.size()));

                for (String language : LANGUAGES) {
                    // add it
                    EFDScheduler.getInstance().add(new Thread(new EFDThread(url, language, jg.get(language), fn.get(language), OUTPUT_FAULTY_FOLDER)));
                }

//                if (++i > 5) {
//                    break;
//                }
            }
            // wait until's done

            while (!EFDScheduler.getInstance()
                    .isDone());

            // close all FileOutputStreams and JsonGenerator
            for (String language : LANGUAGES) {
                jg.get(language).writeEndArray(); // ]
                jg.get(language).flush();
                jg.get(language).close();
                fos.get(language).flush();
                fos.get(language).close();
            }

            executor.shutdownNow();
            iter.close();
        }

        LOG.info("Done. Bye!");
    }

    public static void main(String[] args) throws IOException {

        LOG.info("Start...");

        String gndTtlDump = "Tbgesamt1805gnd.ttl.gz";
        String faultyRequestFolder = "faulty/";
        String outputFile = "{TIMESTAMP}-EFDump-Tb-{LANG}.json";

        final Options options = new Options();
        options.addOption("i", true, "GNT Turtle Dump as GZipped File (default: GND.ttl.gz)");
        options.addOption("o", true, "File name of output file (default: {TIMESTAMP}-EFDump-{LANG}.json)");
        options.addOption("e", true, "Folder, where all error faulty requests will be saved (default: faulty/)");

        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("i")) {
                gndTtlDump = cmd.getOptionValue("i");
            }

            if (cmd.hasOption("e")) {
                faultyRequestFolder = cmd.getOptionValue("e");
            }

            if (cmd.hasOption("o")) {
                outputFile = cmd.getOptionValue("o");
                if (outputFile.contains(".")) {
                    outputFile = new StringBuilder(outputFile).insert(outputFile.lastIndexOf(".") - 1, "-{LANG}").toString();
                } else {
                    outputFile += "-{LANG}";
                }
            }

            LOG.info("Using GND turtle dump in file: " + gndTtlDump);

        } catch (ParseException ex) {
            System.err.println("Wrong parameters: " + ex.getMessage());
            final HelpFormatter help = new HelpFormatter();
            help.printHelp("java -jar GNDParser.jar -i GND.ttl.gz", options);
            System.exit(1);
        }

        try {
            new App(gndTtlDump, outputFile, faultyRequestFolder).run();
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            System.exit(1);
        }

        LOG.info("All done. Bye!");
    }
}
