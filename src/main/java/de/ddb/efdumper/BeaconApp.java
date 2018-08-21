package de.ddb.efdumper;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
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

public final class BeaconApp {

    protected final String GND_DUMP_TTL;
    protected final String OUTPUT_BEACON_FILE;
    protected final static String EF_URL = "http://hub.culturegraph.org/entityfacts/{ID}";

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

    private static final Logger LOG = LoggerFactory.getLogger(BeaconApp.class);

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

    public BeaconApp(String GND_DUMP_TTL, String OUTPUT_BEACON_FILE) {
        this.GND_DUMP_TTL = GND_DUMP_TTL;
        this.OUTPUT_BEACON_FILE = OUTPUT_BEACON_FILE;
        HttpURLConnection.setFollowRedirects(true);
    }

    public void run() throws IOException {
        final InputStream fis = new FileInputStream(GND_DUMP_TTL);
        final GZIPInputStream gzip = new GZIPInputStream(fis);

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

        final String filename = OUTPUT_BEACON_FILE.replace("{TIMESTAMP}", new SimpleDateFormat("yyyyMMdd").format(new Date()));

        LOG.info("Output file will be " + filename);

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), Charset.forName("UTF-8")))) {

            for (String line : BEACON_HEADER) {
                bw.write(line.replace("{DATE}", new SimpleDateFormat("yyyy-MM-dd").format(new Date())));
                bw.newLine();
            }

            String primaryId = "";
            String entityType = "";
            Set<String> variantIds = new HashSet<>();

            int i = 0;
            int j = 0;
            while (iter.hasNext()) {

                // Do something with each triple
                final Triple next = iter.next();

                final String subject = next.getSubject().toString();
                final String object = next.getObject().toString();
                final String predicate = next.getPredicate().toString();

                if (!primaryId.equals(subject)) {
                    if (ALLOWED_ENTITY_TYPES.contains(entityType)) {
                        for (String variantID : variantIds) {
                            bw.write(variantID.replace("http://d-nb.info/gnd/", "").replaceAll("\"", "") + "||" + primaryId.replace("http://d-nb.info/gnd/", ""));
                            bw.newLine();

                        }
                        ++j;
                    }
                    primaryId = subject;
                    entityType = object;
                    variantIds = new HashSet<>();
                    ++i;

                    if (i % 100000 == 0) {
                        LOG.info(i + " entities processed, " + j + " are accepted entity types...");
                    }
                }

                if (predicate.equals("http://d-nb.info/standards/elementset/dnb#deprecatedUri")) {
                    variantIds.add(object); //
                }
            }
        }
        LOG.info("Done. Bye!");
    }

    public static void main(String[] args) throws IOException {

        LOG.info("Start...");
        String gndTtlDump = "Tpgesamt1805gnd.ttl.gz";
        String outputFile = "{TIMESTAMP}-GND-Tp-variantId-beacon.txt";

        final Options options = new Options();
        options.addOption("i", true, "GNT Turtle Dump as GZipped File (default: GND.ttl.gz)");
        options.addOption("o", true, "File name of output file (default: {TIMESTAMP}-GND-variantId-beacon.txt)");

        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("i")) {
                gndTtlDump = cmd.getOptionValue("i");
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
            new BeaconApp(gndTtlDump, outputFile).run();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            System.exit(1);
        }
        LOG.info("Done.");
        System.exit(0); // WTF!
    }
}
