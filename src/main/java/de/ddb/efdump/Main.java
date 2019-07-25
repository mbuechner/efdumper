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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {

        String gndCsvFolder = "input/";
        String gndCsvFile = "";
        String outputFile = "{TIMESTAMP}-EFDump-{LANG}.json";
        int numberOfThreats = 16;
        int maxSubmittedTasks = 100000;

        final Options options = new Options();
        options.addOption("i", true, "CSV file containing one GND-ID in each line.");
        options.addOption("f", true, "Folder with CSV file(s) containing one GND-ID in each line (file name pattern is '*.csv'). Default: " + gndCsvFolder);
        options.addOption("l", true, "Language(s) to dump (comma for separation, e.g. de-DE,en-US). Default: de-DE");
        options.addOption("o", true, "File name of output file. Default: " + outputFile);
        options.addOption("t", true, "Number of parallel downloads. Default: " + numberOfThreats);
        options.addOption("m", true, "Number of maximal submitted download tasks into queue. Default: " + maxSubmittedTasks);
        options.addOption("v", false, "Print version");

        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("v")) {
                final Properties properties = new Properties();
                try (final BufferedReader is = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(".properties"), Charset.forName("UTF-8")));) {
                    properties.load(is);
                } catch (IOException ex) {
                    LOG.warn("Could not get properties in file .properties");
                }
                final StringBuilder sb = new StringBuilder();
                sb.append(properties.getProperty("efdumper.title", "efdump"));
                sb.append(" (");
                sb.append(properties.getProperty("efdumper.name", "efdump"));
                sb.append("), Version ");
                sb.append(properties.getProperty("efdumper.version", "<unknown>"));

                System.out.println(sb.toString());
                System.exit(0);
            }

            if (cmd.hasOption("i")) {
                gndCsvFile = cmd.getOptionValue("i");
            }

            if (cmd.hasOption("f")) {
                gndCsvFolder = cmd.getOptionValue("f");
            }

            if (cmd.hasOption("t")) {
                try {
                    numberOfThreats = Integer.parseInt(cmd.getOptionValue("t"));
                } catch (NumberFormatException e) {
                    // nothing
                }
            }

            if (cmd.hasOption("m")) {
                try {
                    maxSubmittedTasks = Integer.parseInt(cmd.getOptionValue("m"));
                } catch (NumberFormatException e) {
                    // nothing
                }
            }

            if (cmd.hasOption("l")) {
                final String[] threadCount = cmd.getOptionValue("l").split(",");
                final Set<String> lang = new HashSet<>(Arrays.asList(threadCount));
                if (!lang.isEmpty()) {
                    EFDExecutor.setLANGUAGES(lang);
                }
            }

            if (cmd.hasOption("o")) {
                outputFile = cmd.getOptionValue("o");
                if (!outputFile.contains("{LANG}")) {
                    if (outputFile.contains(".")) {
                        if (EFDExecutor.getLANGUAGES().size() > 1) {
                            outputFile = new StringBuilder(outputFile).insert(outputFile.lastIndexOf('.'), "-{LANG}").toString();
                        } else {
                            outputFile = new StringBuilder(outputFile).toString();
                        }
                    } else {
                        if (EFDExecutor.getLANGUAGES().size() > 1) {
                            outputFile += "-{LANG}";
                        }
                    }
                }
            }

        } catch (ParseException ex) {
            final HelpFormatter help = new HelpFormatter();
            help.printHelp("java -jar efdump.jar [-i <file> | -f <folder>] [-l <language>] [-o {TIMESTAMP}-EFDump-{LANG}.json]", options);
            System.exit(1);
        }
        File[] files;
        if (!gndCsvFile.isEmpty()) {
            files = new File[1];
            files[0] = new File(gndCsvFile);

        } else {
            final File dir = new File(gndCsvFolder);
            files = dir.listFiles((File d, String name) -> name.toLowerCase(Locale.GERMAN).endsWith(".csv"));

            if (files == null || files.length < 1) {
                LOG.error("No CSV file(s) in folder {} found.", gndCsvFolder);
                System.exit(1);
            }
        }

        LOG.info("Start with the folowing parameter...");
        LOG.info("CSV files: {}", Arrays.toString(files));
        LOG.info("File name of output file: {}", outputFile);
        LOG.info("Language(s) to dump: {}", EFDExecutor.getLANGUAGES());

        try {
            EFDExecutor.getEFDExecutor().init(files, outputFile, numberOfThreats, maxSubmittedTasks);
            EFDExecutor.getEFDExecutor().makeDump();
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            System.exit(1);
        }

        LOG.info("Done. Bye!");
    }
}
