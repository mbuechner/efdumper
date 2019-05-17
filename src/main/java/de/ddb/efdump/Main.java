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

import java.io.File;
import java.io.IOException;
import static java.lang.System.exit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
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

        final Options options = new Options();
        options.addOption("i", true, "CSV file containing one GND-ID in each line.");
        options.addOption("f", true, "Folder with CSV file(s) containing one GND-ID in each line (file name pattern is '*.csv'). Default: " + gndCsvFolder);
        options.addOption("l", true, "Language(s) to dump (comma for separation, e.g. de-DE,en-US). Default: de-DE");
        options.addOption("o", true, "File name of output file. Default: " + outputFile);

        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("i")) {
                gndCsvFile = cmd.getOptionValue("i");
            }

            if (cmd.hasOption("f")) {
                gndCsvFolder = cmd.getOptionValue("f");
            }

            if (cmd.hasOption("o")) {
                outputFile = cmd.getOptionValue("o");
                if (!outputFile.contains("{LANG}")) {
                    if (outputFile.contains(".")) {
                        outputFile = new StringBuilder(outputFile).insert(outputFile.lastIndexOf('.') - 1, "-{LANG}").toString();
                    } else {
                        outputFile += "-{LANG}";
                    }
                }
            }

            if (cmd.hasOption("l")) {
                final String[] threadCount = cmd.getOptionValue("l").split(",");
                final Set<String> lang = new HashSet<>(Arrays.asList(threadCount));
                if (!lang.isEmpty()) {
                    EFDExecutor.setLANGUAGES(lang);
                }
            }
        } catch (ParseException ex) {
            final HelpFormatter help = new HelpFormatter();
            help.printHelp("java -jar efdump.jar [-i <file> | -f <folder>] [-l <language>] [-o {TIMESTAMP}-EFDump-{LANG}.json]", options);
            exit(1);
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
                exit(1);
            }
        }

        LOG.info("Start with the folowing parameter...");
        LOG.info("CSV files: {}", Arrays.toString(files));
        LOG.info("File name of output file: {}", outputFile);
        LOG.info("Language(s) to dump: {}", EFDExecutor.LANGUAGES);

        try {
            final EFDExecutor exe = new EFDExecutor(files, outputFile);

            exe.makeDump();
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            exit(1);
        }

        LOG.info("Done. Bye!");
    }
}
