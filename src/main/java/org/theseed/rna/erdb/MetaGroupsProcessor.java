/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseReportProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.genome.Genome;
import org.theseed.io.TabbedLineReader;
import org.theseed.reports.NaturalSort;

/**
 * This command takes as input one or more files describing groups in a reference genome and assembles them all into
 * a single file that can be supplied as input to the MetaLoadProcessor.
 *
 * Each input file should have three columns, tab-delimited, with headers.  The first column contains the type of grouping
 * (e.g. operon, modulon, regulon), the second contains the group name, and the third contains a comma-delimited list of
 * feature specifications.  Each feature specification is either the suffix of a FIG ID (.e.g "peg.1213"), the full FIG ID,
 * or a known alias.  The input files will be combined into a single output file with full FIG IDs in the first column and
 * the groups of each type in the remaining columns.
 *
 * If no input file is specified, the output will be a file with headers but no data.
 *
 * The positional parameters are the name of the genome's GTO file and the names of the input files.  The output will be to
 * the standard output.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file (if not STDOUT)
 *
 * --skipErrors		if specified, a group with a missing feature is simply skipped
 *
 * @author Bruce Parrello
 *
 */
public class MetaGroupsProcessor extends BaseReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(MetaGroupsProcessor.class);
    /** genome of interest */
    private Genome refGenome;
    /** alias map for the genome */
    private Map<String, Set<String>> aliasMap;
    /** two-level hash mapping fid -> group-type -> group-id set */
    private Map<String, Map<String, Set<String>>> fidMap;
    /** set of group types found */
    private SortedSet<String> typeSet;
    /** pattern to extract feature ID suffix */
    private static final Pattern FID_SUFFIX = Pattern.compile("fig\\|\\d+\\.\\d+\\.([^.]+\\.\\d+)");

    // COMMAND-LINE OPTIONS

    /** skip missing singletons */
    @Option(name = "--skipErrors", usage = "if specified, a group with a missing feature is simply skipped")
    private boolean skipErrors;

    /** name of the reference genome file */
    @Argument(index = 0, metaVar = "refGenome.gto", usage = "GTO for the reference genome", required = true)
    private File gtoFile;

    /** name(s) of the input file (s) */
    @Argument(index = 1, metaVar = "inFile1 inFile2 ...", usage = "names of the input files")
    private List<File> inFiles;

    @Override
    protected void setReporterDefaults() {
        this.inFiles = new ArrayList<File>();
        this.skipErrors = false;
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
        // Verify the genome file.
        if (! this.gtoFile.canRead())
            throw new FileNotFoundException("Genome GTO file " + this.gtoFile + " is not found or unreadable.");
        // Load the genome and build the alias map.
        log.info("Loading reference genome from {}.", this.gtoFile);
        this.refGenome = new Genome(this.gtoFile);
        this.aliasMap = this.refGenome.getAliasMap();
        // Finally, add aliases for the feature ID suffixes.
        for (var feat : this.refGenome.getFeatures()) {
            String fid = feat.getId();
            Matcher m = FID_SUFFIX.matcher(fid);
            if (m.matches())
                this.aliasMap.put(m.group(1), Set.of(fid));
        }
        log.info("{} aliases loaded for {} features in {}.", this.aliasMap.size(), this.refGenome.getFeatureCount(), this.refGenome);
        // Verify that all the input files are readable.
        for (var inFile : inFiles) {
            if (! inFile.canRead())
                throw new FileNotFoundException("Input file " + inFile + " is not found or unreadable.");
        }
    }

    @Override
    protected void runReporter(PrintWriter writer) throws Exception {
        // Create the two-level hash we are going to use to produce the output file.
        this.fidMap = new TreeMap<String, Map<String, Set<String>>>(new NaturalSort());
        // Create the output group-type set.
        this.typeSet = new TreeSet<String>();
        // Now we loop through the input files to fill it.
        for (var inFile : inFiles) {
            log.info("Processing input file {}.", inFile);
            try (TabbedLineReader inStream = new TabbedLineReader(inFile)) {
                int inCount = 0;
                int errCount = 0;
                for (var line : inStream) {
                    inCount++;
                    // Get the group type and name.  The loader prefixes the genome ID, so we just record the raw name.
                    String type = line.get(0);
                    String name = line.get(1);
                    // Save the type.
                    this.typeSet.add(type);
                    // Get the list of feature IDs.  Note we allow commas after the space but not before.
                    String[] featNames = line.get(2).split(",\\s*");
                    // Each feature name must be converted into an ID set using the alias map.
                    for (var featName : featNames) {
                        // If it is a feature ID, just keep it unchanged.  Otherwise, we use the alias map.
                        Set<String> fids;
                        if (featName.startsWith("fig|"))
                            fids = Set.of(featName);
                        else {
                            fids = this.aliasMap.get(featName);
                            if (fids == null) {
                                if (this.skipErrors) {
                                    log.error("Invalid feature specification \"{}\" in group {}.", featName, name);
                                    errCount++;
                                } else
                                    throw new ParseFailureException("Invalid feature specification \"" + featName + "\".");
                            } else {
                                // Assign this group to each of the features found.
                                for (String fid : fids) {
                                    // Find the set for this group type for this feature.
                                    var typeMap = this.fidMap.computeIfAbsent(fid, x -> new TreeMap<String, Set<String>>());
                                    var groupSet = typeMap.computeIfAbsent(type, x -> new TreeSet<String>());
                                    groupSet.add(name);
                                }
                            }
                        }
                    }
                }
                log.info("{} lines read from {}.  Output map contains {} features in {} group types.  {} errors found.", inCount, inFile,
                        this.fidMap.size(), this.typeSet.size(), errCount);
            }
        }
        // Now we create the output file.  The file will have one row for each feature, and one column for each group type.
        // We will build each line in this buffer.
        StringBuilder buffer = new StringBuilder(80);
        // We start with a header build from the group types.
        buffer.append("fig_id");
        this.typeSet.stream().forEach(x -> buffer.append("\t").append(x));
        writer.println(buffer.toString());
        // Now we loop through the features in the master hash.
        for (var fidEntry : this.fidMap.entrySet()) {
            // Store the feature ID.
            buffer.setLength(0);
            buffer.append(fidEntry.getKey());
            // Now loop through the types, writing each set of group names.
            Map<String, Set<String>> typeMap = fidEntry.getValue();
            for (String type : this.typeSet) {
                // Note we will output an empty column if there are no groups of the current type.
                Set<String> groups = typeMap.getOrDefault(type, Collections.emptySet());
                buffer.append("\t").append(StringUtils.join(groups, ','));
            }
            writer.println(buffer.toString());
        }
    }

}
