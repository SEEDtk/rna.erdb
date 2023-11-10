/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.Arrays;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.Relop;
import org.theseed.samples.SampleId;

/**
 * This command uses a big production table to fill in missing values for a measurement file.  The existing
 * measurement file is read in an echoed to the report.  Then the big production table is processed.  If
 * a sample does not exist in the measurement file and does exist in the database, its measurement values
 * will be added to the end of the output file.
 *
 * The positional parameters are the target genome ID, the project ID, the name of the big production table,
 * and the name of a control file.  The control file should be tab-delimited with headers.  The first column
 * contains column names from the big production table and the second the corresponding measurement name for
 * the measurement file.  The standard input should be the existing measurement file and the new measurement
 * table will be written to the standard output.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	input file containing existing measurements (if not STDIN)
 * -o 	output file to contain the updated measurement file (if not STDOUT)
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 *
 * @author Bruce Parrello
 *
 */
public class MeasureFixProcessor extends BaseDbRnaProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(MeasureFixProcessor.class);
    /** input file stream */
    private TabbedLineReader inStream;
    /** output file writer */
    private PrintWriter writer;
    /** column mapping from big production table to output; each entry contains a production table index and
     * an output index */
    private List<int[]> colMap;
    /** production table input stream */
    private TabbedLineReader prodStream;
    /** sample ID column in big production table */
    private int sampleIdx;

    // COMMAND-LINE OPTIONS

    /** input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "inFile.tbl", usage = "input file (if not STDIN)")
    private File inFile;

    /** output file (if not STDOUT) */
    @Option(name = "--output", aliases = { "-o" }, usage = "output file for report (if not STDOUT)")
    private File outFile;

    /** ID of the target project */
    @Argument(index = 1, metaVar = "PROJECT_ID", usage = "project ID for the samples", required = true)
    private String projectId;

    /** name of the big production table file */
    @Argument(index = 2, metaVar = "big_production.tbl", usage = "master production file containing measurements",
            required = true)
    private File bigProdFile;

    /** name of the control file */
    @Argument(index = 3, metaVar = "columnMap.tbl", usage = "control file containing column mappings",
            required = true)
    private File controlFile;

    @Override
    protected void setDbDefaults() {
        this.inFile = null;
        this.outFile = null;
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException, IOException {
        // Denote we have no open streams.
        this.writer = null;
        this.prodStream = null;
        this.inStream = null;
        // Denote we are not complete.
        boolean done = false;
        // A *lot* of things can go wrong here, so we protect ourselves and insure we can close the files even
        // if we never get to the run step.
        try {
            // Validate the big production table.
            if (! this.bigProdFile.canRead())
                throw new FileNotFoundException("Big production file " + this.bigProdFile + " is not found or unreadable.");
            // Validate the control file.
            if (! this.controlFile.canRead())
                throw new FileNotFoundException("Control file " + this.controlFile + " is not found or unreadable.");
            // Open the output stream.
            if (this.outFile == null) {
                log.info("Writing new measurement file to standard output.");
                this.writer = new PrintWriter(System.out);
            } else {
                log.info("Writing new measurement file to {}.", this.outFile);
                this.writer = new PrintWriter(this.outFile);
            }
            // Open the input stream.
            if (this.inFile == null) {
                log.info("Reading old measurement file from standard input.");
                this.inStream = new TabbedLineReader(System.in);
            } else if (! this.inFile.canRead())
                throw new FileNotFoundException("Input file " + this.inFile + " is not found or unreadable.");
            else {
                log.info("Reading old measurement file from {}.", this.inFile);
                this.inStream = new TabbedLineReader(this.inFile);
            }
            // Open the big production table and get the sample ID column index.
            this.prodStream = new TabbedLineReader(this.bigProdFile);
            this.sampleIdx = this.prodStream.findField("sample");
            // Now we process the control file.  Generally, there are a very small number of mappings, and
            // they go in the "colMap" list.  Conceivably this could be streamed, but for clarity we use a loop.
            this.colMap = new ArrayList<int[]>();
            try (var controlStream = new TabbedLineReader(this.controlFile)) {
                for (var line : controlStream) {
                    int prodCol = this.prodStream.findField(line.get(0));
                    int outCol = this.inStream.findField(line.get(1));
                    this.colMap.add(new int[] { prodCol, outCol });
                }
            }
            // Denote we are complete.
            done = true;
        } finally {
            // If we are aborting and not complete, close the files.
            if (! done) {
                if (this.writer != null) this.writer.close();
                if (this.prodStream != null) this.prodStream.close();
                if (this.inStream != null) this.inStream.close();
            }
        }
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        try {
            // The first task is to copy the input stream to the output, remembering the sample IDs
            // for which we already have measurements.
            int copyCount = 0;
            Set<String> oldSet = new HashSet<String>(500);
            log.info("Copying existing measurements to output.");
            writer.println(this.inStream.header());
            for (var line : this.inStream) {
                writer.println(line.toString());
                oldSet.add(line.get(0));
                copyCount++;
            }
            log.info("{} lines copied, {} sample IDs found.", copyCount, oldSet.size());
            // Now we need to find all the samples from the database.  The tricky part here is that the
            // measurements in the big production table are stored by sample ID, but the sample ID from
            // the measurements may contain a replication suffix.  We therefore map each basic sample ID
            // to all the replicates.  Samples
            Map<String, List<String>> samplesMap = new HashMap<String, List<String>>(1000);
            int sampleCount = 0;
            int newCount = 0;
            try (DbQuery query = new DbQuery(db, "RnaSample")) {
                query.rel("RnaSample.genome_id", Relop.EQ);
                query.rel("RnaSample.project_id", Relop.EQ);
                query.select("RnaSample", "sample_id");
                query.setParm(1, this.getGenomeId());
                query.setParm(2, this.projectId);
                var iter = query.iterator();
                while (iter.hasNext()) {
                    var record = iter.next();
                    var raw_id = record.getString("RnaSample.sample_id");
                    sampleCount++;
                    // If the sample is already measured, don't bother.
                    if (! oldSet.contains(raw_id)) {
                        // Get the true sample ID we'd find in the production table.
                        SampleId sample = new SampleId(raw_id);
                        String trueSampleId = sample.repBaseId();
                        List<String> samples = samplesMap.computeIfAbsent(trueSampleId, x -> new ArrayList<String>(5));
                        samples.add(raw_id);
                        newCount++;
                    }
                }
                log.info("{} samples found in project. {} measurements needed with {} unique sample IDs.", sampleCount,
                        newCount, samplesMap.size());
            }
            if (sampleCount == 0)
                throw new ParseFailureException("No samples found in project.  Check the project ID.");
            // Now we read the big production table and add the new measurements.  The following array will be used
            // to hold the output lines.
            String[] newLine = new String[this.inStream.size()];
            Arrays.fill(newLine, "");
            int bigProdIn = 0;
            int bigProdUsed = 0;
            int measureOut = 0;
            for (var line : this.prodStream) {
                String sampleId = line.get(this.sampleIdx);
                bigProdIn++;
                // Check for the sample in the map.
                List<String> rnaSamples = samplesMap.get(sampleId);
                if (rnaSamples != null) {
                    bigProdUsed++;
                    // Set up the output line by copying the production-table values.
                    this.colMap.stream().forEach(x -> newLine[x[1]] = line.get(x[0]));
                    // Output the values found for each RNA sample associated with this sample ID.
                    for (String rnaSample : rnaSamples) {
                        newLine[0] = rnaSample;
                        writer.println(StringUtils.join(newLine, '\t'));
                        measureOut++;
                    }
                }
            }
            log.info("{} lines read, {} lines used, {} measurements output.", bigProdIn, bigProdUsed, measureOut);
        } finally {
            // Close all the files.
            this.inStream.close();
            this.writer.close();
            this.prodStream.close();
        }
    }

}
