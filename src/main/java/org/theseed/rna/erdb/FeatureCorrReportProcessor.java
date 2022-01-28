/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.clusters.ClusterGroup;
import org.theseed.java.erdb.DbConnection;
import org.theseed.reports.FeatureCorrReporter;
import org.theseed.utils.FloatList;
import org.theseed.utils.ParseFailureException;

/**
 * This command computes feature correlations (or optionally loads them from a file) and
 * writes a report.  The positional parameters are the target genome ID and the report type.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file for the report (if not STDOUT)
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --load		load file for correlations; if specified, it should have the feature IDs in the first
 * 				two columns and the correlation score in the third
 * --save		file in which to save the correlations computed (ignored if --load specified)
 * --levels		comma-delimited list of correlation levels to track in the report, from highest to lowest
 *
 * @author Bruce Parrello
 *
 */
public class FeatureCorrReportProcessor extends BaseFeatureCorrelationProcessor implements FeatureCorrReporter.IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FeatureCorrReportProcessor.class);
    /** list of levels to track */
    private FloatList levels;
    /** report writer */
    private FeatureCorrReporter reporter;
    /** output stream */
    private OutputStream outStream;

    // COMMAND-LINE OPTIONS

    /** output file (if not STDOUT) */
    @Option(name = "--output", aliases = { "-o" }, metaVar = "outfile.tbl", usage = "output file (if not STDOUT)")
    private File outFile;

    /** comma-delimited string of correlation levels to track */
    @Option(name = "--levels", metaVar = "0.66,0.80", usage = "comma-delimited list of correlation levels to track")
    private void setLevels(String levelString) {
        this.levels = new FloatList(levelString);
    }

    /** report type */
    @Argument(index = 1, usage = "type of report to write")
    private FeatureCorrReporter.Type reportType;

    @Override
    protected void setFeatureCorrDefaults() {
        this.levels = new FloatList(0.66, 0.80);
    }

    @Override
    protected void validateFeatureCorrParms() throws ParseFailureException, IOException {
        // Insure that all the cutoffs are between -1 and 1.
        double[] levelValues = this.levels.getValues();
        for (double levelValue : levelValues) {
            if (levelValue < -1.0 || levelValue > 1.0)
                throw new ParseFailureException("Tracking levels must be between -1 and 1.");
        }
        // Set up the output stream.
        if (this.outFile == null) {
            log.info("Output will be to the standard output.");
            this.outStream = System.out;
        } else {
            log.info("Output will be to {}.", this.outFile);
            this.outStream = new FileOutputStream(this.outFile);
        }
        // Create the reporter.
        this.reporter = this.reportType.create(this);
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        try {
            // Get the index of features for the RNA expression arrays.
            this.buildFeatureIndex(db);
            // Tell the reporter about the database.
            this.reporter.setDb(db);
            // Set up the cluster group.
            ClusterGroup clusters = this.processCorrelations(db);
            // Write the report.
            log.info("Producing report.");
            this.reporter.runReport(clusters);
        } finally {
            // Insure the output file is closed.
            if (this.outFile != null)
                this.outStream.close();
        }
    }

    @Override
    public OutputStream getStream() {
        return this.outStream;
    }

    @Override
    public double[] getLevels() {
        return this.levels.getValues();
    }

}
