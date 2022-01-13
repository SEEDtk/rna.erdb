/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;
import org.theseed.reports.BaseRnaDbReporter;
import org.theseed.utils.BaseReportProcessor;
import org.theseed.utils.ParseFailureException;

/**
 * This is a general command for producing database reports.
 *
 * The first positional parameters is the target genome ID and the second is the report type.
 *
 * The built-in command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o 	output file for report (if not STDOUT)
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --sample		sample ID for GENE_DATA report
 *
 * @author Bruce Parrello
 *
 */
public class DbRnaReportProcessor extends BaseDbRnaProcessor implements BaseRnaDbReporter.IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BaseReportProcessor.class);
    /** output stream */
    private OutputStream outStream;

    // COMMAND-LINE OPTIONS

    /** output file (if not STDOUT) */
    @Option(name = "-o", aliases = { "--output" }, usage = "output file for report (if not STDOUT)")
    private File outFile;

    /** type of report to output */
    @Argument(index = 1, metaVar = "reportType", usage = "type of report to produce")
    private BaseRnaDbReporter.Type reportType;

    /** ID of sample to use as focus for the report */
    @Option(name = "--sample", metaVar = "SRR10101", usage = "sample ID to use as report focus")
    private String sampleId;

    @Override
    protected final void setDbDefaults() {
        this.outFile = null;
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException, IOException {
        if (this.outFile == null) {
            log.info("Output will be to the standard output.");
            this.outStream = System.out;
        } else {
            log.info("Output will be to {}.", this.outFile);
            this.outStream = new FileOutputStream(this.outFile);
        }
    }

    @Override
    protected final void runDbCommand(DbConnection db) throws Exception {
        try (PrintWriter writer = new PrintWriter(this.outStream)) {
            BaseRnaDbReporter reporter = this.reportType.create(this, db);
            reporter.writeReport(writer);
        } finally {
            // Insure the output stream is closed.
            if (this.outStream != null)
                this.outStream.close();
        }
    }

    /**
     * @return the focus sample ID
     */
    @Override
    public String getSampleId() {
        return this.sampleId;
    }


}
