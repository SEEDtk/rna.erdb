/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseReportProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.LineReader;
import org.theseed.java.erdb.DbConnection;
import org.theseed.reports.BaseRnaDbReporter;

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
 * -m	type of measurement for MEASURE report
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --sample		sample ID for GENE_DATA report
 * --gFilter	if specified, a CSV containing the genes to include in the GENE_DATA
 * 				report (the default is to include all genes)
 * --sCorr		if specified, the name of a file containing the sample correlations
 * --proj		name of project of interest for MEASURE report
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
    /** gene filter set */
    private Set<String> geneFilter;

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

    /** name of CSV file containing gene filter (first column only) */
    @Option(name = "-gFilter", metaVar = "genes.csv", usage = "CSV file containing genes to output in GENE_DATA report (default is to output all)")
    private File geneFilterFile;

    /** target project ID */
    @Option(name = "--proj", aliases = { "--project" }, metaVar = "PROJ001", usage = "project ID for project-based reports")
    private String projectId;

    /** measurement type of interest */
    @Option(name = "--measure", aliases = { "--mType", "-m" }, metaVar = "thr_g/L", usage = "measurement type to display for measurement reports")
    private String mType;

    /** sample correlation file */
    @Option(name = "--sCorr", metaVar = "clusters.tbl", usage = "sample correlation file for sample-cluster reports")
    private File sampleCorrFile;

    @Override
    protected final void setDbDefaults() {
        this.outFile = null;
        this.geneFilterFile = null;
        this.projectId = null;
        this.mType = "none";
        this.sampleCorrFile = null;
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
        // Check the gene filter file.
        this.geneFilter = null;
        if (this.geneFilterFile != null) {
            // Here we need to create the gene filter.
            try (LineReader geneStream = new LineReader(this.geneFilterFile)) {
                this.geneFilter = new HashSet<String>(100);
                for (String geneLine : geneStream) {
                    String gene = StringUtils.substringBefore(geneLine, ",");
                    this.geneFilter.add(gene);
                }
            }
            log.info("{} genes found in filter file.", this.geneFilter.size());
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

    @Override
    public Set<String> getGeneFilter() {
        return this.geneFilter;
    }

    @Override
    public String getMeasureType() {
        return this.mType;
    }

    @Override
    public String getProjectId() {
        return this.projectId;
    }

    @Override
    public File getSampleCorrFile() {
        return this.sampleCorrFile;
    }


}
