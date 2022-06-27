/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;
import org.theseed.rna.data.RnaFeature;
import org.theseed.utils.ParseFailureException;

/**
 * This command builds a random-forest machine learning directory for the RNA data relating to a particular
 * measurement.
 *
 * The positional parameters are the target genome ID, the name of the measurement, and the name of the output directory.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --split		split point for the measurement; values at or above this point are classed "High", all others
 * 				as "Low"; the default is 1.2
 * --all		if specified, questionable samples will be included
 * --min		minimum fraction of samples that must have a value for a feature to be included; the default is 0.80
 * --clear		erase the output directory before processing
 * --filter		filtering rule for peg selection
 * --rep		method for representing the expression data
 *
 * @author Bruce Parrello
 *
 */
public class XMatrixProcessor extends BaseDbRnaProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(XMatrixProcessor.class);
    /** map of sample names to measurement values */
    private Map<String, Double> measureMap;
    /** list of feature descriptors, in presentation order */
    private List<RnaFeature> fidData;
    /** map of sample IDs to expression value arrays; the arrays match the feature descriptor list */
    private Map<String, double[]> expressionMap;

    // COMMAND-LINE OPTIONS

    /** split point in measurement for low and high samples */
    @Option(name = "--split", metaVar = "2.0", usage = "minimum measurement value for a sample to be considered High")
    private double splitPoint;

    /** if specified, all samples are included */
    @Option(name = "--all", usage = "if specified, questionable samples are included")
    private boolean allFlag;

    /** if specified, the output directory will be erased before processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be cleared before processing")
    private boolean clearFlag;

    /** minimum fraction of samples that must have a value for a feature to be used in the xmatrix */
    @Option(name = "--min", metaVar = "0.50", usage = "minimum fraction of valid samples required for a feature")
    private double minFrac;

    /** name of the target measurement */
    @Argument(index = 1, metaVar = "measurement", usage = "measurement to use for model output")
    private String measureName;

    /** name of the output directory */
    @Argument(index = 2, metaVar = "outDir", usage = "output directory name")
    private File outDir;

    // METHODS

    @Override
    protected void setDbDefaults() {
        this.allFlag = false;
        this.clearFlag = false;
        this.minFrac = 0.80;
        this.splitPoint = 1.2;
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException, IOException {
        if (this.minFrac <= 0.0 || this.minFrac > 1.0)
            throw new ParseFailureException("Minimum fraction must be between 0 and 1.");
        // Set up the output directory.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        } else if (this.clearFlag) {
            log.info("Erasing output directory {}.", this.outDir);
            FileUtils.cleanDirectory(this.outDir);
        } else
            log.info("Output directory is {}.", this.outDir);
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        // First we must get all the measurement values.  This validates the measurement name and
        // gives us our sample list.
        this.buildMeasureMap();
        // Now read in the features.
        this.buildFeatureMap();
        // TODO produce output

    }

    /**
     *
     */
    private void buildFeatureMap() {
        // TODO code for buildFeatureMap

    }

    /**
     *
     */
    private void buildMeasureMap() {
        // TODO code for buildMeasureMap

    }

}
