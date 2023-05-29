/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;
import org.theseed.rna.data.RnaFeatureFilter;
import org.theseed.rna.data.MeasureFinder;
import org.theseed.rna.data.RnaFeature;
import org.theseed.rna.data.RnaFeatureLevelComputer;
import org.theseed.utils.ParseFailureException;
import org.theseed.reports.NaturalSort;
import org.theseed.reports.XMatrixReporter;

/**
 * This method produces machine learning input from an RNA Seq Expression database.  The goal is to create a file
 * that allows the user to learn the relationships between expression levels and cell characteristics.  For this
 * reason, it is designed to be highly flexible.  The output format, the means of representing the aforementioned
 * characteristics, the subset of features to use, and the means of scaling the expression levels are all configurable.
 *
 * The positional parameters are the ID of the source genome and the name of the output file or directory.
 *
 * The following command-line options are supported.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --clear		clear output directory before processing (for directory-type output)
 * --neg		negative-condition label to use for classification output (default "normal")
 * --filter		feature-filter type (default ALL)
 * --levels		feature level-computer type (default TRIAGE)
 * --finder		method for finding measurements (default FILE)
 * --mFile		file containing measurements (for FILE measurement finder)
 * --min		minimum fraction of samples that must have a value for a feature to be included; the default is 0.80
 * --mCol		index (1-based) or name of measurement column in measurement file (for FILE measurement finder)
 * --format		output type (default CSV)
 * --qual		minimum fraction sample quality level (default 0.40)
 * --outCol		output column name (default "condition")
 *
 * @author Bruce Parrello
 *
 */
public class XMatrixProcessor extends BaseDbRnaProcessor implements RnaFeatureFilter.IParms,
        RnaFeatureLevelComputer.IParms, XMatrixReporter.IParms, MeasureFinder.IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(XMatrixProcessor.class);
    /** RNA feature filter */
    private RnaFeatureFilter featFilter;
    /** RNA feature level computer */
    private RnaFeatureLevelComputer levelComputer;
    /** measurement computer */
    private MeasureFinder measurer;

    // COMMAND-LINE OPTIONS

    /** TRUE if we want to clear the output directory before producing directory output (ignored for type CSV) */
    @Option(name = "--clear", usage = "if specified, the output directory will be cleared before processing")
    private boolean clearFlag;

    /** negative-condition label to use for classification output (ignored for types REGRESSION, CSV) */
    @Option(name = "--neg", metaVar = "control", usage = "for classifier directory output, name of the negative-condition label")
    private String negLabel;

    /** type of feature filter to use */
    @Option(name = "--filter", usage = "method to use for selecting features to output")
    private RnaFeatureFilter.Type featFilterType;

    /** type of expression-level computer to use */
    @Option(name = "--levels", usage = "method to use for converting expression levels to output values")
    private RnaFeatureLevelComputer.Type levelComputerType;

    /** method to use for computing sample measurements */
    @Option(name = "--finder", usage = "method to use for determining output sample measurements")
    private MeasureFinder.Type measurerType;

    /** input file containing measurements (type FILE only) */
    @Option(name = "--mFile", metaVar = "conditions.tbl", usage = "for finder=FILE, input file to use")
    private File measureFile;

    /** measurement column identifier */
    @Option(name = "--mCol", metaVar = "status", usage = "for finder=FILE, index (1-based) or name of measurement column")
    private String measureCol;

    /** xmatrix output type */
    @Option(name = "--format", usage = "data output format")
    private XMatrixReporter.Type reporterType;

    /** minimum sample quality */
    @Option(name = "--qual", metaVar = "0.80", usage = "minimum fraction of sample with high-quality mappings")
    private double minQual;

    /** minimum fraction of samples that must have a value for a feature to be used in the xmatrix */
    @Option(name = "--min", metaVar = "0.50", usage = "minimum fraction of valid samples required for a feature")
    private double minFrac;

    /** output column name */
    @Option(name = "--outCol", metaVar = "diagnosis", usage = "name for the output column")
    private String outColName;

    /** ID of input genome */
    @Argument(index = 0, metaVar = "genome_id", usage = "ID of source genome for samples", required = true)
    private String genomeId;

    /** name of output file or directory */
    @Argument(index = 1, metaVar = "outDir", usage = "name of output file or directory", required = true)
    private File outDir;


    @Override
    protected void setDbDefaults() {
        this.clearFlag = false;
        this.featFilterType = RnaFeatureFilter.Type.ALL;
        this.levelComputerType = RnaFeatureLevelComputer.Type.TRIAGE;
        this.measurerType = MeasureFinder.Type.FILE;
        this.reporterType = XMatrixReporter.Type.CSV;
        this.measureFile = null;
        this.negLabel = "normal";
        this.measureCol = "2";
        this.minQual = 0.40;
        this.minFrac = 0.80;
        this.outColName = "condition";
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException, IOException {
        // Create the feature filter.
        log.info("Creating feature filter of type {}.", this.featFilterType);
        this.featFilter = this.featFilterType.create(this);
        // Create the level computer.
        log.info("Creating expression-level translator of type {}.", this.levelComputerType);
        this.levelComputer = this.levelComputerType.create(this);
        // Create the measurement finder.
        log.info("Creating measurement loader of type {}.", this.measurerType);
        this.measurer = this.measurerType.create(this);
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        // Get the features to use.  For each we have its column name, its baseline value, and its index in the
        // database records.
        log.info("Loading features from {}.", this.genomeId);
        Map<String, RnaFeature> fidData = RnaFeature.loadFeats(db, this.genomeId, this.featFilter);
        // Get the measurement map.  This not only tells us the output column for each sample, but also which
        // samples to include.
        Map<String, String> measureMap = this.measurer.getMeasureMap();
        log.info("{} samples have measurements.", measureMap.size());
        // Now we need to get all the database records.  We only keep the ones that appear in the measurement map.
        // When we are done, we have to filter out the features with insufficient mappings.  To start, however,
        // we get a mapping from sample IDs to feature value arrays.
        log.info("Loading samples from the database.");
        Map<String, double[]> sampMap = new HashMap<String, double[]>(measureMap.size() * 4 / 3 + 1);
        try (DbQuery query = new DbQuery(db, "RnaSample")) {
            // We only want samples for the target genome with the specified quality level.  In the database,
            // the quality level is a percent, so we have to scale by 100.
            query.rel("RnaSample.genome_id", Relop.EQ);
            query.rel("RnaSample.quality", Relop.GE);
            query.select("RnaSample", "sample_id", "feat_data");
            query.setParm(1, this.genomeId);
            query.setParm(2, this.minQual * 100.0);
            // Now loop through the records, keeping the ones in the measure map.
            long lastMsg = System.currentTimeMillis();
            int inCount = 0;
            int keptCount = 0;
            Iterator<DbRecord> iter = query.iterator();
            while (iter.hasNext()) {
                DbRecord record = iter.next();
                String sampleId = record.getString("RnaSample.sample_id");
                double[] levels = record.getDoubleArray("RnaSample.feat_data");
                inCount++;
                if (measureMap.containsKey(sampleId)) {
                    keptCount++;
                    sampMap.put(sampleId, levels);
                }
                if (System.currentTimeMillis() - lastMsg >= 5000) {
                    log.info("{} samples processed.  {} kept.", inCount, keptCount);
                    lastMsg = System.currentTimeMillis();
                }
            }
            log.info("{} samples stored.", sampMap.size());
        }
        // Now we loop through the features, removing the ones with insufficient data.
        int minFound = (int) Math.ceil(sampMap.size() * minFrac);
        int deletedCol = 0;
        var featIter = fidData.entrySet().iterator();
        while (featIter.hasNext()) {
            RnaFeature featData = featIter.next().getValue();
            int idx = featData.getIdx();
            int found = 0;
            for (double[] sampData : sampMap.values()) {
                if (Double.isFinite(sampData[idx]))
                    found++;
            }
            if (found < minFound) {
                // Here the feature has insufficient data to be useful.
                featIter.remove();
                deletedCol++;
            }
        }
        final int nFeats = fidData.size();
        log.info("{} features deleted due to insufficient mappings.  {} remaining.", deletedCol, nFeats);
        // Now we have the sample data and the final feature list. Set up the output reporter.
        log.info("Creating report object of type {} for {}.", this.reporterType, this.outDir);
        try (var reporter = this.reporterType.create(this, this.outDir)) {
            // First we need to set the headers.  We sort the feature data into an array by feature ID.
            List<RnaFeature> feats = fidData.keySet().stream().sorted(new NaturalSort()).map(x -> fidData.get(x)).collect(Collectors.toList());
            // We will use the above list to produce output.  We need one of just the names to pass to the reporter.
            List<String> fCols = feats.stream().map(x -> x.getName()).collect(Collectors.toList());
            reporter.setHeaders("sample_id", fCols, this.outColName);
            // We now know the number of feature columns, so we can pre-allocate a data array for expression values.
            double[] xValues = new double[nFeats];
            // Now we loop through the sample map.  For each sample, we need to output a line of data.
            // We not only need the data but we need to convert the expression levels to their output form.
            long lastMsg = System.currentTimeMillis();
            int sampCount = 0;
            for (var sampEntry : sampMap.entrySet()) {
                String sampleId = sampEntry.getKey();
                double[] sampData = sampEntry.getValue();
                // Loop through the features.
                for (int i = 0; i < nFeats; i++) {
                    RnaFeature feat = feats.get(i);
                    xValues[i] = this.levelComputer.compute(feat, sampData[i]);
                }
                // Output this row.
                reporter.processRow(sampleId, xValues, measureMap.get(sampleId));
                sampCount++;
                if (log.isInfoEnabled() && System.currentTimeMillis() - lastMsg >= 5000) {
                    log.info("{} samples processed for output.", sampCount);
                    lastMsg = System.currentTimeMillis();
                }
            }
        }
        // All Done.
    }

    @Override
    public boolean getClearFlag() {
        return this.clearFlag;
    }

    @Override
    public String getNegLabel() {
        return this.negLabel;
    }

    @Override
    public File getMeasureFile() {
        return this.measureFile;
    }

    @Override
    public String getMeasureColumn() {
        return this.measureCol;
    }

}
