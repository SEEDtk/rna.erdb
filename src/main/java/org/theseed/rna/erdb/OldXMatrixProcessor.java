/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.MarkerFile;
import org.theseed.java.erdb.DbConnection;
import org.theseed.reports.NaturalSort;
import org.theseed.rna.data.RnaFeature;
import org.theseed.rna.data.RnaFeatureFilter;
import org.theseed.rna.data.RnaFeatureLevelComputer;
import org.theseed.rna.data.RnaSample;

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
 * --qual		minimum acceptable precent quality level of a sample (default 40.0)
 * --min		minimum fraction of samples that must have a value for a feature to be included; the default is 0.80
 * --clear		erase the output directory before processing
 * --filter		filtering rule for peg selection (default SUBSYSTEM)
 * --rep		method for representing the expression data (default TRIAGE)
 * --scale		scale factor to be divided into output measurement (default 1.0)
 * --col		name to give to the output column (default, same as measurement)
 * --file		if specified, the name of a tab-delimited file containing sample IDs in the first column, and measurement values in
 * 				the column with the same name as the measurement
 *
 * @author Bruce Parrello
 *
 */
public class OldXMatrixProcessor extends BaseDbRnaProcessor implements RnaFeatureFilter.IParms,
        RnaFeatureLevelComputer.IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(OldXMatrixProcessor.class);
    /** map of feature IDs to descriptors */
    private Map<String, RnaFeature> fidData;
    /** collection of RNA samples */
    private Collection<RnaSample> samples;
    /** feature filter */
    private RnaFeatureFilter filter;
    /** expression-level computer */
    private RnaFeatureLevelComputer levelComputer;

    // COMMAND-LINE OPTIONS

    /** split point in measurement for low and high samples */
    @Option(name = "--split", metaVar = "2.0", usage = "minimum measurement value for a sample to be considered High")
    private double splitPoint;

    /** quality filter for samples */
    @Option(name = "--qual", metaVar = "80.0", usage = "minimum acceptable mapping quality percentage for a sample")
    private double minQual;

    /** if specified, the output directory will be erased before processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be cleared before processing")
    private boolean clearFlag;

    /** minimum fraction of samples that must have a value for a feature to be used in the xmatrix */
    @Option(name = "--min", metaVar = "0.50", usage = "minimum fraction of valid samples required for a feature")
    private double minFrac;

    /** feature filtering rule */
    @Option(name = "--filter", usage = "rule to use for selecting output features")
    private RnaFeatureFilter.Type filterType;

    /** scale factor for measurement */
    @Option(name = "--scale", usage = "scale factor to divide into output value")
    private double scale;

    /** name to give to output column */
    @Option(name = "--col", metaVar = "production", usage = "name to give to output column")
    private String outCol;

    /** expression-level translation rule */
    @Option(name = "--rep", usage = "translation method for representing expression levels")
    private RnaFeatureLevelComputer.Type levelType;

    /** name of the target measurement */
    @Argument(index = 1, metaVar = "measurement", usage = "measurement to use for model output")
    private String measureName;

    /** name of the output directory */
    @Argument(index = 2, metaVar = "outDir", usage = "output directory name")
    private File outDir;

    // METHODS

    @Override
    protected void setDbDefaults() {
        this.minQual = 40.0;
        this.clearFlag = false;
        this.minFrac = 0.80;
        this.splitPoint = 1.2;
        this.filterType = RnaFeatureFilter.Type.SUBSYSTEMS;
        this.levelType = RnaFeatureLevelComputer.Type.TRIAGE;
        this.scale = 1.0;
        this.outCol = null;
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException, IOException {
        if (this.minFrac <= 0.0 || this.minFrac > 1.0)
            throw new ParseFailureException("Minimum fraction must be between 0 and 1.");
        if (this.minQual < 0.0 || this.minQual > 100.0)
            throw new ParseFailureException("Quality level must be between 0 and 100.");
        if (this.scale == 0.0)
            throw new ParseFailureException("Scale factor cannot be 0.");
        // Default the output column name.
        if (this.outCol == null)
            this.outCol = this.measureName;
        // Create the filter and the level computer.
        this.filter = this.filterType.create(this);
        this.levelComputer = this.levelType.create(this);
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
        // First we read in the samples.  This also validates the measurement name.
        log.info("Loading samples with {} values in {}.", this.measureName, this.getGenomeId());
        this.samples = RnaSample.load(db, this.getGenomeId(), this.measureName, this.minQual, this.scale);
        log.info("{} samples found.", this.samples.size());
        // Now read in the features.
        this.fidData = RnaFeature.loadFeats(db, this.getGenomeId(), this.filter);
        // The next step is to create the set of valid features.  The main filter has already been applied,
        // but we need to remove features with insufficient valid data.
        var minGoodValues = (long) Math.ceil(this.samples.size() * this.minFrac);
        log.info("{} good values required for a valid feature.", minGoodValues);
        // We will sort the set in natural order (basically, by peg number).
        var fidSet = new TreeSet<String>(new NaturalSort());
        // Loop through the feature data.
        for (Map.Entry<String, RnaFeature> featEntry : this.fidData.entrySet()) {
            // Get the array index for this feature.
            int fidIdx = featEntry.getValue().getIdx();
            // Count the number of times it is good.
            long goodValues = this.samples.stream().filter(x -> x.isValid(fidIdx)).count();
            if (goodValues >= minGoodValues)
                fidSet.add(featEntry.getKey());
        }
        log.info("{} features will be present in output.", fidSet.size());
        // Compute the number of output columns.
        final int colCount = fidSet.size() + 3;
        // Now we need to set up the output.  There are several marker files.
        File outFile = new File(this.outDir, "decider.txt");
        MarkerFile.write(outFile, "RandomForest");
        outFile = new File(this.outDir, "labels.txt");
        try (var writer = new PrintWriter(outFile)) {
            writer.println("Low");
            writer.println("High");
        }
        // Next, we assemble the header, which is used in training.tbl and data.tbl.  We have an
        // extra column at the front for the sample ID, then one for the measure value, then
        // one for the output column with the measure level.
        String outHeader = fidSet.stream().map(x -> this.fidData.get(x).getName())
                .collect(Collectors.joining("\t", "sample_id\t",
                        String.format("\t%s\t%s_type", this.outCol, this.outCol)));
        outFile = new File(this.outDir, "training.tbl");
        MarkerFile.write(outFile, outHeader);
        outFile = new File(this.outDir, "data.tbl");
        try (var writer = new PrintWriter(outFile)) {
            writer.println(outHeader);
            // Loop through the samples, writing the column values.
            for (RnaSample sample : this.samples) {
                // The output columns will be built in here.
                var columns = new ArrayList<String>(colCount);
                columns.add(sample.getSampleId());
                // Add the data values.
                for (String fid : fidSet) {
                    var desc = this.fidData.get(fid);
                    var value = this.levelComputer.compute(desc, sample.getValue(desc.getIdx()));
                    columns.add(Double.toString(value));
                }
                // Add the measurement value and the measurement class.
                double output = sample.getOutput();
                columns.add(Double.toString(output));
                String outClass;
                if (output >= this.splitPoint)
                    outClass = "High";
                else
                    outClass = "Low";
                columns.add(outClass);
                writer.println(StringUtils.join(columns, "\t"));
            }
        }
    }


}
