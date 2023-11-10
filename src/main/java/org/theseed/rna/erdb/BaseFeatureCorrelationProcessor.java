/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.util.ResizableDoubleArray;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.clusters.ClusterGroup;
import org.theseed.clusters.methods.ClusterMergeMethod;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;

/**
 * This is a command-processor superclass for computing RNA feature correlations.  The main method is
 * "processCorrelations", which computes the correlations from the database and returns a raw cluster
 * group.
 *
 * The positional parameter is the target genome ID.
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
 * --load		load file for correlations; if specified, it should have the feature IDs in the first
 * 				two columns and the correlation score in the third
 * --save		file in which to save the correlations computed (ignored if --load specified)
 *
 * @author Bruce Parrello
 *
 */
public abstract class BaseFeatureCorrelationProcessor extends BaseDbRnaProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FeatureCorrelationProcessor.class);
    /** number of feature comparisons generated */
    private int compareCount;

    // COMMAND-LINE OPTIONS

    /** optional load file for correlation scores */
    @Option(name = "--load", metaVar = "corrFile.tbl", usage = "file containing correlation scores (if pre-computed)")
    private File loadFile;

    /** optional save file for correlation scores */
    @Option(name = "--save", metaVar = "saveCorr.tbl", usage = "file in which to save the correlation values (ignored if --load present)")
    private File saveFile;

    @Override
    final protected void setDbDefaults() {
        this.loadFile = null;
        this.saveFile = null;
        this.setFeatureCorrDefaults();
    }

    /**
     * Set the subclass defaults.
     */
    protected abstract void setFeatureCorrDefaults();

    @Override
    final protected void validateDbRnaParms() throws ParseFailureException, IOException {
        if (this.loadFile != null && ! this.loadFile.canRead())
            throw new FileNotFoundException("Load file " + this.loadFile + " is not found or unreadable.");
        else if (this.saveFile != null && this.saveFile.exists() && ! this.saveFile.canWrite())
            throw new IOException("Cannot write to specified save file " + this.saveFile + ".");
        this.validateFeatureCorrParms();
    }

    /**
     * Validate the subclass command-line parameters.
     *
     * @throws ParseFailureException
     * @throws IOException
     */
    protected abstract void validateFeatureCorrParms() throws ParseFailureException, IOException;

    /**
     * Create a cluster group primed with the pearson correlations between the features in the
     * RNA expression database.
     *
     * @param db	RNA expression database to process
     *
     * @return a cluster group primed with the feature correlations
     *
     * @throws SQLException
     * @throws IOException
     *
     */
    protected ClusterGroup processCorrelations(DbConnection db) throws SQLException, IOException {
        ClusterGroup retVal;
        if (this.loadFile == null) {
            retVal = this.createClusterGroup();
            // Compute the expression level arrays.
            ResizableDoubleArray[] levels = this.computeLevelArrays(db);
            // We have an array of expression levels for each feature, one column per sample.  We compute
            // the correlation for each sample pair.  Because this could involve millions of comparisons, we
            // parallelize it.
            this.compareCount = 0;
            IntStream.range(0, getFeatureCount()).parallel().forEach(i -> this.compareLevels(retVal, levels, i));
            // Here we may need to save the correlations.
            if (this.saveFile != null)
                retVal.save(this.saveFile);
        } else {
            // Here the correlations are loaded from a file.
            log.info("Loading correlations from {}.", this.loadFile);
            retVal = ClusterGroup.load(this.loadFile, ClusterMergeMethod.COMPLETE);
        }
        return retVal;
    }

    /**
     * @return an empty cluster group sized to this processor's genome
     */
    protected ClusterGroup createClusterGroup() {
        return new ClusterGroup(this.getFeatureCount(), ClusterMergeMethod.COMPLETE);
    }

    /**
     * Create the level arrays and fill them with expression data.
     *
     * @param db	RNA expression database
     *
     * @throws SQLException
     */
    private ResizableDoubleArray[] computeLevelArrays(DbConnection db) throws SQLException {
        // Loop through the features, loading the level arrays.  We will have one level array per feature, which
        // means we are essentially flipping the native representation, which is one array per sample.
        // First, we create one array per feature.
        final ResizableDoubleArray[] retVal = IntStream.range(0, this.getFeatureCount())
                .mapToObj(i -> new ResizableDoubleArray())
                .toArray(ResizableDoubleArray[]::new);
        // Now we loop through the samples, filling the arrays.
        log.info("Retrieving expression levels for {} features in genome {}.", this.getFeatureCount(), this.getGenomeId());
        try (DbQuery query = new DbQuery(db, "RnaSample")) {
            // Ask for all samples for this genome that are NOT suspicious.
            query.select("RnaSample", "feat_data").rel("RnaSample.genome_id", Relop.EQ)
                .rel("RnaSample.suspicious", Relop.EQ).setParm(1, this.getGenomeId())
                .setParm(2, false);
            query.stream().forEach(x -> this.processLevels(retVal, x));
        }
        return retVal;
    }

    /**
     * This method retrieves the feature array from a database record and adds its expression levels
     * to the array of feature-level arrays.
     *
     * @param record	RnaSample record to process
     */
    private void processLevels(ResizableDoubleArray[] levelArrays, DbRecord record) {
        try {
            double[] levels = record.getDoubleArray("RnaSample.feat_data");
            for (int i = 0; i < levelArrays.length; i++)
                levelArrays[i].addElement(levels[i]);
        } catch (SQLException e) {
            // Convert the SQL exception to unchecked so we can do this in a stream.
            throw new RuntimeException(e);
        }
    }

    /**
     * Generate all the correlations to the feature at the specified index.
     *
     * @param i		index of the feature to compare to all subsequent features
     */
    private void compareLevels(ClusterGroup clusters, ResizableDoubleArray[] levelArrays, int i) {
        // Get a compute for the pearsons correlations.
        PearsonsCorrelation computer = new PearsonsCorrelation();
        String fidI = this.getFeatureId(i);
        ResizableDoubleArray levelsI = levelArrays[i];
        final int n = levelsI.getNumElements();
        // Now loop through all of the features following this one.
        for (int j = i + 1; j < levelArrays.length; j++) {
            String fidJ = this.getFeatureId(j);
            ResizableDoubleArray levelsJ = levelArrays[j];
            // Create arrays for the matching finite values.
            ResizableDoubleArray arrayI = new ResizableDoubleArray(n);
            ResizableDoubleArray arrayJ = new ResizableDoubleArray(n);
            for (int k = 0; k < n; k++) {
                double valI = levelsI.getElement(k);
                double valJ = levelsJ.getElement(k);
                if (Double.isFinite(valI) && Double.isFinite(valJ)) {
                    arrayI.addElement(valI);
                    arrayJ.addElement(valJ);
                }
            }
            // Compute the correlation.
            double pc = 0.0;
            if (arrayI.getNumElements() > 2)
                pc = computer.correlation(arrayI.getElements(), arrayJ.getElements());
            this.storeCorrelation(clusters, fidI, fidJ, pc);
        }
    }

    /**
     * Store the results of a correlation computation.
     *
     * @param fidI		ID of first feature
     * @param fidJ		ID of second feature
     * @param pc		pearson correlation
     */
    private synchronized void storeCorrelation(ClusterGroup clusters, String fidI, String fidJ, double pc) {
        if (! Double.isFinite(pc)) {
            log.warn("Could not compute correlation between {} and {}.", fidI, fidJ);
            pc = 0.0;
        }
        clusters.addSim(fidI, fidJ, pc);
        this.compareCount++;
        if (log.isInfoEnabled() && this.compareCount % 2000 == 0)
            log.info("{} comparisons computed.", this.compareCount);
    }
}
