/**
 *
 */
package org.theseed.rna.baseline;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.theseed.java.erdb.DbRecord;

/**
 * This baseline computer takes the mean of each sample cluster and returns the mean of all the sample
 * clusters as the baseline.  The net effect is to give each cluster equal weight regardless of size.
 * The resulting baseline represents expression levels from a diverse population.
 *
 * @author Bruce Parrello
 *
 */
public class WeightedBaselineComputer extends DbBaselineComputer {

    // FIELDS
    /** map of cluster IDs to statistical object arrays */
    private Map<String, SummaryStatistics[]> summMap;
    /** number of features in the arrays */
    private int featCount;

    /**
     * Initialize the baseline computer.
     *
     * @param processor		controlling command processor
     */
    public WeightedBaselineComputer(IParms processor) {
        this.summMap = new HashMap<String, SummaryStatistics[]>(1000);
    }

    @Override
    public void processRecord(DbRecord sample) throws SQLException {
        // Get the sample cluster.
        String clusterId = sample.getString("RnaSample.cluster_id");
        // Only proceed if the sample is clustered.  An unclustered sample is either suspicious or
        // has not been processed yet.
        if (clusterId != null) {
            // Get the expression level array.
            double[] levels = sample.getDoubleArray("RnaSample.feat_data");
            this.featCount = levels.length;
            // Get the current summaries for the cluster and update them.
            SummaryStatistics[] summaries = this.summMap.computeIfAbsent(clusterId, x -> this.initSummaries());
            IntStream.range(0, this.featCount).parallel().filter(i -> Double.isFinite(levels[i]))
                    .forEach(i -> summaries[i].addValue(levels[i]));
        }
    }

    /**
     * @return an array of empty summary-statistics objects
     */
    private SummaryStatistics[] initSummaries() {
        SummaryStatistics[] retVal = new SummaryStatistics[this.featCount];
        IntStream.range(0, this.featCount).forEach(i -> retVal[i] = new SummaryStatistics());
        return retVal;
    }

    @Override
    public double[] getBaselines() {
        // Summarize the results for each feature.
        double[] retVal = IntStream.range(0, this.featCount).parallel().mapToDouble(i -> this.summarize(i)).toArray();
        return retVal;
    }

    /**
     * @return the computed baseline for the indicated feature
     *
     * @param i		index of the feature to summarize
     */
    private double summarize(int i) {
        DescriptiveStatistics computer = new DescriptiveStatistics();
        // For each cluster that has at least one observation, we get the mean from the statistical object.
        for (SummaryStatistics[] statArray : this.summMap.values()) {
            SummaryStatistics stats = statArray[i];
            if (stats.getN() > 0)
                computer.addValue(stats.getMean());
        }
        // Compute the trimean if possible.
        double retVal;
        if (computer.getN() == 0)
            retVal = Double.NaN;
        else if (computer.getN() <= 2)
            retVal = computer.getMean();
        else
            retVal = (computer.getPercentile(25.0) + 2 * computer.getPercentile(50.0) + computer.getPercentile(75.0)) / 4.0;
        return retVal;
    }

}
