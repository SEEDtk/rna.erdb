/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.stream.IntStream;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;

/**
 * This report evaluates the probability that the distribution of expression levels for each feature
 * is NOT a normal distribution.  It also outputs the skewness, kurtosis, mean, and standard
 * deviation for each feature's expression data.
 *
 * @author Bruce Parrello
 *
 */
public class NormalCheckReporter extends BaseRnaDbReporter {


    public NormalCheckReporter(IParms processor, DbConnection db) {
        super(processor, db);
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        // Set up the feature index.
        log.info("Building feature index.");
        FeatureIndex fIndex = this.getFeatureIndex();
        final int n = fIndex.getFeatureCount();
        log.info("Building statistics computers.");
        // Allocate the statistics array.
        var statsArray = IntStream.range(0, n).mapToObj(i -> new DescriptiveStatistics())
                .toArray(DescriptiveStatistics[]::new);
        // Now we loop through the samples, updating the stats computers.
        try (DbQuery query = new DbQuery(this.getDb(), "RnaSample")) {
            // We want the feature-data arrays, but only for this genome's non-suspicious samples.
            query.select("RnaSample", "feat_data");
            query.rel("RnaSample.genome_id", Relop.EQ);
            query.rel("RnaSample.suspicious", Relop.EQ);
            query.setParm(1, this.getGenomeId());
            query.setParm(2, false);
            // Loop through all the samples.
            int count = 0;
            Iterator<DbRecord> iter = query.iterator();
            while (iter.hasNext()) {
                DbRecord record = iter.next();
                double[] values = record.getDoubleArray("RnaSample.feat_data");
                IntStream.range(0, fIndex.getFeatureCount()).parallel().filter(i -> Double.isFinite(values[i]))
                        .forEach(i -> statsArray[i].addValue(values[i]));
                count++;
                if (log.isInfoEnabled() && count % 100 == 0)
                    log.info("{} samples processed.", count);
            }
            log.info("{} samples processed.", count);
        }
        // Now we have filled in all the descriptive-statistics arrays.  For each one, we can compute its
        // statistical values.  We will need a K-S test computation engine.
        var ksTester = new KolmogorovSmirnovTest();
        // Start the report output.
        log.info("Writing output report.");
        writer.println("fig_id\tbaseline\tgene_name\talias\tmean\tstd_dev\tskewness\tkurtosis\tKS_p_value\tassignment");
        // Loop through the features.
        int count = 0;
        for (int i = 0; i < n; i++) {
            FeatureData feat = fIndex.getFeature(i);
            String fid = feat.getFid();
            var stats = statsArray[i];
            // Get the basic statistics.
            double mean = stats.getMean();
            double sdev = stats.getStandardDeviation();
            double skew = stats.getSkewness();
            double kurt = stats.getKurtosis();
            // Get the gene info.
            String gene = feat.getGene();
            if (gene == null) gene = "";
            String alias = feat.getAlias();
            if (alias == null) alias = "";
            // Compute the distribution error.
            NormalDistribution dist = new NormalDistribution(mean, sdev);
            double kspv = ksTester.kolmogorovSmirnovTest(dist, stats.getValues());
            // Now write all this out.
            writer.format("%s\t%8.2f\t%s\t%s\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%8g\t%s%n", fid, feat.getBaseline(), gene,
                    alias, mean, sdev, skew, kurt, kspv, feat.getAssignment());
            count++;
            if (log.isInfoEnabled() && count % 50 == 0)
                log.info("{} features processed.", count);
        }
    }

}
