/**
 *
 */
package org.theseed.reports;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.counters.WeightMap;
import org.theseed.io.TabbedLineReader;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;
import org.theseed.utils.ParseFailureException;
import org.theseed.utils.StringPair;

/**
 * This report is a simple tab-delimited summary of the nontrivial sample clusters (those with at least two
 * members).  For each cluster, it will report the central member ID (the one with the shortest total distance
 * to others), the number of members, and various statistical values.
 *
 * @author Bruce Parrello
 *
 */
public class SampleClusterReporter extends BaseRnaDbReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleClusterReporter.class);
    /** map of sample ID pairs to similarity scores */
    private Map<StringPair, Double> scoreMap;
    /** ID of current cluster */
    private String clusterId;
    /** members of current cluster */
    private NavigableSet<String> members;

    /**
     * Construct a sample cluster report.
     *
     * @param processor	controlling command processor
     * @param db		RNA expression database
     *
     * @throws ParseFailureException
     * @throws IOException
     */
    public SampleClusterReporter(IParms processor, DbConnection db) throws ParseFailureException, IOException {
        super(processor, db);
        // Get the sample correlation file.
        File sCorrFile = processor.getSampleCorrFile();
        if (sCorrFile == null)
            throw new ParseFailureException("Sample correlation file is required for this report type.");
        if (! sCorrFile.canRead())
            throw new FileNotFoundException("Sample correlation file " + sCorrFile + " is not found or unreadable.");
        // Estimate the hash size from the file size.
        int hashSize = (int) (sCorrFile.length() / 20);
        if (hashSize <= 0)
            throw new IOException("Number of samples is too high for this report (maximum is roughly 40,000.");
        this.scoreMap = new HashMap<StringPair, Double>(hashSize);
        // Read in the correlation hash.  The first two columns are the IDs and the third is the similarity.
        try (TabbedLineReader inStream = new TabbedLineReader(sCorrFile)) {
            int linesIn = 0;
            long lastMsg = System.currentTimeMillis();
            for (var line : inStream) {
                linesIn++;
                StringPair pair = new StringPair(line.get(0), line.get(1));
                this.scoreMap.put(pair, line.getDouble(2));
                if (log.isInfoEnabled() && System.currentTimeMillis() - lastMsg >= 10000) {
                    lastMsg = System.currentTimeMillis();
                    log.info("{} correlations read.", linesIn);
                }
            }
            log.info("{} correlations stored.", linesIn);
        }
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        // Start with the header line.
        writer.println("cluster_id\tcenter\tsize\tmin\tmean\tmax\tsdev");
        // Our basic strategy is to loop through the samples by cluster, writing a line for each one.
        try (DbQuery clusters = new DbQuery(this.getDb(), "RnaSample")) {
            // Set up the parameters to get only clusters for the source genome ID.
            clusters.rel("RnaSample.genome_id", Relop.EQ);
            clusters.setParm(1, this.getGenomeId());
            // Specify the fields to return and the ordering.
            clusters.select("RnaSample", "cluster_id", "sample_id");
            clusters.orderBy("RnaSample.cluster_id");
            // Count the samples we find.
            int sampCount = 0;
            // Ask for an iterator to run the query.
            Iterator<DbRecord> iter = clusters.iterator();
            this.clusterId = "";
            this.members = new TreeSet<String>();
            while (iter.hasNext()) {
                DbRecord record = iter.next();
                // Get this cluster ID and sample ID.
                String newClusterId = record.getString("RnaSample.cluster_id");
                String newMember = record.getString("RnaSample.sample_id");
                // Skip unclustered samples (these are usually also suspicious.
                if (newClusterId != null) {
                    if (! this.clusterId.contentEquals(newClusterId)) {
                        // We have a new cluster.  Output the old cluster and set up the new one.
                        this.writeCluster(writer);
                        this.clusterId = newClusterId;
                        this.members.clear();
                    }
                    this.members.add(newMember);
                    sampCount++;
                }
            }
            // Insure the residual cluster is described.
            this.writeCluster(writer);
            if (sampCount == 0)
                throw new SQLException("This database does not appear to have sample-clustering data for genome "
                        + this.getGenomeId() + ".");
        }
    }

    /**
     * Write the data for the current cluster to the report.
     *
     * @param writer	output print writer for the report
     *
     * @throws IOException
     */
    private void writeCluster(PrintWriter writer) throws IOException {
        // Only proceed if the cluster is nontrivial.
        if (this.members.size() >= 2) {
            // We need a map to track total similarity for each sample.
            WeightMap simTotals = new WeightMap();
            // We need a statistical analyzer for the similarity set.
            SummaryStatistics stats = new SummaryStatistics();
            // Get the similarities for each member.  For each member, we get the similarities to all the others.
            // This is double the data we need for the stats, but it doesn't change the results.
            for (String member1 : this.members) {
                for (String member2 : this.members) {
                    // Skip cases where both sample IDs are the same.
                    if (! member1.contentEquals(member2)) {
                        StringPair pair = new StringPair(member1, member2);
                        Double dist = this.scoreMap.get(pair);
                        if (dist == null)
                            throw new IOException("Correlation between " + member1 + " and " + member2
                                    + " missing from correlation file.");
                        // Record this distance.
                        stats.addValue(dist);
                        simTotals.count(member1, dist);
                    }
                }
            }
            // Get the ID of the center sample.  One will exist because the existence of two members
            // indicates at least one similarity score.
            var center = simTotals.getBestEntry();
            // Write the results.
            writer.println(this.clusterId + "\t" + center.getKey() + "\t" + this.members.size()
                    + "\t" + stats.getMin() + "\t" + stats.getMean() + "\t" + stats.getMax()
                    + "\t" + stats.getStandardDeviation());
        }
    }

}
