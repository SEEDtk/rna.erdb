/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.theseed.clusters.Similarity;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;

/**
 * This report counts the non-operon correlations at various user-specified levels.  This is
 * a text-format report.
 *
 * @author Bruce Parrello
 *
 */
public class NonOperonPairReporter extends FeatureCorrReporter {

    // FIELDS
    /** list of tracking levels, sorted from lowest to highest */
    private double[] levels;
    /** base genome ID */
    private String genomeId;

    public NonOperonPairReporter(IParms processor) {
        super(processor);
        // Get the tracking levels.
        this.levels = processor.getLevels();
        // Sort them for our use.
        Arrays.sort(levels);
        // Save the base genome ID.
        this.genomeId = processor.getGenomeId();
    }

    @Override
    protected void writeReport(Iterator<Similarity> iter, DbConnection rnaDb, OutputStream stream)
            throws SQLException, IOException {
        // Create a counter for each tracking level.
        int[] counters = new int[this.levels.length];
        // Create a counter for all untracked pairs, all same-operon pairs, and for the total.
        int untracked = 0;
        int total = 0;
        int skipped = 0;
        // We need to get the operon ID for each feature.
        Map<String, String> operonMap = this.getOperonMap(rnaDb);
        // Loop through all the pairs, counting.
        while (iter.hasNext()) {
            Similarity sim = iter.next();
            String op1 = operonMap.get(sim.getId1());
            String op2 = operonMap.get(sim.getId2());
            total++;
            if (op1 != null && op2 != null && op1.contentEquals(op2)) {
                // Skip features in the same operon.  (This is a very tiny percentage, but it matters.
                skipped++;
            } else {
                // Here we have a correlation we care about.  It is tracked in the first
                // category with the lower correlation level, starting from the end of the
                // level array.
                double score = sim.getScore();
                boolean found = false;
                int idx = this.levels.length - 1;
                while (idx >= 0 && ! found) {
                    if (score >= this.levels[idx]) {
                        found = true;
                        counters[idx]++;
                    } else
                        idx--;
                }
                if (! found) {
                    // Here the correlation does not match any of the specified levels.
                    untracked++;
                }
            }
            if (log.isInfoEnabled() && total % 100000 == 0)
                log.info("{} pairs processed, {} skipped, {} untracked.", total, skipped, untracked);
        }
        // Now we have counted all the correlations.  Produce the report.
        try (PrintWriter writer = new PrintWriter(stream)) {
            writer.println("lower_limit\tpairs");
            for (int idx = this.levels.length - 1; idx >= 0; idx--)
                writer.format("%6.4f\t%d%n", this.levels[idx], counters[idx]);
            if (untracked > 0)
                writer.format("%6.4f\t%d%n", -1.0, untracked);
            writer.println();
            writer.format("%d pairs in operons.%n", skipped);
            writer.format("%d total pairs.", total);
        }
    }

    /**
     * Compute the operon for each feature we might encounter.
     *
     * @param rnaDb		RNA expression database connection
     *
     * @return a map from feature ID to operon ID
     *
     * @throws SQLException
     */
    private Map<String, String> getOperonMap(DbConnection rnaDb) throws SQLException {
        log.info("Retrieving operon IDs from database.");
        var retVal = new HashMap<String, String>(4000);
        // Use a database query to get the operon IDs.
        try (DbQuery query = new DbQuery(rnaDb, "Feature FeatureToGroup FeatureGroup")) {
            query.rel("FeatureGroup.group_type", Relop.EQ);
            query.rel("Feature.genome_id", Relop.EQ);
            query.select("FeatureToGroup", "fig_id", "group_id");
            query.setParm(1, "operon", this.genomeId);
            Iterator<DbRecord> iter = query.iterator();
            while (iter.hasNext()) {
                var record = iter.next();
                retVal.put(record.getString("FeatureToGroup.fig_id"), record.getString("FeatureToGroup.group_id"));
            }
        }
        log.info("{} features found in operons.", retVal.size());
        return retVal;
    }

}
