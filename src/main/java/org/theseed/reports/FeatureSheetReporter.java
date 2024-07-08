/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;

/**
 * This report contains the basic TPM data for a genome in tabular format.  The output is a giant spreadsheet, which each row being a gene and
 * each column being a good sample.
 *
 * The basic approach for this report is to read in all the samples in lexical order and then loop through the features, extracting the sample
 * data for each row.
 *
 * @author Bruce Parrello
 *
 */
public class FeatureSheetReporter extends BaseRnaDbReporter {

    // FIELDS
    /** genome of interest */
    private String genomeId;

    /**
     * Construct a new feature-sheet reporter.
     *
     * @param processor		controlling command processor
     * @param db			database containing the RNA Seq data
     */
    public FeatureSheetReporter(IParms processor, DbConnection db) {
        super(processor, db);
        this.genomeId = processor.getGenomeId();
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        // Get the RNA Seq database.
        DbConnection db = this.getDb();
        // Our first task is to get the sample data.  We will use a sorted map, keyed by sample ID, with the array of
        // TPM values as the value for each.
        SortedMap<String, double[]> sampleMap = new TreeMap<String, double[]>();
        // We build a query for the samples.
        try (DbQuery query = new DbQuery(db, "RnaSample")) {
            query.select("RnaSample", "sample_id", "feat_data");
            query.rel("RnaSample.suspicious", Relop.EQ);
            query.rel("RnaSample.genome_id", Relop.EQ);
            query.setParm(1, false);
            query.setParm(2, this.genomeId);
            // Get all the samples in memory.
            log.info("Reading samples for genome {}.", this.genomeId);
            Iterator<DbRecord> iter = query.iterator();
            while(iter.hasNext()) {
                DbRecord record = iter.next();
                String sampleId = record.getString("RnaSample.sample_id");
                double[] levels = record.getDoubleArray("RnaSample.feat_data");
                sampleMap.put(sampleId, levels);
            }
            log.info("{} samples saved in memory.", sampleMap.size());
        }
        // Create an output buffer for the print lines.
        StringBuilder buffer = new StringBuilder(12 * sampleMap.size() + 11);
        // Initialize it with the header.
        buffer.append("feature_id");
        sampleMap.keySet().stream().forEach(x -> buffer.append("\t").append(x));
        writer.println(buffer.toString());
        // Now we use a query to loop through the features.
        try (DbQuery query = new DbQuery(db, "Feature")) {
            query.select("Feature", "fig_id", "seq_no");
            query.rel("Feature.genome_id", Relop.EQ);
            query.setParm(1, this.genomeId);
            // Loop through the features, writing lines.
            log.info("Processing features for {}.", this.genomeId);
            Iterator<DbRecord> iter = query.iterator();
            while (iter.hasNext()) {
                DbRecord record = iter.next();
                String fid = record.getString("Feature.fig_id");
                int seqNo = record.getInt("Feature.seq_no");
                // Clear the line buffer.
                buffer.setLength(0);
                // Store the feature ID.
                buffer.append(fid);
                // Loop through the map, filling in the data.  None that an NaN is blanked.
                for (double[] levels : sampleMap.values()) {
                    buffer.append("\t");
                    double level = levels[seqNo];
                    if (! Double.isNaN(level))
                        buffer.append(level);
                }
                // Write the line.
                writer.println(buffer.toString());
            }
        }
    }

}
