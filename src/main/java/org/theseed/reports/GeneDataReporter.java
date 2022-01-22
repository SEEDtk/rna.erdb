/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbRecord;

/**
 * This report generates a gene data load file from a single RNA sequence data
 * expression sample.  The load file will contain the gene alias and the expression
 * level for each gene in the sample.
 *
 * For compatibility with Escher maps, the output will be a CSV file, not a tab-
 * delimited file.
 *
 * @author Bruce Parrello
 *
 */
public class GeneDataReporter extends BaseRnaDbReporter {

    // FIELDS
    /** focus sample ID */
    private String sampleId;
    /** gene filter (null to use all genes) */
    private Set<String> geneFilter;

    public GeneDataReporter(IParms processor, DbConnection db) {
        super(processor, db);
        // Get the ID of the target sample.
        this.sampleId = processor.getSampleId();
        // Get the gene filter.
        this.geneFilter = processor.getGeneFilter();
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        log.info("Writing gene data for sample {}.", this.sampleId);
        // Get the feature index.  This will allow us to compute the b-number
        // (alias) for each entry in the sample's expression array.
        FeatureIndex fIndex = this.getFeatureIndex();
        // Now get the sample record for the focus sample.
        DbRecord sample = this.getDb().getRecord("RnaSample", this.sampleId);
        if (sample == null)
            throw new SQLException("Specified sample \"" + this.sampleId + "\" not found.");
        // We have the sample record.  Start the output CSV.
        writer.println("gene,tpm");
        // Get the expression-level array.
        double[] levels = sample.getDoubleArray("RnaSample.feat_data");
        // We loop through the expression levels in parallel with the feature index.
        for (int i = 0; i < levels.length; i++) {
            double level = levels[i];
            if (Double.isFinite(level)) {
                String alias = fIndex.getFeature(i).getAlias();
                if (! StringUtils.isBlank(alias) &&
                        (this.geneFilter == null || this.geneFilter.contains(alias)))
                    writer.format("%s,%6.2f%n", alias, level);
            }
        }
    }

}
