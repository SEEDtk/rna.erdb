/**
 *
 */
package org.theseed.rna.data;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.Relop;

/**
 * This object contains the raw expression data and the key measurement for a single RNA sample.  It is loaded
 * from a database query.
 *
 * @author Bruce Parrello
 *
 */
public class RnaSample {

    // FIELDS
    /** sample ID */
    private String sampleId;
    /** array of expression levels */
    private double[] levels;
    /** measurement value */
    private double output;

    /**
     * Construct a new rna-sample descriptor.
     *
     * @param id		ID of the sample
     * @param xArray	array of expression level values
     * @param out		output measurement value
     */
    protected RnaSample(String id, double[] xArray, double out) {
        this.sampleId = id;
        this.levels = xArray;
        this.output = out;
    }

    /**
     * Construct a list of the RNA samples from a database query.
     *
     * @param db		database connection
     * @param genomeId	ID of the target genome
     * @param mType		measurement type
     * @param qual		minimum quality level
     * @param scale		scale factor to divide into output value
     *
     * @throws SQLException
     */
    public static Collection<RnaSample> load(DbConnection db, String genomeId, String mType, double qual, double scale) throws SQLException {
        var retVal = new ArrayList<RnaSample>(4000);
        // Create the query.
        try (DbQuery query = new DbQuery(db, "Measurement RnaSample")) {
            // Filter the query for the measurement type and target genome.
            query.select("Measurement", "value");
            query.select("RnaSample", "sample_id", "feat_data");
            query.rel("Measurement.genome_id", Relop.EQ);
            query.rel("Measurement.measure_type", Relop.EQ);
            query.rel("RnaSample.quality", Relop.GE);
            query.setParm(1, genomeId);
            query.setParm(2, mType);
            query.setParm(3, qual);
            // Now loop through the samples found.
            var iter = query.iterator();
            while (iter.hasNext()) {
                var record = iter.next();
                // Scale the output value.
                var output = record.getDouble("Measurement.value");
                if (Double.isFinite(output)) output /= scale;
                // Create the sample.
                var sample = new RnaSample(record.getString("RnaSample.sample_id"),
                        record.getDoubleArray("RnaSample.feat_data"), output);
                retVal.add(sample);
            }
        }
        return retVal;
    }

    /**
     * @return TRUE if the value at the specified feature index is real, else FALSE
     *
     * @param idx	array index of the feature in question
     */
    public boolean isValid(int idx) {
        return Double.isFinite(this.levels[idx]);
    }

    /**
     * @return the expression value at the specified feature index
     *
     * @param idx	array index of the feature in question
     */
    public double getValue(int idx) {
        return this.levels[idx];
    }

    /**
     * @return the sample ID
     */
    public String getSampleId() {
        return this.sampleId;
    }

    /**
     * @return the measurement value
     */
    public double getOutput() {
        return this.output;
    }

}
