/**
 *
 */
package org.theseed.rna.baseline;

import java.sql.SQLException;

import org.theseed.java.erdb.DbRecord;

/**
 * This is the base class for a baseline computer.  The baseline computer takes as input
 * the RnaSample records, one after the other, and eventually outputs an array of feature
 * baseline values.  This can then be interrogated to update the baseline values in the
 * genome's feature records.
 *
 * @author Bruce Parrello
 *
 */
public abstract class DbBaselineComputer {

    /**
     * This interface must be supported by the controlling command-line processor, and is
     * used to ask for additional parameters.
     */
    public interface IParms {

    }

    /**
     * This enumeration describes the different types of baseline computers.
     */
    public static enum Type {
        WEIGHTED {
            @Override
            public DbBaselineComputer create(IParms processor) {
                return new WeightedBaselineComputer(processor);
            }
        };

        /**
         * @return a baseline computer of this type
         *
         * @param processor		controlling command processor
         *
         * @throws SQLException
         */
        public abstract DbBaselineComputer create(IParms processor) throws SQLException;

    }

    /**
     * Accumulate data for a particular sample.
     *
     * @param sample	DbRecord for the sample in question
     *
     * @throws SQLException
     */
    public abstract void processRecord(DbRecord sample) throws SQLException;

    /**
     * @return the computed baselines
     */
    public abstract double[] getBaselines();

}
