/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;

/**
 * This is the base class for RNA database reports.  Because these reports are extremely varied, there is
 * only setup and a main method.
 *
 * @author Bruce Parrello
 *
 */
public abstract class BaseRnaDbReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BaseRnaDbReporter.class);
    /** source database */
    private DbConnection db;
    /** genome ID for the genome of interest */
    private String genomeId;

    /**
     * This interface is used by the reports to get additional parameters from the controlling command processor
     */
    public interface IParms {

        /**
         * @return the ID of the genome of interest
         */
        String getGenomeId();

        /**
         * @return the ID of the sample of interest
         */
        String getSampleId();

    }

    /**
     * This enumeration lists the report types.
     */
    public enum Type {
        FEATURE_CLUSTER {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new FeatureClusterReporter(processor, db);
            }
        }, NORMAL_CHECK {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new NormalCheckReporter(processor, db);
            }
        }, GENE_DATA {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new GeneDataReporter(processor, db);
            }
        };

        /**
         * @return a reporter of this type
         *
         * @param processor		controlling command processor
         * @param db			source database connection
         */
        public abstract BaseRnaDbReporter create(IParms processor, DbConnection db);

    }

    /**
     * Construct a new RNA database reporter.
     *
     * @param processor		controlling command processor
     * @param db			source database connection
     */
    protected BaseRnaDbReporter(IParms processor, DbConnection db) {
        // Get the database.
        this.db = db;
        // Get the target genome ID.
        this.genomeId = processor.getGenomeId();

    }

    /**
     * Write the report.
     *
     * @param writer	print writer for the report
     *
     * @throws IOException
     * @throws SQLException
     */
    public abstract void writeReport(PrintWriter writer) throws IOException, SQLException;

    /**
     * @return the database connection
     */
    protected DbConnection getDb() {
        return this.db;
    }

    /**
     * @return the ID of the genome of interest
     */
    protected String getGenomeId() {
        return this.genomeId;
    }

    /**
     * @return a feature index for the genome of interest
     *
     * @throws SQLException
     */
    protected FeatureIndex getFeatureIndex() throws SQLException {
        return new FeatureIndex(this.db, this.genomeId);
    }

}
