/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.clusters.Cluster;
import org.theseed.clusters.ClusterGroup;
import org.theseed.clusters.Similarity;
import org.theseed.java.erdb.DbConnection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This is the base class for feature correlation reports.  The client passes in the master database, the output
 * stream, and an iterator through the correlations.  The subclass then writes the appropriate report to the
 * appropriate file.
 *
 * @author Bruce Parrello
 *
 */
public abstract class FeatureCorrReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FeatureCorrReporter.class);
    /** output stream */
    private OutputStream outStream;
    /** database connection */
    private DbConnection db;

    /**
     * This interface must be supported by controlling command processors.  Used to get report parameters.
     */
    public interface IParms {

        /**
         * @return the output stream for the report
         */
        public OutputStream getStream();

        /**
         * @return the tracking levels for the correlation scores
         */
        public double[] getLevels();

        /**
         * @return the ID of the base genome
         */
        public String getGenomeId();
    }

    /**
     * This enumeration describes the types of reports supported.
     */
    public static enum Type {
        /** compute statistics for non-operon pairs */
        NON_OPERON {
            @Override
            public FeatureCorrReporter create(IParms processor) {
                return new NonOperonPairReporter(processor);
            }
        };

        /**
         * @return a reporter of this type
         *
         * @param processor		controlling command processor
         */
        public abstract FeatureCorrReporter create(IParms processor);
    }

    /**
     * This class iterates through all the correlations in a cluster group.
     */
    public static class Iter implements Iterator<Similarity> {

        /** next similarity to return */
        private Similarity nextSim;
        /** iterator through the clusters */
        private Iterator<Cluster> clusterIter;
        /** current cluster ID */
        private String clusterId;
        /** iterator through the similarities in the current cluster */
        private Iterator<Similarity> simIter;

        public Iter(ClusterGroup sourceClusters) {
            this.clusterIter = sourceClusters.getClusters().iterator();
            if (! this.clusterIter.hasNext()) {
                // Here the cluster group is empty.
                this.nextSim = null;
            } else {
                // Prime the iteration by setting up the first cluster.
                this.nextCluster();
                // Position on the first eligible similarity.
                this.readForward();
            }
        }

        /**
         * Set up the next cluster for iteration.
         */
        private void nextCluster() {
            Cluster cluster = this.clusterIter.next();
            this.clusterId = cluster.getId();
            this.simIter = cluster.getSims().iterator();
        }

        /**
         * Position on the next eligible similarity.
         */
        private void readForward() {
            this.findSim();
            while (this.nextSim == null && this.clusterIter.hasNext()) {
                this.nextCluster();
                this.findSim();
            }
        }

        /**
         * Try to find an eligible similarity in the current cluster.
         */
        private void findSim() {
            this.nextSim = null;
            while (this.nextSim == null && simIter.hasNext()) {
                Similarity testSim = simIter.next();
                // We only accept similarities where the first ID is equal to the cluster ID, to prevent
                // duplicates.
                if (testSim.getId1().equals(this.clusterId))
                    this.nextSim = testSim;
            }
        }

        @Override
        public boolean hasNext() {
            return (this.nextSim != null);
        }

        @Override
        public Similarity next() {
            if (this.nextSim == null)
                throw new NoSuchElementException("Attempt to iterate past end of cluster group.");
            // Return the next similarity.
            Similarity retVal = this.nextSim;
            // Position for the one after that.
            this.readForward();
            return retVal;
        }

    }

    /**
     * Construct a reporter object.
     *
     * @param processor		controlling command processor
     */
    public FeatureCorrReporter(IParms processor) {
        this.outStream = processor.getStream();
        this.db = null;
    }

    /**
     * Specify the master RNA database.
     *
     * @param db	database to use
     */
    public void setDb(DbConnection db) {
        this.db = db;
    }

    /**
     * Run the report.
     *
     * @param clusters		cluster group object containing the correlations
     *
     * @throws IOException
     * @throws SQLException
     */
    public void runReport(ClusterGroup clusters) throws SQLException, IOException {
        if (this.db == null)
            throw new IllegalArgumentException("Must set database before starting report.");
        Iterator<Similarity> iter = new Iter(clusters);
        this.writeReport(iter, this.db, this.outStream);
    }

    /**
     * Create the report from the specified correlations on the specified output stream.
     *
     * @param iter		iterator for the similarites
     * @param rnaDb		master RNA database
     * @param stream	output stream for the report
     *
     * @throws IOException
     * @throws SQLException
     */
    protected abstract void writeReport(Iterator<Similarity> iter, DbConnection rnaDb, OutputStream stream)
            throws SQLException, IOException;

}
