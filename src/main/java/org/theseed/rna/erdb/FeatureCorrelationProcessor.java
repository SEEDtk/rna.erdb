/**
 *
 */
package org.theseed.rna.erdb;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.util.ResizableDoubleArray;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.clusters.Cluster;
import org.theseed.clusters.ClusterGroup;
import org.theseed.clusters.methods.ClusterMergeMethod;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;
import org.theseed.java.erdb.SqlBuffer;
import org.theseed.utils.ParseFailureException;

/**
 * This command computes the correlations between RNA expression levels for features in a genome.  This
 * requires loading all the expression levels into memory, so it is a very memory-intensive process.
 *
 * The positional parameter is the target genome ID.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o 	output file for report (if not STDOUT)
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --min		minimum clustering score, default 0.70
 *
 * @author Bruce Parrello
 *
 */
public class FeatureCorrelationProcessor extends BaseDbRnaProcessor {

     // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FeatureCorrelationProcessor.class);
    /** list of feature arrays, in index order */
    private ResizableDoubleArray[] levelArrays;
    /** number of feature comparisons generated */
    private int compareCount;
    /** clustering controller */
    private ClusterGroup clusters;

    // COMMAND-LINE OPTIONS

    /** minimum score for clustering */
    @Option(name = "--min", metaVar = "0.60", usage = "minimum acceptable score for feature clustering")
    private double minScore;

    @Override
    protected void setDbDefaults() {
        this.minScore = 0.70;
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException {
        if(this.minScore <= 0.0 || this.minScore > 1.0)
            throw new ParseFailureException("Minimum score must be between 0 and 1.");
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        try (var xact = db.new Transaction()) {
            // Erase the existing clusters.
            this.deleteOldClusters(db);
            // Save the feature data.
            this.buildFeatureIndex(db);
            // Loop through the features, loading the level arrays.  We will have one level array per feature, which
            // means we are essentially flipping the native representation, which is one array per sample.
            // First, we create one array per feature.
            this.levelArrays = IntStream.range(0, this.getFeatureCount()).mapToObj(i -> new ResizableDoubleArray())
                    .toArray(ResizableDoubleArray[]::new);
            // Now we loop through the samples, filling the arrays.
            log.info("Retrieving expression levels for {} features in genome {}.", this.getFeatureCount(), this.getGenomeId());
            try (DbQuery query = new DbQuery(db, "RnaSample")) {
                // Ask for all samples for this genome that are NOT suspicious.
                query.select("RnaSample", "feat_data").rel("RnaSample.genome_id", Relop.EQ)
                    .rel("RnaSample.suspicious", Relop.EQ).setParm(1, this.getGenomeId())
                    .setParm(2, false);
                query.stream().forEach(x -> this.processLevels(x));
            }
            // Now we need to do the comparisons and then load them into the cluster controller.
            log.info("Performing comparisons.");
            this.clusters = new ClusterGroup(this.getFeatureCount(), ClusterMergeMethod.COMPLETE);
            // We have an array of expression levels for each feature, one column per sample.  We compute
            // the correlation for each sample pair.  Because this could involve millions of comparisons, we
            // parallelize it.
            this.compareCount = 0;
            IntStream.range(0, getFeatureCount()).parallel().forEach(i -> this.compareLevels(i));
            // Now we create clusters.
            // Perform the merges.
            int mergeCount = 0;
            while(this.clusters.merge(this.minScore)) {
                mergeCount++;
                if (mergeCount % 100 == 0)
                    log.info("{} merges performed.", mergeCount);
            }
            // Now we need to add the clusters to the database.
            try (DbLoader groupLoader = DbLoader.single(db, "FeatureGroup");
                    DbLoader connectLoader = DbLoader.batch(db, "FeatureToGroup")) {
                // Set the group type to clusters.
                groupLoader.set("group_type", "cluster");
                // Loop through the clusters, forging the connections.
                int connections = 0;
                int clNum = 0;
                for (Cluster cluster : this.clusters.getClusters()) {
                    // Create the cluster's group record.
                    clNum++;
                    String clusterId = String.format("CL%04d", clNum);
                    log.info("Creating cluster group {}.", clusterId);
                    groupLoader.set("group_name", clusterId);
                    String groupId = this.getGenomeId() + ":" + clusterId;
                    groupLoader.set("group_id", groupId);
                    groupLoader.insert();
                    // Now we attached the cluster members to the group.
                    connectLoader.set("group_id", groupId);
                    for (String member : cluster.getMembers()) {
                        connectLoader.set("fig_id", member);
                        log.debug("Storing feature {}.", member);
                        connectLoader.insert();
                        connections++;
                        if (log.isInfoEnabled() && connections % 100 == 0)
                            log.info("{} cluster connections forged.", connections);
                    }
                }
            }
            // Commit the changes.
            xact.commit();
        }
    }

    /**
     * This method removes old feature clusters for this genome so that new ones can be created.
     *
     * @param db	database connection
     *
     * @throws SQLException
     */
    private void deleteOldClusters(DbConnection db) throws SQLException {
        log.info("Deleting old clusters for {}.", this.getGenomeId());
        // We delete any cluster group whose ID begins with the genome ID.
        SqlBuffer buffer = new SqlBuffer(db).append("DELETE FROM ").quote("FeatureGroup")
                .append(" WHERE ").quote("FeatureGroup", "group_type").append(" = 'cluster' AND ")
                .quote("FeatureGroup", "group_id").append(" LIKE ").appendMark();
        try (PreparedStatement stmt = db.createStatement(buffer)) {
            stmt.setString(1, this.getGenomeId() + ":%");
            stmt.execute();
        }
    }

    /**
     * Generate all the correlations to the feature at the specified index.
     *
     * @param i		index of the feature to compare to all subsequent features
     */
    private void compareLevels(int i) {
        // Get a compute for the pearsons correlations.
        PearsonsCorrelation computer = new PearsonsCorrelation();
        String fidI = this.getFeatureId(i);
        ResizableDoubleArray levelsI = this.levelArrays[i];
        final int n = levelsI.getNumElements();
        // Now loop through all of the features following this one.
        for (int j = i + 1; j < this.levelArrays.length; j++) {
            String fidJ = this.getFeatureId(j);
            ResizableDoubleArray levelsJ = this.levelArrays[j];
            // Create arrays for the matching finite values.
            ResizableDoubleArray arrayI = new ResizableDoubleArray(n);
            ResizableDoubleArray arrayJ = new ResizableDoubleArray(n);
            for (int k = 0; k < n; k++) {
                double valI = levelsI.getElement(k);
                double valJ = levelsJ.getElement(k);
                if (Double.isFinite(valI) && Double.isFinite(valJ)) {
                    arrayI.addElement(valI);
                    arrayJ.addElement(valJ);
                }
            }
            // Compute the correlation.
            double pc = 0.0;
            if (arrayI.getNumElements() > 2)
                pc = computer.correlation(arrayI.getElements(), arrayJ.getElements());
            this.storeCorrelation(fidI, fidJ, pc);
        }
    }

    /**
     * Store the results of a correlation computation.
     *
     * @param fidI		ID of first feature
     * @param fidJ		ID of second feature
     * @param pc		pearson correlation
     */
    private synchronized void storeCorrelation(String fidI, String fidJ, double pc) {
        if (! Double.isFinite(pc)) {
            log.warn("Could not compute correlation between {} and {}.", fidI, fidJ);
            pc = 0.0;
        }
        this.clusters.addSim(fidI, fidJ, pc);
        this.compareCount++;
        if (log.isInfoEnabled() && this.compareCount % 2000 == 0)
            log.info("{} comparisons computed.", this.compareCount);
    }

    /**
     * This method retrieves the feature array from a database record and adds its expression levels
     * to the array of feature-level arrays.
     *
     * @param record	RnaSample record to process
     */
    private void processLevels(DbRecord record) {
        try {
            double[] levels = record.getDoubleArray("RnaSample.feat_data");
            for (int i = 0; i < this.levelArrays.length; i++)
                this.levelArrays[i].addElement(levels[i]);
        } catch (SQLException e) {
            // Convert the SQL exception to unchecked so we can do this in a stream.
            throw new RuntimeException(e);
        }
    }

}
