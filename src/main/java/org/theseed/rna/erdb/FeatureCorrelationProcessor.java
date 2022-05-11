/**
 *
 */
package org.theseed.rna.erdb;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.clusters.Cluster;
import org.theseed.clusters.ClusterGroup;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.java.erdb.SqlBuffer;
import org.theseed.utils.ParseFailureException;

/**
 * This command computes the correlations between RNA expression levels for features in a genome and updates
 * the database.  This requires loading all the expression levels into memory, so it is a very
 * memory-intensive process.
 *
 * The positional parameter is the target genome ID.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --min		minimum clustering score, default 0.90
 * --load		load file for correlations; if specified, it should have the feature IDs in the first
 * 				two columns and the correlation score in the third
 * --save		file in which to save the correlations computed
 *
 * @author Bruce Parrello
 *
 */
public class FeatureCorrelationProcessor extends BaseFeatureCorrelationProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FeatureCorrelationProcessor.class);


    // COMMAND-LINE OPTIONS

    /** minimum score for clustering */
    @Option(name = "--min", metaVar = "0.60", usage = "minimum acceptable score for feature clustering")
    private double minScore;

    @Override
    protected void setFeatureCorrDefaults() {
        this.minScore = 0.70;
    }

    @Override
    protected void validateFeatureCorrParms() throws ParseFailureException, IOException {
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
            // Form a cluster database out of the feature correlations.
            ClusterGroup clusters = this.processCorrelations(db);
            // Perform the merges.
            int mergeCount = 0;
            while(clusters.merge(this.minScore)) {
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
                for (Cluster cluster : clusters.getClusters()) {
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

}
