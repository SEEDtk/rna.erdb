/**
 *
 */
package org.theseed.rna.erdb;

import java.io.IOException;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.DbUpdate;
import org.theseed.java.erdb.Relop;
import org.theseed.rna.baseline.DbBaselineComputer;
import org.theseed.rna.baseline.DbBaselineComputer.IParms;

/**
 * This command computes the baseline level for each feature in a particular genome.  We read in
 * the samples one at a time and compute the mean value for each level.  Only one baseline value
 * is kept, but we provide multiple algorithms for computing the baseline.
 *
 * The positional parameter is the ID of the target genome.  The command-line options are as
 * follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --method		baseline computation algorithm to use
 *
 *
 * @author Bruce Parrello
 *
 */
public class GenomeBaselineProcessor extends BaseDbRnaProcessor implements IParms {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(GenomeBaselineProcessor.class);
    /** baseline computer */
    private DbBaselineComputer computer;

    // COMMAND-LINE OPTIONS

    /** baseline computation method */
    @Option(name = "--method", usage = "method for computing the baselines")
    private DbBaselineComputer.Type method;

    @Override
    protected void setDbDefaults() {
        this.method = DbBaselineComputer.Type.WEIGHTED;
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException, IOException {
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        // Load the feature index.  We will need this later.
        this.buildFeatureIndex(db);
        // Initialize the baseline computer.
        this.computer = this.method.create(this);
        // Now we need to loop through the non-suspicious samples for this genome and run them
        // through the baseline computer.
        try (DbQuery query = new DbQuery(db, "RnaSample")) {
            query.selectAll("RnaSample").rel("RnaSample.genome_id", Relop.EQ).rel("RnaSample.suspicious", Relop.EQ);
            query.setParm(1, this.getGenomeId());
            query.setParm(2, false);
            log.info("Processing samples for genome {}.", this.getGenomeId());
            int count = 0;
            for (DbRecord record : query) {
                this.computer.processRecord(record);
                count++;
                if (log.isInfoEnabled() && count % 500 == 0)
                    log.info("{} samples processed.", count);
            }
            log.info("{} total samples processed.", count);
        }
        // Now we get the baseline from the computer and update the feature records.
        try (var xact = db.new Transaction()) {
            double[] baselines = this.computer.getBaselines();
            try (DbUpdate updater = DbUpdate.batch(db, "Feature")) {
                // We are changing the baseline and filtering the update using the primary key.
                updater.change("baseline").primaryKey().createStatement();
                log.info("Updating baselines for {} features.", baselines.length);
                // Loop through the feature index.
                for (int i = 0; i < baselines.length; i++) {
                    String fid = this.getFeatureId(i);
                    updater.set("baseline", baselines[i]);
                    updater.set("fig_id", fid);
                    updater.update();
                }
            }
            xact.commit();
        }
    }

}
