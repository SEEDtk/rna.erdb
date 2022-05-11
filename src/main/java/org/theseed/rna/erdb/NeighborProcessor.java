/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbUpdate;
import org.theseed.neighbors.Neighborhood;
import org.theseed.utils.ParseFailureException;

/**
 * This command reads a feature correlation file and updates the neighbor fields in the
 * Feature records of the RNA Seq database.
 *
 * The feature correlation table should be on the standard input.  It has three columns,
 * tab-delimited with headers, the first two columns containing feature IDs and the third
 * a correlation coefficient between them.
 *
 * The first positional parameters is the target genome ID.
 *
 * The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -i	name of the file containing the feature correlations (if not STDIN)
 * -n	number of neighbors to keep for each feature
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 *
 * @author Bruce Parrello
 *
 */
public class NeighborProcessor extends BaseDbRnaProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(NeighborProcessor.class);
    /** input stream for correlations */
    private TabbedLineReader inStream;
    /** map of feature IDs to neighborhoods */
    private Map<String, Neighborhood> neighborhoodMap;

    // COMMAND-LINE OPTIONS

    /** name of input file (if not STDIN) */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "corrFile.tbl",
            usage = "name of the correlation input file (if not STDIN")
    private File inFile;

    /** maximum size of neighborhood */
    @Option(name = "--max", aliases = { "-n" }, metaVar = "5",
            usage = "maximum number of neighbors to retain")
    private int nMax;

    @Override
    protected void setDbDefaults() {
        this.inFile = null;
        this.nMax =  10;
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException, IOException {
        // Validate the neighborhood size.
        if (this.nMax <= 0)
            throw new ParseFailureException("Maximum neighborhood size must be positive.");
        // Set up the input file.
        if (this.inFile == null) {
            log.info("Correlations will be read from the standard input.");
            this.inStream = new TabbedLineReader(System.in);
        } else {
            log.info("Correlations wiill be read from {}.", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        }
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        try {
            // Create the empty neighbor map.
            this.neighborhoodMap = new HashMap<String, Neighborhood>(4000);
            // Loop through the input file, saving correlations.
            int inCount = 0;
            for (TabbedLineReader.Line line : this.inStream) {
                String id1 = line.get(0);
                String id2 = line.get(1);
                double corr = line.getDouble(2);
                this.addCorr(id1, id2, corr);
                this.addCorr(id2, id1, corr);
                inCount++;
                if (log.isInfoEnabled() && inCount % 5000 == 0)
                    log.info("{} correlations processed.", inCount);
            }
            // Now we must store the neighborhoods in the features.  Get the feature table.
            log.info("Updating database.");
            int outCount = 0;
            int nCount = 0;
            try (DbUpdate updater = DbUpdate.batch(db, "Feature")) {
                updater.change("neighbors").primaryKey();
                for (Map.Entry<String, Neighborhood> hoodEntry : this.neighborhoodMap.entrySet()) {
                    String fid = hoodEntry.getKey();
                    Neighborhood hood = hoodEntry.getValue();
                    if (hood.size() > 0) {
                        String neighbors = Arrays.stream(hood.getNeighbors()).map(x -> x.getId())
                                .collect(Collectors.joining(","));
                        updater.set("neighbors", neighbors);
                        updater.set("fig_id", fid);
                        updater.update();
                        outCount++;
                        nCount += hood.size();
                    }
                }
            }
            log.info("{} features updated with {} neighbors.", outCount, nCount);
        } finally {
            this.inStream.close();
        }
    }

    /**
     * Record a correlation in the neighbor map.
     *
     * @param id1		ID of first feature
     * @param id2		ID of correlated feature
     * @param corr		correlation coefficient
     */
    private void addCorr(String id1, String id2, double corr) {
        Neighborhood hood  = this.neighborhoodMap.computeIfAbsent(id1, x -> new Neighborhood(this.nMax));
        hood.merge(id2, corr);
    }

}
