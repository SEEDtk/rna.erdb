/**
 *
 */
package org.theseed.rna.erdb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.erdb.utils.BaseDbProcessor;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;

/**
 * This is the base class for commands that process RNA samples in the database.
 *
 * The first positional parameters is the target genome ID.
 *
 * The built-in command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)

 * @author Bruce Parrello
 *
 */
public abstract class BaseDbRnaProcessor extends BaseDbProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BaseDbRnaProcessor.class);
    /** list of feature IDs in the order expected by the database */
    private List<String> featureIndex;
    /** match pattern for genome IDs */
    private static final Pattern GENOME_ID_PATTERN = Pattern.compile("\\d+\\.\\d+");

    // COMMAND-LINE OPTIONS

    /** target genome ID */
    @Argument(index = 0, metaVar = "genomeId", usage = "ID of genome to which the RNA was mapped", required = true)
    private String genomeId;

    @Override
    protected final boolean validateParms() throws ParseFailureException, IOException {
        // Insure the genome ID is reasonable.
        this.validateGenomeId();
        // Validate the subclass parameters.
        this.validateDbRnaParms();
        return true;
    }

    /**
     * Perform any parameter validation required by the subclass.
     *
     * @throws ParseFailureException
     * @throws IOException
     */
    protected abstract void validateDbRnaParms() throws ParseFailureException, IOException;

    /**
     * Insure the reference genome ID is valid.
     *
     * @throws ParseFailureException
     */
    protected void validateGenomeId() throws ParseFailureException {
        if (! GENOME_ID_PATTERN.matcher(this.getGenomeId()).matches())
            throw new ParseFailureException("Invalid genome ID \"" + this.getGenomeId() + "\".");
    }

    /**
     * Build the feature index that maps array indices in the feature data array to features in
     * the genome.
     *
     * @param db	RNA Seq database being updated
     *
     * @throws SQLException
     * @throws ParseFailureException
     */
    protected void buildFeatureIndex(DbConnection db) throws SQLException, ParseFailureException {
        log.info("Loading feature index for {}.", this.genomeId);
        // Create the array of feature indices.
        this.featureIndex = computeFeatureIndex(db, this.genomeId);
    }

    /**
     * Compute the feature index.  This is a list containing the feature IDs in the order the
     * features appear in the expression level array inside each sample.
     *
     * @param db			database to query
     * @param genome_id		genome ID whose feature index is to be built
     *
     * @return the feature index for the given genome
     *
     * @throws SQLException
     * @throws ParseFailureException
     */
    public static List<String> computeFeatureIndex(DbConnection db, String genome_id)
            throws SQLException, ParseFailureException {
        List<String> retVal = new ArrayList<String>(4000);
        try (DbQuery query = new DbQuery(db, "Genome Feature")) {
            query.select("Feature", "fig_id", "seq_no");
            query.rel("Genome.genome_id", Relop.EQ);
            query.setParm(1, genome_id);
            Iterator<DbRecord> iter = query.iterator();
            while (iter.hasNext()) {
                DbRecord feat = iter.next();
                String fid = feat.getString("Feature.fig_id");
                int idx = feat.getInt("Feature.seq_no");
                retVal.add(idx, fid);
            }
        }
        if (retVal.isEmpty())
            throw new ParseFailureException("Genome ID \"" + genome_id + "\" is not found or has no features.");
        return retVal;
    }

    /**
     * @return the ID of the feature at the specified position in the expression level array
     *
     * @param i		array index of interest
     */
    protected String getFeatureId(int i) {
        return this.featureIndex.get(i);
    }

    /**
     * @return the number of features in the expression level array
     */
    protected int getFeatureCount() {
        return this.featureIndex.size();
    }

    /**
     * @return the reference genome ID
     */
    public String getGenomeId() {
        return this.genomeId;
    }

}
