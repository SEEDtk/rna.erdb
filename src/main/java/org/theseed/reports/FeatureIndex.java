/**
 *
 */
package org.theseed.reports;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.counters.Shuffler;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;

/**
 * This object manages all the features in a genome for reporting purposes.  It contains
 * the list of FeatureData objects in index order as well as the set of group types.
 *
 * @author Bruce Parrello
 *
 */
public class FeatureIndex implements Iterable<FeatureData> {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FeatureIndex.class);
    /** index of feature data objects */
    private List<FeatureData> features;
    /** set of group types */
    private SortedSet<String> groupTypes;
    /** ID of the source genome */
    private final String genomeId;
    /** number of pegs in the genome */
    private final int nPegs;
    /** genome name */
    private final String gName;

    /**
     * Load the feature index from the database.
     *
     * @param db		source database connection
     * @param genomeId	ID of the source genome
     *
     * @throws SQLException
     */
    public FeatureIndex(DbConnection db, String genomeId) throws SQLException {
        this.genomeId = genomeId;
        // Get the genome record.
        DbRecord genome = db.getRecord("Genome", genomeId);
        this.nPegs = genome.getInt("Genome.peg_count");
        this.gName = genome.getString("Genome.genome_name");
        // Allocate the feature structures.
        this.features = new Shuffler<FeatureData>(this.nPegs);
        this.groupTypes = new TreeSet<String>();
        // Now we loop through the genome's features and group relationships.  The first time we see a
        // feature we put it in the feature index.  Otherwise, we add the group.
        try (DbQuery query = new DbQuery(db, "Feature < FeatureToGroup < FeatureGroup")) {
            query.selectAll("Feature");
            query.select("FeatureGroup", "group_type", "group_name");
            query.rel("Feature.genome_id", Relop.EQ);
            query.setParm(1, genomeId);
            var iter = query.iterator();
            while (iter.hasNext()) {
                var record = iter.next();
                // Get the sequence number of this feature.
                int seqNo = record.getInt("Feature.seq_no");
                // Get the feature descriptor.  We may have to create it.
                FeatureData feat = this.features.get(seqNo);
                if (feat == null) {
                    feat = new FeatureData(record);
                    this.features.set(seqNo, feat);
                }
                // We have a feature descriptor.  Check for a group.
                String groupType = record.getString("FeatureGroup.group_type");
                if (groupType != null) {
                    String groupName = record.getString("FeatureGroup.group_name");
                    feat.addGroup(groupType, groupName);
                    this.groupTypes.add(groupType);
                }
            }
        }
    }

    /**
     * @return the feature descriptor for the feature at the specified position
     *
     * @param idx	relevant feature index
     */
    public FeatureData getFeature(int idx) {
        return this.features.get(idx);
    }

    /**
     * @return the set of group types
     */
    protected SortedSet<String> getGroupTypes() {
        return this.groupTypes;
    }

    /**
     * @return the genome ID
     */
    protected String getGenomeId() {
        return this.genomeId;
    }

    /**
     * @return the number of features
     */
    protected int getFeatureCount() {
        return this.nPegs;
    }

    /**
     * @return the genome name
     */
    protected String getGenomeName() {
        return this.gName;
    }

    /**
     * @return the genome ID and name
     */
    public String getGenomeFullName() {
        return this.genomeId + ": " + this.gName;
    }

    @Override
    public Iterator<FeatureData> iterator() {
        return this.features.iterator();
    }

}
