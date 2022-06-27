/**
 *
 */
package org.theseed.rna.data;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.Relop;

/**
 * This object represents data about a feature used for expression data analysis.
 *
 * @author Bruce Parrello
 *
 */
public class RnaFeature {

    // FIELDS
    /** ID of the feature */
    private String fid;
    /** column name to give the feature */
    private String name;
    /** baseline expression value for the feature */
    private double baseLine;
    /** feature array index */
    private int idx;
    /** types of groups to which the feature belongs */
    private Set<String> groupTypes;

    /**
     * Create a new RNA feature descriptor.
     *
     * @param fig_id		ID of the feature
     * @param gene_name		gene name of the feature (if any)
     * @param baseline		baseline expression value
     * @param seq_no		sequence number in the data array (0-based)
     */
    protected RnaFeature(String fig_id, String gene_name, double baseline, int seq_no) {
        this.baseLine = baseline;
        this.fid = fig_id;
        this.groupTypes = new TreeSet<String>();
        // Form the name from the fig ID and the gene name.
        String prefix = (StringUtils.isBlank(gene_name) ? "peg" : gene_name);
        String suffix = StringUtils.substringAfterLast(fig_id, ".");
        this.name = prefix + "." + suffix;
    }

    /**
     * Load the features for a genome from the RNA database.
     *
     * @param db		source database connection
     * @param genomeId	ID of target genome
     * @param filter	feature filter to apply
     *
     * @return a set of features to use
     *
     * @throws SQLException
     */
    public static Set<RnaFeature> loadFeats(DbConnection db, String genomeId, RnaFeatureFilter filter) throws SQLException {
        // We will start by building a map of the features.
        var featMap = new HashMap<String, RnaFeature>(4000);
        try (DbQuery fQuery = new DbQuery(db, "Feature")) {
            fQuery.select("Feature", "fig_id", "gene_name", "baseline", "seq_no");
            fQuery.rel("Feature.genome_id", Relop.EQ);
            fQuery.setParm(1, genomeId);
            // Get all features for the genome.
            var iter = fQuery.iterator();
            while (iter.hasNext()) {
                var record = iter.next();
                var fid = record.getString("Feature.fig_id");
                var feat = new RnaFeature(fid, record.getString("gene_name"), record.getDouble("Feature.baseline"),
                        record.getInt("Feature.seq_no"));
                featMap.put(fid, feat);
            }
        }
        // Now add in all the group types.
        try (DbQuery tQuery = new DbQuery(db, "Feature FeatureToGroup FeatureGroup")) {
            tQuery.select("FeatureToGroup", "fig_id");
            tQuery.select("FeatureGroup", "group_type");
            tQuery.rel("Feature.genome_id", Relop.EQ);
            tQuery.setParm(1, genomeId);
            // Get the groups for all the features.
            var iter = tQuery.iterator();
            while (iter.hasNext()) {
                var record = iter.next();
                var fid = record.getString("FeatureToGroup.fig_id");
                var type = record.getString("FeatureGroup.group_type");
                var feat = featMap.get(fid);
                if (feat != null)
                    feat.groupTypes.add(type);
            }
        }
        // Form the list into an array.
        var retVal = featMap.values().stream().filter(x -> filter.include(x)).collect(Collectors.toSet());
        return retVal;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.fid == null) ? 0 : this.fid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RnaFeature)) {
            return false;
        }
        RnaFeature other = (RnaFeature) obj;
        if (this.fid == null) {
            if (other.fid != null) {
                return false;
            }
        } else if (!this.fid.equals(other.fid)) {
            return false;
        }
        return true;
    }

    /**
     * @return TRUE if this feature is in a group of the specified type
     *
     * @param type		relevant group type
     */
    public boolean isInGroupType(String type) {
        return this.groupTypes.contains(type);
    }

    /**
     * @return the column name of this feature
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return the index of this feature in the expression array
     */
    public int getIdx() {
        return this.idx;
    }

    /**
     * @return the baseline expression level for this feature
     */
    public double getBaseLine() {
        return this.baseLine;
    }

}
