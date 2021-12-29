/**
 *
 */
package org.theseed.reports;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.theseed.java.erdb.DbRecord;

/**
 * This object encapsulates data about a feature that we need for various reports.
 * This includes the ID, the aliases, the function, and the groups to which it belongs.
 *
 * @author Bruce Parrello
 *
 */
public class FeatureData {

    // FIELDS
    /** feature ID */
    private String fid;
    /** gene name (or NULL if none) */
    private String gene;
    /** alias name (or NULL if none) */
    private String alias;
    /** functional assignment */
    private String assignment;
    /** baseline value */
    private double baseline;
    /** map of group lists by type */
    private Map<String, Set<String>> groupMap;
    /** constant for the empty group set */
    private static final Set<String> NO_GROUPS = Collections.emptySet();

    /**
     * Construct a feature-data object from a database record.
     *
     * @param record	source database record
     *
     * @throws SQLException
     */
    public FeatureData(DbRecord record) throws SQLException {
        this.fid = record.getString("Feature.fig_id");
        this.gene = record.getString("Feature.gene_name");
        this.alias = record.getString("Feature.alias");
        this.baseline = record.getDouble("Feature.baseline");
        // The assignment may need to be defaulted.
        this.assignment = record.getString("Feature.assignment");
        if (StringUtils.isBlank(this.assignment))
            this.assignment = "hypothetical protein";
        // Create the empty group map.
        this.groupMap = new TreeMap<String, Set<String>>();
    }

    /**
     * Add a group to this feature.
     *
     * @param type		type of group
     * @param name		name of group
     */
    public void addGroup(String type, String name) {
        // If there is a genome ID prefix, we need to remove it.  The separator is always a colon.
        Set<String> groups = this.groupMap.computeIfAbsent(type, k -> new TreeSet<String>());
        groups.add(name);
    }

    /**
     * @return the feature ID
     */
    public String getFid() {
        return this.fid;
    }

    /**
     * @return the gene name
     */
    public String getGene() {
        return this.gene;
    }

    /**
     * @return the alias
     */
    public String getAlias() {
        return this.alias;
    }

    /**
     * @return the baseline
     */
    public double getBaseline() {
        return this.baseline;
    }

    /**
     * @return the functional assignment
     */
    public String getAssignment() {
        return this.assignment;
    }

    /**
     * @return the set of groups of the specified type
     *
     * @param type		type of group desired
     */
    public Set<String> getGroups(String type) {
        return this.groupMap.getOrDefault(type, NO_GROUPS);
    }

    /**
     * @return all the group types and their members in the form of map entries
     */
    public Set<Map.Entry<String, Set<String>>> getGroupings() {
        return this.groupMap.entrySet();
    }

}
