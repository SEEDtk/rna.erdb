/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.theseed.counters.CountMap;
import org.theseed.java.erdb.DbConnection;

/**
 * This is the base class for feature cluster reports based on the RNA database.  It
 * computes the various counts and groupings, then allows the subclass to do the
 * reporting.
 *
 * @author Bruce Parrello
 *
 */
public abstract class BaseFeatureClusterReporter extends BaseRnaDbReporter {

    // FIELDS
    /** count of features in each group */
    private CountMap<String> groupCounts;
    /** count of features in each group type */
    private CountMap<String> typeFeatureCounts;
    /** count of groups of each type */
    private CountMap<String> typeGroupCounts;
    /** number of nontrivial clusters */
    private int bigClusters;
    /** number of features in nontrivial clusters */
    private int bigClusterCoverage;
    /** map of cluster names to feature IDs for nontrivial clusters */
    private Map<String, Set<String>> clusterMap;
    /** map of feature IDs to feature data objects */
    private Map<String, FeatureData> featureMap;
    /** main feature index */
    private FeatureIndex featureIndex;
    /** ordering to use for features in a cluster */
    private static final Comparator<String> FID_SORTER = new NaturalSort();

    protected BaseFeatureClusterReporter(IParms processor, DbConnection db) {
        super(processor, db);
    }

    /**
     * Scan the feature index and fill in all the tables and counters.
     *
     * @throws SQLException
     */
    protected void initializeData() throws SQLException {
        // Get the feature data from the database.
        log.info("Reading features from database.");
        this.featureIndex = this.getFeatureIndex();
        log.info("Scanning features to count groupings.");
        // Get the feature count.  We use this to estimate map sizes.
        int nPegs = this.featureIndex.getFeatureCount();
        // Initialize the map of cluster IDs to feature IDs.
        this.clusterMap = new TreeMap<String, Set<String>>();
        // Initialize the map of feature IDs to feature data objects.
        this.featureMap = new HashMap<String, FeatureData>(nPegs * 4 / 3 + 1);
        // Initialize the map of group IDs to member counts.
        this.groupCounts = new CountMap<String>();
        // Initialize the type coverage counters.
        this.typeFeatureCounts = new CountMap<String>();
        // This will track the set of groups found for each type.  That, in turn, will eventually
        // be turned into "typeGroupCounts".
        var typeGroupMap = new TreeMap<String, Set<String>>();
        // Now loop through the features in the feature index.
        for (FeatureData feat : this.featureIndex) {
            // Add this feature to the feature map.
            this.featureMap.put(feat.getFid(), feat);
            // Loop through its group types.
            for (Map.Entry<String, Set<String>> typeEntry : feat.getGroupings()) {
                String type = typeEntry.getKey();
                Set<String> groups = typeEntry.getValue();
                // If this is clusters, it is handled differently.
                if (type.contentEquals("cluster")) {
                    // There is always only one cluster.  If there is no cluster for this feature,
                    // it would not even show up in the type list, and clusters are partitions, so
                    // there is never more than one.
                    String clId = groups.stream().findFirst().get();
                    // Connect this feature to the cluster.  Note that the feature set is sorted
                    // in natural order, which in this case means by peg number.
                    Set<String> clusterFeatures = this.clusterMap.computeIfAbsent(clId,
                            k -> new TreeSet<String>(FID_SORTER));
                    clusterFeatures.add(feat.getFid());
                } else {
                    // Here we have one of the other feature types.  We need to do all the counting.
                    // First, count this feature as covered by this type.
                    this.typeFeatureCounts.count(type);
                    // Process the list of groups of this type.
                    for (String group : groups) {
                        // This helps us figure out the group size for the coverage math.
                        this.groupCounts.count(group);
                        // Now we add this group to the set of groups of this type.
                        Set<String> typeGroups = typeGroupMap.computeIfAbsent(type, k -> new HashSet<String>(100));
                        typeGroups.add(group);
                    }
                }
            }
        }
        log.info("Accumulating counters.");
        // Now we need to convert the typeGroupMap to counts.
        this.typeGroupCounts = new CountMap<String>();
        typeGroupMap.entrySet().stream().forEach(x -> this.typeGroupCounts.count(x.getKey(), x.getValue().size()));
        // Finally, we count the number and coverage for nontrivial clusters.
        this.bigClusterCoverage = 0;
        this.bigClusters = 0;
        for (Map.Entry<String, Set<String>> clEntry : this.clusterMap.entrySet()) {
            Set<String> members = clEntry.getValue();
            if (members.size() > 1) {
                this.bigClusters++;
                this.bigClusterCoverage += members.size();
            }
        }
    }

    /**
     * @return an array of the non-cluster feature group types
     */
    protected String[] getTypeArray() {
        return this.typeFeatureCounts.keys().stream().toArray(String[]::new);
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        this.initializeData();
        this.writeClusterReport(writer);
    }

    /**
     * @return the number of features in the database
     */
    protected int getFeatureCount() {
        return this.featureIndex.getFeatureCount();
    }

    /**
     * Write the initialized clustering data to the output.
     *
     * @param writer	output print writer
     *
     * @throws IOException
     * @throws SQLException
     */
    protected abstract void writeClusterReport(PrintWriter writer) throws IOException, SQLException;

    /**
     * @return the number of nontrivial clusters
     */
    public int getBigClusterCount() {
        return this.bigClusters;
    }

    /**
     * @return the number of features in nontrivial clusters
     */
    public int getBigClusterCoverage() {
        return this.bigClusterCoverage;
    }

    /**
     * @return an iterable through the cluster map
     */
    protected Iterable<Map.Entry<String, Set<String>>> getClusterEntries() {
        return this.clusterMap.entrySet();
    }

    /**
     * @return the full name of the base genome
     */
    protected String getGenomeFullName() {
        return this.featureIndex.getGenomeFullName();
    }

    /**
     * @return the feature data for the feature with the specified ID
     *
     * @param fid	ID of the desired feature
     */
    protected FeatureData getFeature(String fid) {
        return this.featureMap.get(fid);
    }

    /**
     * @return the total number of features in the specified group
     *
     * @param name		name of the group of interest
     */
    protected int getGroupCount(String name) {
        return this.groupCounts.getCount(name);
    }

    /**
     * @return the number of features in groups of the specified type
     *
     * @param type		group-type of interest
     */
    protected int getTypeFeatureCount(String type) {
        return this.typeFeatureCounts.getCount(type);
    }

    /**
     * @return the number of groups of the specified type
     *
     * @param type		group-type of interest
     */
    protected int getTypeGroupCount(String type) {
        return this.typeGroupCounts.getCount(type);
    }

}
