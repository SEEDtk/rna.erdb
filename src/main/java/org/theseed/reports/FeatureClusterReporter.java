/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.theseed.counters.CountMap;
import org.theseed.java.erdb.DbConnection;

import j2html.tags.ContainerTag;
import static j2html.TagCreator.*;

/**
 * This report produces a giant web page describing the feature clusters.  The feature clusters are
 * important constructs that we use to isolate proteins that work together, so this report is fairly
 * complex, and can only be rendered in HTML.
 *
 * @author Bruce Parrello
 *
 */
public class FeatureClusterReporter extends BaseRnaDbReporter {

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
    /** table of contents (OL with LI elements) */
    private ContainerTag toc;
    /** list of cluster sections (each is a DIV) */
    private List<ContainerTag> clusterSections;
    /** main feature index */
    private FeatureIndex featureIndex;
    /** buffer for building table cell contents */
    private StringBuilder cellBuilder;
    /** array of group types (other than cluster) */
    private String[] types;
    /** URL for style sheet */
    private static final String CSS_HREF = "https://core.theseed.org/SEEDtk/css/erdb.css";
    /** HTML for a an empty table cell */
    private static final ContainerTag EMPTY_CELL = td(rawHtml("&nbsp;"));
    /** separator for group elements */
    private static final String SEPARATOR = " <span style=\"background-color: #FFFF00; font-weight: bold\">|</span> ";
    /** ordering to use for features in a cluster */
    private static final Comparator<String> FID_SORTER = new NaturalSort();

    public FeatureClusterReporter(IParms processor, DbConnection db) {
        super(processor, db);
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        // Get the feature data from the database.
        log.info("Reading features from database.");
        this.featureIndex = this.getFeatureIndex();
        // Fill all the data structures and compute the counts.
        log.info("Scanning features to count groupings.");
        this.initializeData();
        // Create the type list and the string buffer.
        this.types = this.typeFeatureCounts.keys().stream().toArray(String[]::new);
        this.cellBuilder = new StringBuilder(80);
        // Build a descriptive section from the various counts.
        ContainerTag genomeNotes = ul().with(li(String.format("%d protein features in genome.", this.featureIndex.getFeatureCount())))
                .with(li(String.format("%d features in %d non-trivial clusters.", this.bigClusterCoverage, this.bigClusters)));
        for (String type : this.types)
            genomeNotes.with(li(String.format("%d features in %d %ss.", this.typeFeatureCounts.getCount(type),
                    this.typeGroupCounts.getCount(type), type)));
        // Now we go through each cluster.  For each group appearing in the cluster, we want to know how much of the
        // group is covered.  Unlike the initialization phase, we are going to be generating HTML now, so we
        // must generate our main containers.
        this.clusterSections = new ArrayList<ContainerTag>(this.bigClusters);
        this.toc = ol();
        for (Map.Entry<String, Set<String>> clusterEntry : this.clusterMap.entrySet()) {
            String clId = clusterEntry.getKey();
            Set<String> fids = clusterEntry.getValue();
            log.info("Processing cluster {}.", clId);
            this.buildClusterSection(clId, fids);
        }
        // Build the headers and assemble all the pieces.
        ContainerTag head = head().with(title("Gene Clusters for " + this.featureIndex.getGenomeId()))
                .with(link().withHref(CSS_HREF).withRel("stylesheet").withType("text/css"));
        ContainerTag heading = h1("Gene Clusters for " + this.featureIndex.getGenomeFullName());
        ContainerTag body = body().with(heading, genomeNotes, this.toc).with(this.clusterSections);
        ContainerTag page = html().with(head, body);
        writer.println(page.renderFormatted());
    }

    /**
     * Create and attach the HTML section for this cluster, including the feature table and the summary.
     *
     * @param clId		cluster ID
     * @param fids		list of member features
     */
    protected void buildClusterSection(String clId, Collection<String> fids) {
        // This structure computes our totals.  For each group type, we total the features in each group.
        var typeGroupCounts = new TreeMap<String, CountMap<String>>();
        // Here we build the table of features in the cluster.
        ContainerTag fidTable = table().withClass("big");
        // Construct the header row and add it.
        ContainerTag row = tr().with(th("fid"), th("gene"), th("alias"));
        row.with(Arrays.stream(types).map(x -> th(x)));
        row.with(th("function").withClass("big"));
        fidTable.with(row);
        // Now we build a row for each feature.  Along the way, we count the number of features and fill
        // in the type-group counts.
        for (String fid : fids) {
            // Get the feature and output its identifying columns.
            FeatureData feat = this.featureMap.get(fid);
            row = tr().with(td(fid), safe_td(feat.getGene()), safe_td(feat.getAlias()));
            // Create the group lists.
            for (String type : types) {
                // Get all the groups of this type.
                var groups = feat.getGroups(type);
                // If there are none, this table cell is empty.
                if (groups.isEmpty())
                    row.with(EMPTY_CELL);
                else {
                    // Count our membership in the groups.
                    CountMap<String> groupCounts = typeGroupCounts.computeIfAbsent(type, k -> new CountMap<String>());
                    groups.stream().forEach(x -> groupCounts.count(x));
                    // Form them into a table cell.  The special case of one group occurs a LOT, so we give it a
                    // fast path.
                    Iterator<String> iter = groups.iterator();
                    String group1 = iter.next();
                    if (groups.size() == 1)
                        row.with(td(group1));
                    else {
                        cellBuilder.setLength(0);
                        cellBuilder.append(text(group1).render());
                        while (iter.hasNext())
                            cellBuilder.append(SEPARATOR).append(text(iter.next()).render());
                        row.with(td(rawHtml(cellBuilder.toString())));
                    }
                }
            }
            // Add the function column at the end.  Note that this is never null or empty.
            row.with(td(feat.getAssignment()));
            fidTable.with(row);
        }
        // Now all the features have been put into "fidTable".  Generate the group summary.
        ContainerTag groupSummary = ul();
        int typeCount = 0;
        for (String type : types) {
            var memberCounter = typeGroupCounts.get(type);
            if (memberCounter != null) {
                // Describe each group of this type.
                var groupsOfType = memberCounter.sortedCounts();
                ContainerTag groupsOfTypeList = ol();
                for (CountMap<String>.Count groupCount : groupsOfType) {
                    String groupName = groupCount.getKey();
                    int memberCount = groupCount.getCount();
                    double pct = memberCount * 100.0 / this.groupCounts.getCount(groupName);
                    groupsOfTypeList.with(
                            li(String.format("%s: %d members, %4.1f%% coverage", groupName, memberCount, pct)));
                }
                ContainerTag typeElement = li(join(String.format("%s (%d groups)", type, groupsOfType.size()),
                        groupsOfTypeList));
                groupSummary.with(typeElement);
                typeCount++;
            }
        }
        // Finally, we need to create a header, put a link to it in the table of contents,
        // and add the group summary at the end.
        String clusterLabel = String.format("%s: %d members", clId, fids.size());
        this.toc.with(li(a(clusterLabel).withHref("#" + clId)));
        ContainerTag section = div().with(h1(a(clusterLabel).withName(clId)))
                .with(fidTable);
        if (typeCount > 0)
            section.with(groupSummary);
        this.clusterSections.add(section);
    }

    /**
     * This is a utility method for building a table cell.  If the incoming string is null or blank,
     * it will put in a non-breaking space.
     *
     * @param content	proposed content for a table cell
     *
     * @return a table cell containing the content
     */
    private static ContainerTag safe_td(String content) {
        ContainerTag retVal;
        if (StringUtils.isBlank(content))
            retVal = EMPTY_CELL;
        else
            retVal = td(content);
        return retVal;
    }

    /**
     * Scan the feature index and fill in all the tables and counters.
     */
    private void initializeData() {
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

}
