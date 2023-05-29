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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
public class FeatureClusterReporter extends BaseFeatureClusterReporter {

    // FIELDS
    /** array of group types (other than cluster) */
    private String[] types;
    /** table of contents (OL with LI elements) */
    private ContainerTag toc;
    /** list of cluster sections (each is a DIV) */
    private List<ContainerTag> clusterSections;
    /** buffer for building table cell contents */
    private StringBuilder cellBuilder;
    /** separator for group elements */
    private static final String SEPARATOR = " <span style=\"background-color: #FFFF00; font-weight: bold\">|</span> ";

    public FeatureClusterReporter(IParms processor, DbConnection db) {
        super(processor, db);
    }

    @Override
    protected void writeClusterReport(PrintWriter writer) throws IOException, SQLException {
        // Create the type list and the string buffer.
        this.types = this.getTypeArray();
        this.cellBuilder = new StringBuilder(80);
        // Build a descriptive section from the various counts.
        ContainerTag genomeNotes = ul().with(li(String.format("%d protein features in genome.", this.getFeatureCount())))
                .with(li(String.format("%d features in %d non-trivial clusters.", this.getBigClusterCoverage(),
                        this.getBigClusterCount())));
        for (String type : this.types)
            genomeNotes.with(li(String.format("%d features in %d %ss.", this.getTypeFeatureCount(type),
                    this.getTypeGroupCount(type), type)));
        // Now we go through each cluster.  For each group appearing in the cluster, we want to know how much of the
        // group is covered.  Unlike the initialization phase, we are going to be generating HTML now, so we
        // must generate our main containers.
        this.clusterSections = new ArrayList<ContainerTag>(this.getBigClusterCount());
        this.toc = ol();
        for (Map.Entry<String, Set<String>> clusterEntry : this.getClusterEntries()) {
            String clId = clusterEntry.getKey();
            Set<String> fids = clusterEntry.getValue();
            log.info("Processing cluster {}.", clId);
            this.buildClusterSection(clId, fids);
        }
        // Build the headers and assemble all the pieces.
        ContainerTag head = head().with(title("Gene Clusters for " + this.getGenomeId()))
                .with(link().withHref(CSS_HREF).withRel("stylesheet").withType("text/css"));
        ContainerTag heading = h1("Gene Clusters for " + this.getGenomeFullName());
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
            FeatureData feat = this.getFeature(fid);
            row = tr().with(td(fid), safe_td(feat.getGene()), safe_td(feat.getAlias()));
            // Create the group lists.
            for (String type : types) {
                // Get all the groups of this type.
                var groups = feat.getGroups(type);
                // If there are none, this table cell is empty.
                if (groups.isEmpty())
                    row.with(safe_td(null));
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
                    double pct = memberCount * 100.0 / this.getGroupCount(groupName);
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

}
