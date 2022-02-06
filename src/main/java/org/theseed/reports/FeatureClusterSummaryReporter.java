/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.theseed.counters.CountMap;
import org.theseed.java.erdb.DbConnection;

/**
 * This is a flat file report that displays the statistics for each feature cluster.
 *
 * @author Bruce Parrello
 */
public class FeatureClusterSummaryReporter extends BaseFeatureClusterReporter {

    protected FeatureClusterSummaryReporter(IParms processor, DbConnection db) {
        super(processor, db);
    }

    @Override
    protected void writeClusterReport(PrintWriter writer) throws IOException, SQLException {
        // Get the array of types.
        String[] types = this.getTypeArray();
        // Write the header line.
        writer.println("cluster\tsize\t" + StringUtils.join(types, '\t'));
        // Now loop through the clusters, producing output.
        for (Map.Entry<String, Set<String>> clusterEntry : this.getClusterEntries()) {
            String clusterId = clusterEntry.getKey();
            var cluster = clusterEntry.getValue();
            int size = cluster.size();
            // This will count the features for each group type in this cluster.
            var typeCounts = new CountMap<String>();
            for (String fid : clusterEntry.getValue()) {
                var feat = this.getFeature(fid);
                for (String type : types)  {
                    var groups = feat.getGroups(type);
                    if (groups != null && ! groups.isEmpty())
                        typeCounts.count(type);
                }
            }
            String line = clusterId + "\t" + Integer.toString(size) + "\t"
                    + Arrays.stream(types).map(x -> Integer.toString(typeCounts.getCount(x)))
                            .collect(Collectors.joining("\t"));
            writer.println(line);
        }
    }

}
