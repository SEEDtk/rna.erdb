/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.Relop;

/**
 * This report outputs all samples for a project along with a single measurement value (when present).
 *
 * @author Bruce Parrello
 *
 */
public class SampleMeasureReporter extends BaseRnaDbReporter {

    // FIELDS
    /** target project ID (NULL for all of them) */
    private String projectId;
    /** target measurement type */
    private String mType;
    /** target genome ID */
    private String genomeId;

    /**
     * Construct the sample measurement reporter and get the key parameters from the command processor.
     *
     * @param processor		controlling command processor
     * @param db			RNA Seq database
     */
    public SampleMeasureReporter(IParms processor, DbConnection db) {
        super(processor, db);
        this.projectId = processor.getProjectId();
        this.mType = processor.getMeasureType();
        this.genomeId = processor.getGenomeId();
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        // Construct the measurement query.  We will build a hash of sample ID to measurement value.
        Map<String, Double> measureMap = new HashMap<String, Double>(1000);
        try (DbQuery query = new DbQuery(this.getDb(), "Measurement")) {
            query.select("Measurement", "sample_id", "value");
            query.rel("Measurement.measure_type", Relop.EQ);
            query.setParm(1, this.mType);
            // Loop through the values found.
            var iter = query.iterator();
            while (iter.hasNext()) {
                var record = iter.next();
                measureMap.put(record.getString("Measurement.sample_id"), record.getDouble("Measurement.value"));
            }
        }
        log.info("{} measurement values found.", measureMap.size());
        // Construct the main query.
        try (DbQuery query = new DbQuery(this.getDb(), "RnaSample")) {
            query.select("RnaSample", "sample_id", "feat_count", "project_id", "pubmed", "quality", "suspicious");
            query.rel("RnaSample.genome_id", Relop.EQ);
            // If we have a project filter, add it.
            if (this.projectId != null)
                query.rel("RnaSample.project_id", Relop.EQ);
            // Now, set the parameter values and run the query.
            query.setParm(1, this.genomeId);
            if (this.projectId != null)
                query.setParm(2, this.projectId);
            var iter = query.iterator();
            // Write the output header.
            writer.println("sample_id\tpeg_count\tproject\tpubmed\tquality\tsuspicious\t" + this.mType);
            // Loop through the records.
            int count = 0;
            int goodCount = 0;
            int measureCount = 0;
            int bothCount = 0;
            while (iter.hasNext()) {
                var record = iter.next();
                var sampleId = record.getString("RnaSample.sample_id");
                var measured = measureMap.containsKey(sampleId);
                var good = ! record.getBool("RnaSample.suspicious");
                String measurement = (measured ? String.format("%8.6f", measureMap.get(sampleId)) : "");
                String pubmed = (record.isNull("RnaSample.pubmed") ? "" :
                    String.format("%d", record.getInt("Measurement.pubmed")));
                String suspicious = (good ? "" : "Y");
                writer.format("%s\t%d\t%s\t%s\t%6.2f\t%s\t%s%n", sampleId, record.getInt("RnaSample.feat_count"),
                        record.getReportString("RnaSample.project_id"), pubmed,
                        record.getDouble("RnaSample.quality"), suspicious, measurement);
                count++;
                if (measured) measureCount++;
                if (good) goodCount++;
                if (good && measured) bothCount++;
            }
            log.info("{} samples found. {} good, {} with measurements, {} good with measurements.", count,
                    goodCount, measureCount, bothCount);
        }

    }

}
