/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;

/**
 * This report displays a list of the samples in the database.  The sample list is restricted to a single base
 * genome.
 *
 * @author Bruce Parrello
 *
 */
public class SampleListReporter extends BaseRnaDbReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleListReporter.class);
    /** target genome ID */
    private String genomeId;
    /** list of fields to display */
    private static final String[] FIELD_LIST = new String[] { "sample_id", "base_count", "feat_count",
            "process_date", "quality", "read_count" };
    /** output format for a line */
    private static final String LINE_FORMAT = "%s\t%d\t%d\t%s\t%6.2f\t%d%n";

    public SampleListReporter(IParms processor, DbConnection db) {
        super(processor, db);
        this.genomeId = processor.getGenomeId();
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        writer.println(StringUtils.join(FIELD_LIST, '\t'));
        try (DbQuery query = new DbQuery(this.getDb(), "RnaSample")) {
            query.select("RnaSample", FIELD_LIST);
            query.rel("RnaSample.genome_id", Relop.EQ);
            query.setParm(1, this.genomeId);
            query.stream().forEach(x -> this.showSample(writer, x));
        }
    }

    /**
     * Write a sample's data to the output.
     *
     * @param writer	output writer to receive the report line
     * @param record	sample record to write
     */
    private void showSample(PrintWriter writer, DbRecord record) {
        try {
            // Format the process date.
            var processDate = record.getDate("RnaSample.process_date");
            String dateString = processDate.toString();
            // Get the counts.
            int baseCount = record.getInt("RnaSample.base_count");
            int featCount = record.getInt("RnaSample.feat_count");
            int readCount = record.getInt("RnaSample.read_count");
            double quality = record.getDouble("RnaSample.quality");
            // Write the data.
            writer.format(LINE_FORMAT, record.getString("RnaSample.sample_id"), baseCount, featCount,
                    dateString, quality, readCount);
        } catch (SQLException e) {
            // Convert the exception to something unchecked so we can use this method in a stream.
            throw new RuntimeException(e);
        }
    }

}
