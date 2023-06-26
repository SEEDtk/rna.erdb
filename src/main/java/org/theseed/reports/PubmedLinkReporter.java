/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;
import org.theseed.java.erdb.Relop;

import j2html.tags.ContainerTag;

import static j2html.TagCreator.*;

/**
 * This RNA sample report generates a web page showing all the samples for a genome with basic statistics and a link
 * to the associated PUBMED article page when available.
 *
 * @author Bruce Parrello
 *
 */
public class PubmedLinkReporter extends BaseRnaDbReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(PubmedLinkReporter.class);
    /** RNA database to use */
    private DbConnection db;
    /** ID for genome of interest */
    private String genomeId;
    /** list of cell specifiers for building each row of the table */
    private static final List<CellFormatter> COLUMNS = List.of(
            new CellFormatter.Text("Sample ID", "RnaSample.sample_id"),
            new CellFormatter.Text("Project ID", "RnaSample.project_id"),
            new CellFormatter.PubMed("Pubmed Link", "RnaSample.pubmed"),
            new CellFormatter.Count("Base Pairs", "RnaSample.base_count"),
            new CellFormatter.Count("Features Mapped", "RnaSample.feat_count"),
            new CellFormatter.Count("Num Reads", "RnaSample.read_count"),
            new CellFormatter.Percent("% Quality", "RnaSample.quality")
        );

    /**
     * Construct a pubmed link report.
     *
     * @param processor		controlling command processor
     * @param db			RNA expression database
     */
    public PubmedLinkReporter(IParms processor, DbConnection db) {
        super(processor, db);
        this.db = db;
        this.genomeId = processor.getGenomeId();
    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        // We will be creating an HTML table, then putting it in a simple document.  We start
        // by putting in the header row.  Note that the table is mostly built using the
        // column formatter list.
        log.info("Initializing output table.");
        ContainerTag table = table().withStyle("big");
        ContainerTag headerRow = tr().with(COLUMNS.stream().map(x -> x.getHeader()));
        table.with(headerRow);
        // Now create the query and begin looping through the samples.
        log.info("Building database query for {} samples.", this.genomeId);
        try (DbQuery query = new DbQuery(this.db, "RnaSample")) {
            // Insure we have the fields we need.
            CellFormatter.getSelectInfo(query, COLUMNS);
            // Filter for the target genome.
            query.rel("RnaSample.genome_id", Relop.EQ);
            query.setParm(1, this.genomeId);
            // Loop through the samples, creating a table row for each one.
            int dbCount = 0;
            var iter = query.iterator();
            while (iter.hasNext()) {
                DbRecord record = iter.next();
                ContainerTag row = tr().with(COLUMNS.stream().map(x -> x.getCell(record)));
                table.with(row);
                dbCount++;
            }
            log.info("{} samples found for {}.", dbCount, this.genomeId);
            // Format a web page for the table.
            ContainerTag head = head().with(title("Sample List for " + this.getGenomeId()))
                    .with(link().withHref(CSS_HREF).withRel("stylesheet").withType("text/css"));
            ContainerTag heading = h1("Sample List for " + this.getGenomeName());
            ContainerTag body = body().with(heading).with(table);
            ContainerTag page = html().with(head, body);
            log.info("Writing web page.");
            writer.println(page.renderFormatted());
        }

    }

}
