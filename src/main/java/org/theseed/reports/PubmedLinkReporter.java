/**
 *
 */
package org.theseed.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;

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
    /** array of header names for table; this is also the list of field names */
    private String[] headers = new String[] { "sample_id", "base_count", "feat_count", "read_count", "quality", "project_id", "pubmed" };
    /** array of styles for table */
    private String[] styles = new String[] { "text", "num", "num", "num", "num", "text", "text" };
    /** index of pubmed link column */
    private int linkIdx;

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
        // Find the index of the pubmed column link column.

    }

    @Override
    public void writeReport(PrintWriter writer) throws IOException, SQLException {
        // We will be creating an HTML table, then putting it in a simple document.
        ContainerTag table;

        // TODO code for pubmed link report
    }

}
