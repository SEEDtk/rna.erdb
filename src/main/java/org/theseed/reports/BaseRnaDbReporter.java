/**
 *
 */
package org.theseed.reports;

import static j2html.TagCreator.rawHtml;
import static j2html.TagCreator.td;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbRecord;
import org.theseed.utils.ParseFailureException;

import j2html.tags.ContainerTag;

/**
 * This is the base class for RNA database reports.  Because these reports are extremely varied, there is
 * only setup and a main method.
 *
 * @author Bruce Parrello
 *
 */
public abstract class BaseRnaDbReporter {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BaseRnaDbReporter.class);
    /** source database */
    private DbConnection db;
    /** genome ID for the genome of interest */
    private String genomeId;
    /** URL for style sheet (DEBUG) */
    protected static final String CSS_HREF = "https://arcanestuff.com/css/erdb.css"; // "https://core.theseed.org/SEEDtk/css/erdb.css";
    /** URL format for pubmed IDs */
    protected static String PUBMED_URL = "https://pubmed.ncbi.nlm.nih.gov/%s/";
    /** HTML for a an empty table cell */
    private static final ContainerTag EMPTY_CELL = td(rawHtml("&nbsp;"));

    /**
     * This interface is used by the reports to get additional parameters from the controlling command processor
     */
    public interface IParms {

        /**
         * @return the ID of the genome of interest
         */
        public String getGenomeId();

        /**
         * @return the ID of the sample of interest
         */
        public String getSampleId();

        /**
         * @return the set of gene aliases for a gene data report, or NULL to allow all genes
         */
        public Set<String> getGeneFilter();

        /**
         * @return the measurement type of interest
         */
        public String getMeasureType();

        /**
         * @return the project of interest
         */
        public String getProjectId();

        /**
         * @return the name of the sample correlation file
         */
        public File getSampleCorrFile();

    }

    /**
     * This enumeration lists the report types.
     */
    public enum Type {
        /** display statistics for each feature cluster */
        FID_CLUSTER_SUMMARY {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new FeatureClusterSummaryReporter(processor, db);
            }
        },
        /** feature cluster web page */
        FEATURE_CLUSTER {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new FeatureClusterReporter(processor, db);
            }
        },
        /** list the value distribution of expression levels for features */
        NORMAL_CHECK {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new NormalCheckReporter(processor, db);
            }
        },
        /** generate a gene data load file from a sample that can be used with an Escher map */
        GENE_DATA {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new GeneDataReporter(processor, db);
            }
        },
        /** list the available samples for a genome */
        SAMPLES {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new SampleListReporter(processor, db);
            }
        },
        /** list all the samples for a genome and include a specified measurement value */
        MEASURED {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new SampleMeasureReporter(processor, db);
            }
        },
        /** generate a web page of a genome's samples with links to the pubmed articles (when available) */
        PUBMED_LINKS {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) {
                return new PubmedLinkReporter(processor, db);
            }
        },
        /** generate a summary report for the sample clusters */
        SAMPLE_CLUSTER_SUMMARY {
            @Override
            public BaseRnaDbReporter create(IParms processor, DbConnection db) throws ParseFailureException, IOException {
                return new SampleClusterReporter(processor, db);
            }
        };

        /**
         * @return a reporter of this type
         *
         * @param processor		controlling command processor
         * @param db			source database connection
         *
         * @throws IOException
         * @throws ParseFailureException
         */
        public abstract BaseRnaDbReporter create(IParms processor, DbConnection db) throws IOException, ParseFailureException;

    }

    /**
     * Construct a new RNA database reporter.
     *
     * @param processor		controlling command processor
     * @param db			source database connection
     */
    protected BaseRnaDbReporter(IParms processor, DbConnection db) {
        // Get the database.
        this.db = db;
        // Get the target genome ID.
        this.genomeId = processor.getGenomeId();

    }

    /**
     * Write the report.
     *
     * @param writer	print writer for the report
     *
     * @throws IOException
     * @throws SQLException
     */
    public abstract void writeReport(PrintWriter writer) throws IOException, SQLException;

    /**
     * @return the database connection
     */
    protected DbConnection getDb() {
        return this.db;
    }

    /**
     * @return the ID of the genome of interest
     */
    protected String getGenomeId() {
        return this.genomeId;
    }

    /**
     * @return a feature index for the genome of interest
     *
     * @throws SQLException
     */
    protected FeatureIndex getFeatureIndex() throws SQLException {
        return new FeatureIndex(this.db, this.genomeId);
    }

    /**
     * @return the name of the base genome
     *
     * @throws SQLException
     */
    protected String getGenomeName() throws SQLException {
        DbRecord genome = db.getRecord("Genome", genomeId);
        String retVal = genome.getReportString("Genome.genome_name");
        return retVal;
    }

    /**
     * This is a utility method for building a table cell.  If the incoming string is null or blank,
     * it will put in a non-breaking space.
     *
     * @param content	proposed content for a table cell
     *
     * @return a table cell containing the content
     */
    protected static ContainerTag safe_td(String content) {
        ContainerTag retVal;
        if (StringUtils.isBlank(content))
            retVal = EMPTY_CELL;
        else
            retVal = td(content);
        return retVal;
    }

}
