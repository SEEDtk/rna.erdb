/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.utils.PatternMap;

/**
 * This is the base class for commands that load the RnaSample table.
 *
 * The first positional parameters is the target genome ID.
 *
 * The built-in command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 * --ncbi		the name of a tab-delimited file with headers containing (0) the sample ID, (1) the NCBI project ID,
 * 				and (2) the PUBMED ID of the associated paper
 * --proj		the name of a tab-delimited file with headers containing (0) a regex for the sample ID, (1) the project
 * 				ID to use, and (2) the PUBMED ID of the associated paper
 * --replace	if specified, the new samples will be removed from the database before loading

 * @author Bruce Parrello
 *
 */
public abstract class BaseDbLoadProcessor extends BaseDbRnaProcessor {

    /**
     * This object describes project/paper information for a sample.
     */
    public static class ProjInfo {

        /** project ID, or NULL if none */
        private String projectId;
        /** pubmed ID, or 0 if none */
        private int pubmedId;

        /**
         * Create a project info record from the project and pubmed strings.
         *
         * @param project	project string, or empty if no project
         * @param pubmed	pubmed ID number, or empty if no associated paper
         */
        public ProjInfo(String project, String pubmed) {
            this.projectId = (StringUtils.isBlank(project) ? null : project);
            this.pubmedId = (StringUtils.isBlank(pubmed) ? 0 : Integer.valueOf(pubmed));
        }

        /**
         * Store this project information in the specified loader.
         *
         * @param jobLoader		sample record loader
         *
         * @throws SQLException
         */
        public void store(DbLoader jobLoader) throws SQLException {
            jobLoader.set("project_id", this.projectId);
            if (this.pubmedId == 0)
                jobLoader.setNull("pubmed");
            else
                jobLoader.set("pubmed",  this.pubmedId);
        }

    }

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(BaseDbLoadProcessor.class);
    /** map of sample IDs to project info */
    private Map<String, ProjInfo> stringMap;
    /** map of sample ID patterns to project info */
    private PatternMap<ProjInfo> patternMap;

    // COMMAND-LINE OPTIONS

    /** NCBI attribute file name */
    @Option(name = "--ncbi", metaVar = "srrReport.tbl", usage = "name of a file containing pubmed and project data from NCBI")
    private File ncbiFile;

    /** project pattern file name */
    @Option(name = "--proj", metaVar = "patterns.tbl", usage = "name of a file containing pubmed and project data based on sample ID regex patterns")
    private File patternFile;

    /** if specified, existing samples will be deleted before adding the new ones */
    @Option(name = "--replace", usage = "if specified, existing copies of the named samples will be deleted before loading")
    private boolean replaceFlag;

    @Override
    protected final void setDbDefaults() {
        this.ncbiFile = null;
        this.patternFile = null;
        this.replaceFlag = false;
        this.setDbLoadDefaults();
    }

    /**
     * Specify the parameter defaults for the subclass.
     */
    protected abstract void setDbLoadDefaults();

    @Override
    protected final void validateDbRnaParms() throws ParseFailureException, IOException {
        // Insure the genome ID is reasonable.
        this.validateGenomeId();
        // Process the project-data maps.
        this.readProjectFiles();
        // Do the subclass validation.
        this.validateDbLoadParms();
    }

    /**
     * Perform any parameter validation required by the subclass.
     *
     * @throws ParseFailureException
     * @throws IOException
     */
    protected abstract void validateDbLoadParms() throws ParseFailureException, IOException;

    /**
     * Read in the files of project and pubmed information and set up the project assignment maps.
     *
     * @throws IOException
     */
    protected void readProjectFiles() throws IOException {
        this.stringMap = new HashMap<String, ProjInfo>(1000);
        if (this.ncbiFile != null) {
            try (TabbedLineReader ncbiStream = new TabbedLineReader(this.ncbiFile)) {
                int keyCol = ncbiStream.findColumn("sample_id");
                int projCol = ncbiStream.findColumn("project");
                int pubCol = ncbiStream.findColumn("pubmed");
                for (TabbedLineReader.Line line : ncbiStream) {
                    String sampleId = line.get(keyCol);
                    String project = line.get(projCol);
                    String pubmed = line.get(pubCol);
                    this.stringMap.put(sampleId, new ProjInfo(project, pubmed));
                }
            }
            log.info("{} sample IDs found in {}.", this.stringMap.size(), this.ncbiFile);
        }
        this.patternMap = new PatternMap<ProjInfo>();
        if (this.patternFile != null) {
            try (TabbedLineReader patternStream = new TabbedLineReader(this.patternFile)) {
                int keyCol = patternStream.findColumn("sample_id");
                int projCol = patternStream.findColumn("project");
                int pubCol = patternStream.findColumn("pubmed");
                for (TabbedLineReader.Line line : patternStream) {
                    String pattern = line.get(keyCol);
                    String project = line.get(projCol);
                    String pubmed = line.get(pubCol);
                    this.patternMap.add(pattern, new ProjInfo(project, pubmed));
                }
            }
            log.info("{} sample patterns found in {}.", this.patternMap.size(), this.patternFile);
        }
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        // Get the index of features for the RNA expression arrays.
        this.buildFeatureIndex(db);
        // If we are replacing, we delete the samples first.  We do this outside of the main transaction, to avoid
        // locking problems.
        if (this.replaceFlag)
            this.deleteOldSamples(db);
        // Perform all the updates in a single transaction.
        try (var xact = db.new Transaction()) {
            this.loadSamples(db);
            // Commit the updates.
            xact.commit();
        }
    }

    /**
     * Load all the samples into the database,
     *
     * @param db	database connection to use for loading
     *
     * @throws Exception
     */
    protected abstract void loadSamples(DbConnection db) throws Exception;

    /**
     * Delete any copies of the RNA samples that might still be in the database.
     *
     * @param db	database from which the samples are to be deleted
     *
     * @throws SQLException
     */
    private void deleteOldSamples(DbConnection db) throws SQLException {
        try (var xact = db.new Transaction()) {
            // Get the list of sample IDs.
            Collection<String> samples = this.getNewSamples();
            log.info("Deleting existing copies of {} samples.", samples.size());
            db.deleteRecords("RnaSample", samples);
            // Lock the deletes.
            xact.commit();
        }
    }


    /**
     * @return the IDs of samples being loaded
     */
    protected abstract Collection<String> getNewSamples();

    /**
     * Initialize the RnaSample table loader with the genome ID and no sample cluster ID.
     *
     * @param jobLoader		RnaSample loader to initialize
     *
     * @throws SQLException
     */
    protected void initJobLoader(DbLoader jobLoader) throws SQLException {
        // We don't have cluster data, so null the cluster ID.
        jobLoader.setNull("cluster_id");
        // Point all samples at the target genome.
        jobLoader.set("genome_id", this.getGenomeId());
    }

    /**
     * Compute the project and pubmed information for the specified sample.
     *
     * @param jobLoader		loader for the RnaSample table, to be modified
     * @param sampleId		ID of the sample of interest
     * @throws SQLException
     */
    protected void computeProjectInfo(DbLoader jobLoader, String sampleId) throws SQLException {
        // First, check for an entry in the string map, then the pattern map.
        ProjInfo proj = this.stringMap.get(sampleId);
        if (proj == null)
            proj = this.patternMap.get(sampleId);
        if (proj != null)
            proj.store(jobLoader);
        else {
            jobLoader.setNull("project_id");
            jobLoader.setNull("pubmed");
        }
    }

}
