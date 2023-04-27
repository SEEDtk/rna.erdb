/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.TabbedLineReader;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbLoader;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.Relop;
import org.theseed.utils.ParseFailureException;

/**
 * This command loads sample data downloaded from PATRIC directly into an RNA Seq database.
 *
 * The positional parameters are the ID of the reference genome and the name of the directory
 * containing the tpm and html report files.
 *
 * The command-line options are as follows.
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
 * --qual		minimum percent quality required for a good sample
 * --min		minimum representation required for a good sample
 * --clear		erase all existing samples for the genome
 *
 * @author Bruce Parrello
 *
 */
public class SampleUploadProcessor extends BaseDbLoadProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleUploadProcessor.class);
    /** set of samples to process */
    private Set<String> sampleSet;
    /** pattern for extracting sample ID */
    private static final Pattern TPM_FILE_PATTERN = Pattern.compile("(.+)_genes\\.tpm");
    /** parser for SAMSTAT description */
    protected static final Pattern SAMSTAT_STATS_LINE = Pattern.compile("(\\d+) reads,\\s+size:\\s*(\\d+).+created\\s+(\\d+-\\d+-\\d+).*");

    // COMMAND-LINE OPTIONS

    /** minimum percent features that must be represented for a sample to be considered good */
    @Option(name = "--min", metaVar = "50.0", usage = "minimum percent of features that must have valid levels")
    private double minFeaturePct;

    /** minimum percent quality that must be represented for a sample to be considered good */
    @Option(name = "--qual", metaVar = "80.0", usage = "minimum percent of mappings that must be high-quality")
    private double minQualPct;

    /** if specified, all the samples for the genome will be deleted before loading */
    @Option(name = "--clear", usage = "erase existing samples before loading")
    private boolean clearFlag;

    /** name of the input directory containing the RNA Seq Utility output files */
    @Argument(index = 1, metaVar = "sampleDir", usage = "directory containing the TPM and SAMSTAT files for the samples",
            required = true)
    private File inDir;

    @Override
    protected void setDbLoadDefaults() {
        this.minFeaturePct = 40.0;
        this.minQualPct = 50.0;
        this.clearFlag = false;
    }

    @Override
    protected void validateDbLoadParms() throws ParseFailureException, IOException {
        if (! this.inDir.isDirectory())
            throw new FileNotFoundException("Input directory " + this.inDir + " is not found or invalid.");
        // Validate the quality cutoffs.
        if (this.minFeaturePct <= 0.0 || minFeaturePct > 100.0)
            throw new FileNotFoundException("Minimum feature percent must be between 0 and 100.");
        if (this.minFeaturePct <= 0.0 || minQualPct > 100.0)
            throw new FileNotFoundException("Minimum quality percent must be between 0 and 100.");
        // Now we need to parse the directory for the sample IDs.
        File[] inFiles = this.inDir.listFiles();
        this.sampleSet = Arrays.stream(inFiles).map(f -> TPM_FILE_PATTERN.matcher(f.getName())).filter(m -> m.matches())
                .map(m -> m.group(1)).collect(Collectors.toSet());
        log.info("{} samples found in {}.", this.sampleSet.size(), this.inDir);
    }

    @Override
    protected void loadSamples(DbConnection db) throws Exception {
        // Check here to see if we need to delete existing samples.
        if (this.clearFlag) {
            try (var xact = db.new Transaction(); var query = new DbQuery(db, "RnaSample")) {
                // Get all the samples for this genome.
                Set<String> samples = new HashSet<String>(1000);
                log.info("Deleting old RNA samples for {}.", this.getGenomeId());
                query.select("RnaSample", "sample_id");
                query.rel("RnaSample.genome_id", Relop.EQ);
                query.setParm(1, this.getGenomeId());
                var iter = query.iterator();
                while (iter.hasNext())
                    samples.add(iter.next().getString("RnaSample.sample_id"));
                log.info("{} samples to delete.", samples.size());
                // Loop through the samples, deleting them.
                int count = 0;
                for (var sample : samples) {
                    db.deleteRecord("RnaSample", sample);
                    count++;
                    if (count % 100 == 0)
                        log.info("{} samples deleted.", count);
                }
                xact.commit();
                log.info("{} samples deleted for genome {}.", count, this.getGenomeId());
            }
        }
        // Now process the loading.
        try (DbLoader jobLoader = DbLoader.batch(db, "RnaSample")) {
            // Store the genome ID and cluster ID in the loader.  These are
            // always the same.
            jobLoader.set("genome_id", this.getGenomeId());
            jobLoader.setNull("cluster_id");
            // We count the samples processed and the ones skipped.
            int count = 0;
            int skipped = 0;
            int badSamples = 0;
            final int nSamples = this.sampleSet.size();
            // Loop through the samples.
            for (String sampleId : this.sampleSet) {
                count++;
                // Set the sample ID.
                jobLoader.set("sample_id", sampleId);
                // Get the data files.
                File samstatFile = new File(this.inDir, sampleId + ".samstat.html");
                File tpmFile = new File(this.inDir, sampleId + "_genes.tpm");
                // We know the TPM file exists because we used it to find the sample ID.  Check the
                // SAMSTAT file.
                if (! samstatFile.exists()) {
                    log.warn("No SAMSTAT file found for sample {}.", sampleId);
                    skipped++;
                } else {
                    log.info("Processing sample {} of {}: {}.", count, nSamples, sampleId);
                    // Find the data.
                    try {
                        double qual = this.processSamstat(jobLoader, samstatFile);
                        double representation = this.processTpm(jobLoader, tpmFile);
                        // Analyze the sample quality.
                        boolean suspicious = (qual < this.minQualPct || representation < this.minFeaturePct);
                        jobLoader.set("suspicious", suspicious);
                        if (suspicious) {
                            log.warn("Sample {} is suspicious: qual = {}, representation = {}.",
                                    sampleId, qual, representation);
                            badSamples++;
                        }
                        // Set the project and the pubmed.
                        this.computeProjectInfo(jobLoader, sampleId);
                        // Insert the sample.
                        jobLoader.insert();
                    } catch (IOException e) {
                        log.error("Bad output for sample {}: {}", sampleId, e.toString());
                        skipped++;
                    }
                }
            }
            log.info("{} samples processed, {} skipped, {} were bad.", count, skipped, badSamples);
        }
    }

    /**
     * Fill the table loader with data from the TPM file.
     *
     * @param jobLoader		loader for the RnaSample table
     * @param tpmFile		file containing the TPM expression results
     *
     * @return the percent of features expressed
     *
     * @throws IOException
     * @throws SQLException
     */
    private double processTpm(DbLoader jobLoader, File tpmFile) throws IOException, SQLException {
        double retVal = 0.0;
        try (TabbedLineReader inStream = new TabbedLineReader(tpmFile)) {
            // The main content of this file is the expression levels (feat_data, feat_count).
            // Each feature is on a line by itself, and we store its level in the following array.
            final int n = this.getFeatureCount();
            // Get the columns of the key data fields.  Note that the result column name is no longer
            // constant, but its position is fixed.
            int fidCol = inStream.findField("Gene_id");
            int tpmCol = 1;
            // First, create a map of feature IDs to expression levels.
            Map<String, Double> levels = new HashMap<String, Double>(n * 4 / 3);
            for (TabbedLineReader.Line line : inStream) {
                double tpm = line.getDouble(tpmCol);
                if (tpm > 0.0) {
                    String fid = line.get(fidCol);
                    levels.put(fid, tpm);
                }
            }
            // The feature count and percent are computed from the map size.
            jobLoader.set("feat_count", levels.size());
            retVal = levels.size() * 100.0 / n;
            // Create the feature-data array.
            double[] featData = IntStream.range(0, n)
                    .mapToDouble(i -> levels.getOrDefault(this.getFeatureId(i), Double.NaN))
                    .toArray();
            jobLoader.set("feat_data", featData);
        }
        return retVal;
    }

    /**
     * Fill the table loader with data from the SAMSTAT file.
     *
     * @param jobLoader		loader for the RnaSample table
     * @param samstatFile	file containing the SAMSTAT report Html
     *
     * @return the percent of mappings with high quality
     *
     * @throws IOException
     * @throws SQLException
     */
    private double processSamstat(DbLoader jobLoader, File samstatFile) throws IOException, SQLException {
        Document samStat = Jsoup.parse(samstatFile, StandardCharsets.UTF_8.toString());
        // First, we get the counts and the date from the description line.
        Element description = samStat.selectFirst("h1 + p");
        if (description == null)
            throw new IOException("Missing description string in " + samstatFile + ".");
        String descriptionText = description.text();
        Matcher m = SAMSTAT_STATS_LINE.matcher(descriptionText);
        if (! m.matches())
            throw new IOException("Invalid description string in " + samstatFile + ".");
        int readCount = Integer.valueOf(m.group(1));
        double baseCount = (double) Long.valueOf(m.group(2));
        LocalDate procDate = LocalDate.parse(m.group(3));
        jobLoader.set("read_count", readCount);
        jobLoader.set("base_count", baseCount);
        jobLoader.set("process_date", procDate);
        // Now get the quality table.
        Element qualCell = samStat.selectFirst("tr:contains(MAPQ >= 30) > td:last-of-type");
        if (qualCell == null)
            throw new IOException("Missing quality table row for MAPQ >+ 30 in " + samstatFile + ".");
        double retVal = Double.valueOf(qualCell.text());
        jobLoader.set("quality", retVal);
        return retVal;
    }

    @Override
    protected Collection<String> getNewSamples() {
        return this.sampleSet;
    }

}
