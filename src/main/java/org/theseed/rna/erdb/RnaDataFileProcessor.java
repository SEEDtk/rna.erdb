/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.Relop;
import org.theseed.rna.RnaData;
import org.theseed.utils.ParseFailureException;

/**
 * This command will output a project from the RNA Seq database to an old-format RNA data file.
 *
 * The positional parameters are the ID of the target genome, the ID of the project, the name of a GTO
 * for the target genome, and the name of the output file for the RNA database.
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 *
 * @author Bruce Parrello
 *
 */
public class RnaDataFileProcessor extends BaseDbRnaProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaDataFileProcessor.class);
    /** rna data file */
    private RnaData data;
    /** target genome */
    private Genome genome;
    /** genome features sorted by location */
    private TreeSet<Feature> featuresByLocation;
    /** default threonine data (no values) */
    private static final ThrData DEFAULT_THRDATA = new ThrData();

    // COMMAND-LINE OPTIONS

    /** project ID */
    @Argument(index = 1, metaVar = "PROJECT_ID", usage = "ID of the project to store in the data file", required = true)
    private String projectId;

    /** GTO file for target genome */
    @Argument(index = 2, metaVar = "genome.gto", usage = "GTO file for the target genome", required = true)
    private File gtoFile;

    /** output file name */
    @Argument(index = 3, metaVar = "rnaData.ser", usage = "output file in which to store project RNA data", required = true)
    private File outFile;

    // UTILITY CLASSES

    /**
     * This object contains the production and optical density data for a sample.
     */
    protected static class ThrData {

        /** threonine production */
        private double prod;
        /** optical density */
        private double dens;

        /**
         * Create a data object.
         */
        public ThrData() {
            this.prod = Double.NaN;
            this.dens = Double.NaN;
        }

        /**
         * @return the threonine production level
         */
        public double getProd() {
            return this.prod;
        }

        /**
         * Specify the threonine production level.
         *
         * @param prod 		production level to set
         */
        public void setProd(double prod) {
            this.prod = prod;
        }

        /**
         * @return the optical density
         */
        public double getDens() {
            return this.dens;
        }

        /**
         * Specify the optical density.
         *
         * @param dens 		optical density to set
         */
        public void setDens(double dens) {
            this.dens = dens;
        }

    }

    // METHODS

    @Override
    protected void setDbDefaults() {
    }

    @Override
    protected void validateDbRnaParms() throws ParseFailureException, IOException {
        // Load the target genome.
        if (! this.gtoFile.canRead())
            throw new FileNotFoundException("GTO file " + this.gtoFile + " is not found or unreadable.");
        log.info("Reading genome from {}.", this.gtoFile);
        this.genome = new Genome(this.gtoFile);
        if (! this.genome.getId().contentEquals(this.getGenomeId()))
            throw new ParseFailureException("Genome " + this.genome.getId() + " found in file, but genome " +
                    this.getGenomeId() + " requested from database.");
        // Create the sorted-by-location map.
        this.featuresByLocation = new TreeSet<Feature>(new Feature.LocationComparator());
        this.genome.getPegs().stream().forEach(x -> this.featuresByLocation.add(x));
        log.info("{} proteins found in genome file.", this.featuresByLocation.size());
    }

    @Override
    protected void runDbCommand(DbConnection db) throws Exception {
        // Insure we have a feature index.
        this.buildFeatureIndex(db);
        // Create the RNA data structure.
        this.data = new RnaData();
        // Get the production and optical density measurements for the project.
        log.info("Recording measurements for project {}.", this.projectId);
        Map<String, ThrData> thrDataMap = new HashMap<String, ThrData>(1000);
        try (DbQuery query = new DbQuery(db, "RnaSample Measurement")) {
            query.select("Measurement", "measure_type", "sample_id", "value");
            query.rel("RnaSample.project_id", Relop.EQ);
            query.setParm(1, this.projectId);
            var iter = query.iterator();
            int valCount = 0;
            while (iter.hasNext()) {
                var measureRecord = iter.next();
                var thrData = thrDataMap.computeIfAbsent(measureRecord.getString("Measurement.sample_id"),
                        x -> new ThrData());
                double val = measureRecord.getDouble("Measurement.value");
                switch (measureRecord.getString("Measurement.measure_type")) {
                case "thr_g/L" :
                    thrData.setProd(val);
                    valCount++;
                    break;
                case "density" :
                    thrData.setDens(val);
                    valCount++;
                    break;
                }
            }
            log.info("{} measurements stored for {} samples.", valCount, thrDataMap.size());
        }
        // Get the number of features in each sample.
        final int fCount = this.getFeatureCount();
        // Create the expression map.  This maps each sample ID to its TPM array.
        var expressionMap = new HashMap<String, double[]>(1000);
        // Loop through the samples in the project.
        log.info("Building query for project {} samples.", this.projectId);
        try (DbQuery query = new DbQuery(db, "RnaSample")) {
            query.select("RnaSample", "sample_id", "base_count", "feat_count", "feat_data",
                    "process_date", "quality", "read_count", "suspicious");
            query.rel("RnaSample.project_id", Relop.EQ);
            query.rel("RnaSample.genome_id", Relop.EQ);
            query.setParm(1, this.projectId, this.getGenomeId());
            var iter = query.iterator();
            while (iter.hasNext()) {
                var sampleRecord = iter.next();
                // Get the sample ID and suspicion flag.
                String sampleId = sampleRecord.getString("RnaSample.sample_id");
                boolean suspectFlag = sampleRecord.getBool("RnaSample.suspicious");
                // Get the measurements.
                ThrData thrData = thrDataMap.getOrDefault(sampleId, DEFAULT_THRDATA);
                // Create a job for this sample.
                var job = this.data.addJob(sampleId, thrData.getProd(), thrData.getDens(), "", suspectFlag);
                // Update the metadata fields.
                job.setBaseCount((long) sampleRecord.getDouble("RnaSample.base_count"));
                job.setProcessingDate(LocalDate.ofInstant(sampleRecord.getDate("RnaSample.process_date"), ZoneId.systemDefault()));
                job.setQuality(sampleRecord.getDouble("RnaSample.quality"));
                job.setReadCount(sampleRecord.getInt("RnaSample.read_count"));
                // Save the expression levels.
                expressionMap.put(sampleId, sampleRecord.getDoubleArray("RnaSample.feat_data"));
            }
            log.info("{} samples initialized.", this.data.size());
        }
        // Now store the expression levels.  The jobs have to all be present before we can do this.
        for (var expressionEntry : expressionMap.entrySet()) {
            String sampleId = expressionEntry.getKey();
            double[] tpms = expressionEntry.getValue();
            int tpmCount = 0;
            for (int i = 0; i < fCount; i++) {
                double tpm = tpms[i];
                if (Double.isFinite(tpm)) {
                    // Here we have an expression value for this feature.
                    String fid = this.getFeatureId(i);
                    Feature feat = this.genome.getFeature(fid);
                    Feature neighbor = this.featuresByLocation.higher(feat);
                    // Ignore the neighbor if it is on a different contig.
                    if (neighbor != null && neighbor.getLocation().isContig(feat.getLocation().getContigId()))
                        neighbor = null;
                    // Find the row for this feature and store the TPM level as an exact hit.
                    var row = this.data.getRow(feat, neighbor);
                    row.store(sampleId, true, tpm);
                    tpmCount++;
                }
            }
            log.info("{} TPM values stored for {}.", tpmCount, sampleId);
        }
        // Now we need to store the feature groups.  The operons and atomic regulons are easy, but
        // there may be more than one modulon per feature.  We accumulate the modulons in this map
        // (keyed on feature ID) and unspool them afterward.
        Map<String, List<String>> modulonMap = new HashMap<String, List<String>>(this.featuresByLocation.size() * 3 / 2 + 1);
        log.info("Storing feature metadata.");
        // Query the feature groups.
        int pairsFound = 0;
        int pairsSkipped = 0;
        int arStored = 0;
        int opStored = 0;
        int modStored = 0;
        try (DbQuery query = new DbQuery(db, "Feature FeatureToGroup FeatureGroup")) {
            query.select("Feature", "fig_id", "baseline");
            query.select("FeatureGroup", "group_type", "group_name");
            query.rel("Feature.genome_id", Relop.EQ);
            query.in("FeatureGroup.group_type", 3);
            query.setParm(1, this.getGenomeId());
            query.setParm(2, "operon", "modulon", "regulon");
            var iter = query.iterator();
            while (iter.hasNext()) {
                var record = iter.next();
                String fid = record.getString("Feature.fig_id");
                double baseline = record.getDouble("Feature.baseline");
                pairsFound++;
                // Get the feature data.  Only proceed if the feature is found.  If it isn't found, there is
                // no data for it.
                var row = this.data.getRow(fid);
                if (row == null)
                    pairsSkipped++;
                else {
                    // Store the baseline.  This may be redundant, but it is faster than any method we have for
                    // eliminating the redundancy.
                    var feat = row.getFeat();
                    feat.setBaseLine(baseline);
                    // Store the grouping.
                    String name = record.getString("FeatureGroup.group_name");
                    switch (record.getString("FeatureGroup.group_type")) {
                    case "modulon" :
                        // Store the modulon in the modulon list map.
                        var modList = modulonMap.computeIfAbsent(fid, x -> new ArrayList<String>(5));
                        modList.add(name);
                        modStored++;
                        break;
                    case "operon" :
                        // Store the operon in the feature data.
                        feat.setOperon(name);
                        opStored++;
                        break;
                    case "regulon" :
                        // Parse the regulon (of the form AR###) and store it in the feature data.
                        int regNum = Integer.valueOf(name.substring(2));
                        feat.setAtomicRegulon(regNum);
                        arStored++;
                        break;
                    }
                }
            }
        }
        log.info("{} pairings found, {} skipped due to unused feature, {} regulons, {} operons, {} modulons.",
                pairsFound, pairsSkipped, arStored, opStored, modStored);
        // Unspool the modulons from the modulon map.
        for (var modEntry : modulonMap.entrySet()) {
            String fid = modEntry.getKey();
            String mods = StringUtils.join(modEntry.getValue(), ',');
            var feat = this.data.getRow(fid).getFeat();
            feat.setiModulons(mods);
        }
        log.info("{} features had modulons.", modulonMap.size());
        this.data.updateQuality();
        log.info("Quality data updated.");
        log.info("Saving data to {}.", this.outFile);
        this.data.save(this.outFile);
    }

}
