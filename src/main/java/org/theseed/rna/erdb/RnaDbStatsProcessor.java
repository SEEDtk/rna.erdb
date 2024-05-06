/**
 *
 */
package org.theseed.rna.erdb;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.erdb.utils.BaseDbReportProcessor;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbRecord;

/**
 *
 * This is a simple command that lists the genomes in the database and the number of samples for each.
 *
 * The report is written to the standard output.  The command-line options are as follows.
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 * -o	output file (if not STDOUT)
 *
 * --type		type of database (default SQLITE)
 * --dbfile		database file name (SQLITE only)
 * --url		URL of database (host and name)
 * --parms		database connection parameter string (currently only MySQL)
 *
 * @author Bruce Parrello
 *
 */
public class RnaDbStatsProcessor extends BaseDbReportProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaDbStatsProcessor.class);
    /** map of genome IDs to descriptors */
    private Map<String, GenomeData> gMap;

    /**
     * This descriptor contains the data we want to remember about each genome.
     */
    protected class GenomeData {

        /** genome ID */
        private String genomeId;
        /** genome name */
        private String name;
        /** number of pegs */
        private int pegCount;
        /** number of good samples */
        private int goodSamples;
        /** number of suspicious samples */
        private int badSamples;
        /** number of reads in all the samples */
        private long readCount;

        /**
         * Construct a genome descriptor from a genome database record.
         *
         * @param gRecord 	database record
         *
         * @throws SQLException
         */
        protected GenomeData(DbRecord gRecord) throws SQLException {
            this.genomeId = gRecord.getString("Genome.genome_id");
            this.name = gRecord.getString("Genome.genome_name");
            this.pegCount = gRecord.getInt("Genome.peg_count");
            this.goodSamples = 0;
            this.badSamples = 0;
        }

        /**
         * @return the genome ID
         */
        public String getId() {
            return this.genomeId;
        }

        /**
         * @return the genome name
         */
        public String getName() {
            return this.name;
        }

        /**
         * @return the number of CDS features in the genome
         */
        public int getPegCount() {
            return this.pegCount;
        }

        /**
         * @return the number of good samples
         */
        public int getGoodSamples() {
            return this.goodSamples;
        }

        /**
         * @return the number of bad samples
         */
        public int getBadSamples() {
            return this.badSamples;
        }

        /**
         * @return the total number of samples
         */
        public int getSamples() {
            return this.goodSamples + this.badSamples;
        }

        /**
         * Mean number of reads per sample.
         */
        public long meanReadCount() {
            long retVal;
            if (this.readCount == 0)
                retVal = 0L;
            else {
                double mean = ((double) this.readCount) / (this.badSamples + this.goodSamples);
                retVal = (long) Math.round(mean);
            }
            return retVal;
        }

        /**
         * Update the genome descriptor based on the data in a sample.
         *
         * @param sData		sample data record
         *
         * @throws SQLException
         */
        public void update(DbRecord sData) throws SQLException {
            if (sData.getBool("RnaSample.suspicious"))
                this.badSamples++;
            else
                this.goodSamples++;
            this.readCount += sData.getInt("RnaSample.read_count");
        }

    }


    @Override
    protected void setReporterDefaults() {
    }

    @Override
    protected void validateReporterParms() throws IOException, ParseFailureException {
    }

    @Override
    protected void runDbReporter(DbConnection db, PrintWriter writer) throws Exception {
        // Initialize the genome map.
        this.gMap = new TreeMap<String, GenomeData>();
        // First we get the list of genomes.
        log.info("Reading genome list.");
        try (DbQuery query = new DbQuery(db, "Genome")) {
            query.select("Genome", "genome_id", "genome_name", "peg_count");
            Iterator<DbRecord> iter = query.iterator();
            while (iter.hasNext()) {
                GenomeData gData = new GenomeData(iter.next());
                this.gMap.put(gData.getId(), gData);
            }
            log.info("{} genomes found in database.", this.gMap.size());
        }
        // Create the sample query.
        log.info("Counting samples.");
        int sampCount = 0;
        try (DbQuery query = new DbQuery(db, "RnaSample")) {
            query.select("RnaSample", "genome_id", "read_count", "suspicious");
            Iterator<DbRecord> iter = query.iterator();
            while (iter.hasNext()) {
                sampCount++;
                DbRecord sData = iter.next();
                String genomeId = sData.getString("RnaSample.genome_id");
                GenomeData gData = this.gMap.get(genomeId);
                gData.update(sData);
            }
            log.info("{} total samples processed.", sampCount);
        }
        // Create the report.
        writer.println("genome_id\tgenome_name\tpegs\tgood_samples\tbad_samples\treads/sample");
        for (GenomeData gData : gMap.values()) {
            writer.println(gData.getId() + "\t" + gData.getName() + "\t" + gData.getPegCount() + "\t" + gData.getGoodSamples()
                    + "\t" + gData.getBadSamples() + "\t" + gData.meanReadCount());
        }
    }

}
