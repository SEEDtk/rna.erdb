/**
 *
 */
package org.theseed.modulons;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.io.CsvLineReader;
import org.theseed.io.TabbedLineReader;
import org.theseed.locations.BLocation;
import org.theseed.locations.FLocation;
import org.theseed.locations.Location;

/**
 * This class produces a hash of the features in the gene information file downloaded from the iModulon database.
 * The gene information file is a zip file, and the member "gene_info.csv" is a comma-separated value file with
 * a variable number of columns.  Of interest to us in particular is the location, which consists of a start, stop
 * and strand.  The start is in a column called "start", the stop in a column called "stop" or "end", and the strand
 * in column called "strand".  The strand can be coded as a character (+ or -) or a number (1 or -1).  In addition,
 * each row contains the gene name in "gene_name", a unique gene ID in the first (un-labeled) column, and an optional
 * operon name in a column called "operon".
 *
 * @author Bruce Parrello
 *
 */
public class GeneInfoFile {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(GeneInfoFile.class);
    /** default contig ID */
    private String contigId;
    /** index of gene-name column */
    private int nameIdx;
    /** index of start-offset column */
    private int startIdx;
    /** index of stop-offset column */
    private int stopIdx;
    /** index of operon ID column, or -1 if none */
    private int operonIdx;
    /** index of strand column */
    private int strandIdx;
    /** map of iModulon feature IDs to gene descriptors */
    private Map<String, Gene> geneMap;


    /**
     * This class tracks information on a single gene.
     */
    public class Gene {

        /** internal ID for the gene */
        private String id;
        /** gene name or NULL if none */
        private String name;
        /** location */
        private Location loc;
        /** operon ID (or NULL if none) */
        private String operon;
        /** modulon name set */
        private Set<String> modulon;

        /**
         * Construct a gene descriptor from an input line.
         *
         * @param line	input line to parse
         */
        protected Gene(TabbedLineReader.Line line) {
            this.id = line.get(0);
            this.name = line.get(GeneInfoFile.this.nameIdx);
            // If there is no gene name or it is the same as the ID, we null it out.
            if (StringUtils.isBlank(this.name) || this.name.contentEquals(id))
                this.name = null;
            // Form the location.  The input files tend to have different conventions, so we
            // need to sort the start and end.  Note also the strand is specified in multiple
            // ways, either +/- or 1/-1.
            int left = line.getInt(GeneInfoFile.this.startIdx);
            int right = line.getInt(GeneInfoFile.this.stopIdx);
            if (left > right) {
                int tmp = left;
                left = right;
                right = tmp;
            }
            String strand = line.get(GeneInfoFile.this.strandIdx);
            if (strand.contains("-"))
                this.loc = new BLocation(GeneInfoFile.this.contigId);
            else
                this.loc = new FLocation(GeneInfoFile.this.contigId);
            this.loc.putRegion(left, right);
            // Now check for an operon.
            if (GeneInfoFile.this.operonIdx >= 0)
                this.operon = line.get(GeneInfoFile.this.operonIdx);
            else
                this.operon = null;
            // Denote we have no modulon assigned.
            this.modulon = new TreeSet<String>();
        }

        /**
         * @return the modulon name set
         */
        public Set<String> getModulon() {
            return this.modulon;
        }

        /**
         * Specify the name of a new associated modulon
         *
         * @param modulon the modulon name to add
         */
        public void addModulon(String modName) {
            this.modulon.add(modName);
        }

        /**
         * @return the gene id
         */
        public String getId() {
            return this.id;
        }

        /**
         * @return the gene name
         */
        public String getName() {
            return this.name;
        }

        /**
         * @return the location
         */
        public Location getLoc() {
            return this.loc;
        }

        /**
         * @return the operon ID
         */
        public String getOperon() {
            return this.operon;
        }

    }

    /**
     * Construct a gene info file from the data in the specifed download.
     *
     * @param geneFile	name of the downloaded zip file
     * @param contig	contig ID to use
     *
     * @throws IOException
     * @throws ZipException
     */
    public GeneInfoFile(File geneFile, String contig) throws ZipException, IOException {
        this.contigId = contig;
        // Get an input stream for the gene_info file.
        log.info("Extracting gene information from {}.", geneFile);
        try (ZipFile controller = new ZipFile(geneFile)) {
            ZipEntry entry = controller.getEntry("gene_info.csv");
            InputStream geneStream = controller.getInputStream(entry);
            // Initialize the hash map.
            this.geneMap = new HashMap<String, Gene>(1000);
            // Open a tabbed line reader on the stream.  This will get us access to the columns by header name.
            try (var reader = new CsvLineReader(geneStream)) {
                // Compute the necessary column indices.  Note that "findField" throws an error if the column name is not found.
                this.startIdx = reader.findField("start");
                this.strandIdx = reader.findField("strand");
                this.nameIdx = reader.findField("gene_name");
                // This will be -1 if there is no operon column, which is what we want.
                this.operonIdx = reader.findColumn("operon");
                // For the stop column, there are two possible names.
                this.stopIdx = reader.findColumn("stop");
                if (this.stopIdx < 0)
                    this.stopIdx = reader.findField("end");
                // Now we loop through the gene file, creating gene objects.
                for (var line : reader) {
                    Gene gene = new Gene(line);
                    this.geneMap.put(gene.getId(), gene);
                }
            }
        }
        log.info("{} genes read from file.", this.geneMap.size());
    }


    /**
     * @return the gene descriptor for a specified internal ID, or NULL if there is none
     *
     * @param id	ID of desired gene
     */
    public Gene get(String id) {
        return this.geneMap.get(id);
    }

    /**
     * @return the full set of genes from the gene map
     */
    public Collection<Gene> getAll() {
        return this.geneMap.values();
    }

}
