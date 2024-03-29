/**
 *
 */
package org.theseed.rna.jobs;

import java.io.File;
import java.util.List;

import org.theseed.cli.CliService;
import org.theseed.cli.CliTaskException;
import org.theseed.cli.DirEntry;
import org.theseed.cli.DirTask;
import org.theseed.cli.RnaSource;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This service submits the request to align the trimmed FASTQ files to the target genome.
 *
 * @author Bruce Parrello
 */
public class AlignService extends CliService {

    // FIELDS
    /** parameters for the service */
    private JsonObject parms;

    /**
     * Construct the alignment service request.
     *
     * @param job			job for which this service performs the trimming phase
     * @param workDir		wokring directory for temporary files
     * @param workspace		controlling workspace
     */
    public AlignService(RnaJob job, File workDir, String workspace) {
        super(job.getName(), workDir, workspace);
        // Compute the output directory from the trim phase.
        String outPath = job.getOutDir() + "/." + RnaJob.Phase.TRIM.getOutputName(job.getName());
        // List the FASTQ files.  There should only be two.
        DirTask lister = new DirTask(workDir, workspace);
        List<DirEntry> outFiles = lister.list(outPath);
        String leftFile = null;
        String rightFile = null;
        for (DirEntry entry : outFiles) {
            if (entry.getType() == DirEntry.Type.READS) {
                // Store this file.
                if (leftFile == null)
                    leftFile = outPath + "/" + entry.getName();
                else if (rightFile == null)
                    rightFile = outPath + "/" + entry.getName();
                else
                    throw new ArrayIndexOutOfBoundsException("Too many FASTQ files in directory " + outPath + ".");
            }
        }
        if (leftFile == null) {
            // Here the trim failed, but the failure was not detected properly.  We force a failure in the alignment.
            throw new CliTaskException(this, "Attempt to perform alignment after failure of the TRIM step.");
        }
        // Create the RNA source.
        RnaSource source = new RnaSource.Paired(leftFile, rightFile);
        // Now build the output.
        this.parms = new JsonObject();
        this.parms.put("output_file", RnaJob.Phase.ALIGN.getOutputName(job.getName()));
        this.parms.put("output_path", job.getOutDir());
        this.parms.put("reference_genome_id", job.getAlignmentGenomeId());
        this.parms.put("recipe", "HTSeq-DESeq");
        this.parms.put("genome_type", "bacteria");
        this.parms.put("strand_specific", "1");
        source.store(this.parms);
    }

    @Override
    protected void startService() {
        this.submit("RNASeq", this.parms);
    }

}
