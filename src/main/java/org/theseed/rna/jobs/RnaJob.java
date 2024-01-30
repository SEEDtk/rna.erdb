/**
 *
 */
package org.theseed.rna.jobs;

import java.io.File;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.cli.CopyTask;
import org.theseed.cli.DirEntry;
import org.theseed.cli.DirTask;
import org.theseed.cli.RnaSource;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This object represents an RNA-Seq processing job in progress.  The job has a task ID that indicates a current
 * task in progress, a name, and a phase.  The phase indicates what the current task is doing.  All phases for
 * a job must be completed before we can call the job complete.
 *
 * @author Bruce Parrello
 *
 */
public class RnaJob implements Comparable<RnaJob> {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaJob.class);

    /** name of this job */
    private String name;
    /** ID of the current task (or NULL if none) */
    private String taskId;
    /** RNA Sequence source */
    private RnaSource source;
    /** output directory name */
    private String outDir;
    /** current phase */
    private Phase phase;
    /** alignment genome */
    private String alignmentGenomeId;
    /** name of the TPM directory */
    public static final String TPM_DIR = "Output";
    /** name of the job directory */
    public static final String JOB_DIR = "Jobs";

    /**
     * Enumeration for job phases
     */
    public enum Phase {
        TRIM("_fq"), ALIGN("_rna"), COPY("_genes.tpm"), DONE("");

        private String suffix;

        /**
         * Construct a phase
         *
         * @param jobSuffix		suffix for output folders of this job phase
         */
        private Phase(String jobSuffix) {
            this.suffix = jobSuffix;
        }

        /**
         * @return the job name if an output folder is from this phase, else NULL
         *
         * @param jobResult		output folder name to check
         */
        public String checkSuffix(String jobResult) {
            String retVal = null;
            if (StringUtils.endsWith(jobResult, this.suffix))
                retVal = StringUtils.removeEnd(jobResult, this.suffix);
            return retVal;
        }

        /**
         * @return the output folder name for the task required by this phase
         *
         * @param jobName	relevant job name
         */
        public String getOutputName(String jobName) {
            return jobName + this.suffix;
        }

    }

    /**
     * Create an RNA job.
     *
     * @param name			job name
     * @param outDir		output directory name
     * @param genomeId		alignment genome ID
     */
    public RnaJob(String name, String outDir, String genomeId) {
        this.name = name;
        this.taskId = null;
        this.source = null;
        this.outDir = outDir;
        this.phase = Phase.TRIM;
        this.alignmentGenomeId = genomeId;
    }

    /**
     * @return the job name
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return the ID of the current task (or NULL if there is none)
     */
    public String getTaskId() {
        return this.taskId;
    }

    /**
     * @return the current phase
     */
    public Phase getPhase() {
        return this.phase;
    }

    /**
     * Specify an RNA sequencing source.
     *
     * @param source	source containing the RNA Seq data
     */
    public void setSource(RnaSource source) {
        this.source = source;
    }

    /**
     * @return the current RNA Seq source
     */
    public RnaSource getSource() {
        return this.source;
    }

    /**
     * @return TRUE if this job has both read files
     */
    public boolean isPrepared() {
        return (this.source != null);
    }

    /**
     * Update this job so that it is at least at the specified phase.
     *
     * @param nextPhase		new phase to store
     *
     * @return TRUE if the job was updated, else FALSE
     */
    public boolean mergeState(Phase nextPhase) {
        boolean retVal = false;
        if (nextPhase.ordinal() > this.phase.ordinal()) {
            this.phase = nextPhase;
            retVal = true;
        }
        return retVal;
    }

    /**
     * Store the ID of the currently-running task for this job.
     *
     * @param value		new task ID to store
     */
    public void setTaskId(String value) {
        this.taskId = value;
    }

    /**
     * @return TRUE if this job does not have a running task
     */
    public boolean needsTask() {
        return (this.taskId == null);
    }

    /**
     * Start the task for the current phase.
     *
     * @param workDir		work directory for temporary files
     * @param workspace		workspace name
     */
    public void startTask(File workDir, String workspace) {
        switch (this.phase) {
        case TRIM:
            TrimService trimmer = new TrimService(this, workDir, workspace);
            this.taskId = trimmer.start();
            break;
        case ALIGN:
            AlignService aligner = new AlignService(this, workDir, workspace);
            this.taskId = aligner.start();
            break;
        case COPY:
            this.copyTpmFile(workDir, workspace);
            break;
        case DONE:
            // Nothing to do here.
        }
    }

    /**
     * Perform the COPY task, which copies the TPM and SAMSTAT files from the RNA job folder to the TPM folder.
     *
     * @param workDir		work directory for temporary files
     * @param workspace		workspace name
     */
    private void copyTpmFile(File workDir, String workspace) {
        // We have to copy two files:  the SAMSTAT HTML file and the TPM file.
        String sourceFile1 = this.getOutDir() + "/." + Phase.ALIGN.getOutputName(this.name) + "/" + this.samstatName();
        String sourceFile2 = this.getOutDir() + "/." + Phase.ALIGN.getOutputName(this.name) + "/" + this.countName();
        // Verify the files both exist.
        boolean foundSamStat = this.checkFile(sourceFile1, workDir, workspace);
        boolean foundTpm = this.checkFile(sourceFile2, workDir, workspace);
        if (! foundSamStat || ! foundTpm)
            log.warn("RNA job did not complete for {}:  files missing.", this.name);
        else {
            CopyTask copier = new CopyTask(workDir, workspace);
            String targetFile1 = this.getCopyDir() + "/" + this.name + ".samtools_stats";
            String targetFile2 = this.getCopyDir() + "/" + Phase.COPY.getOutputName(this.name);
            log.info("Copying remote file {} to {}.", sourceFile1, targetFile1);
            copier.copyRemoteFile(sourceFile1, targetFile1);
            log.info("Copying remote file {} to {}.", sourceFile2, targetFile2);
            copier.copyRemoteFile(sourceFile2, targetFile2);
        }
        // A copy aborts if it fails, so we can mark this task done.
        this.phase = Phase.DONE;
    }

    /**
     * Verify that a file exists in the workspace.
     *
     * @param sourceFile	fully-qualified file name
     * @param workDir		work directory for temporary files
     * @param workspace		workspace name
     *
     * @return TRUE if it is found, else FALSE
     */
    private boolean checkFile(String sourceFile, File workDir, String workspace) {
        DirTask verify = new DirTask(workDir, workspace);
        // Split the file name into a directory and a base name.
        String sourceDir = StringUtils.substringBeforeLast(sourceFile, "/");
        String sourceName = sourceFile.substring(sourceDir.length() + 1);
        // Search the directory for the named file.
        List<DirEntry> files = verify.list(sourceDir);
        boolean retVal = files.stream().anyMatch(x -> x.getName().contentEquals(sourceName));
        return retVal;
    }

    /**
     * @return the name of the samstat file for this RNA Seq sample.
     */
    private String samstatName() {
        return "no_condition/" + this.name + "/" + this.name + ".samtools_stats";
    }

    /**
     * @return the name of the count file for this RNA Seq sample.
     */
    private String countName() {
        return "tpm_counts_matrix.tsv";
    }

    /**
     * Increment this job to the next phase.
     *
     * @return TRUE if the job is done, else FALSE
     */
    public boolean nextPhase() {
        // A phase change means the task in progress is complete.
        this.taskId = null;
        // Move to the next phase.
        this.phase = Phase.values()[this.phase.ordinal() + 1];
        return (this.phase == Phase.DONE);
    }

    /**
     * @return the output directory name
     */
    public String getOutDir() {
        return this.outDir + "/" + JOB_DIR;
    }

    /**
     * @return the copy directory name
     */
    public String getCopyDir() {
        return this.outDir + "/" + TPM_DIR;
    }

    /**
     * @return the alignment genome ID
     */
    public String getAlignmentGenomeId() {
        return this.alignmentGenomeId;
    }

    /**
     * Update a parameter object with this RNA source.
     *
     * @param parms		parameter JSON to update
     */
    public void storeSource(JsonObject parms) {
        this.source.store(parms);
    }

    /**
     * Jobs are sorted by name.
     */
    @Override
    public int compareTo(RnaJob o) {
        return this.name.compareTo(o.name);
    }

    /**
     * Denote this job has failed.
     */
    public void setFailed() {
        this.taskId = null;
        this.phase = Phase.DONE;
    }

}
