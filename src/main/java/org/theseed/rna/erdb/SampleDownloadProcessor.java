/**
 *
 */
package org.theseed.rna.erdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Strings;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.cli.CopyTask;
import org.theseed.cli.DirTask;
import org.theseed.io.TabbedLineReader;
import org.theseed.rna.jobs.RnaJob;

/**
 * This command downloads samples from a PATRIC RNA-seq processing directory.
 *
 * The positional parameters are the name of the PATRIC directory (generally ending in "/Output"), the PATRIC workspace ID, and
 * the name of the local output directory.  The samples will be downloaded into a subdirectory named "Output".
 *
 * The command-line options are as follows:
 *
 * -h	display command-line usage
 * -v	display more frequent log messages
 *
 * --workDir	working directory for temporary files (default is "Temp" in the current directory
 * --filter		if specified, the name tab-delimited file with headers containing sample IDs to download in the first column
 * 				(the default is to download them all)
 * --clear		erase the output directory before processing
 *
 * @author Bruce Parrello
 *
 */
public class SampleDownloadProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(SampleDownloadProcessor.class);
    /** output directory for copied files */
    private File tpmDir;
    /** set of sample IDs to download, or NULL if there is no filtering */
    private Set<String> sampleSet;
    /** pattern for extracting sample ID from a file name */
    private static final Pattern SAMPLE_PATTERN = Pattern.compile("(.+)(?:_genes\\.tpm|\\.samtools_stats)");

    // COMMAND-LINE OPTIONS

    /** work directory for temporary files */
    @Option(name = "--workDir", metaVar = "workDir", usage = "work directory for temporary files")
    private File workDir;

    /** name of optional filter file */
    @Option(name = "--filter", metaVar = "samples.tbl", usage = "if specified, the name of a file with sample IDs to download")
    private File filterFile;

    /** if specified, the output directory will be erased before processing */
    @Option(name = "--clear", usage = "if specified, the output directory will be erased before processing")
    private boolean clearFlag;

    /** PATRIC directory containing the samples */
    @Argument(index = 0, metaVar = "user@patricbrc.org/inputDirectory", usage = "PATRIC input directory for samples", required = true)
    private String inDir;

    /** controlling workspace name */
    @Argument(index = 1, metaVar = "user@patricbrc.org", usage = "controlling workspace", required = true)
    private String workspace;

    /** output directory name */
    @Argument(index = 2, metaVar = "sampleDir", usage = "output directory name", required = true)
    private File outDir;

    @Override
    protected void setDefaults() {
        this.workDir = new File(System.getProperty("user.dir"), "Temp");
        this.filterFile = null;
        this.clearFlag = false;
    }

    @Override
    protected void validateParms() throws IOException, ParseFailureException {
        // Verify we have a work directory.
        if (! this.workDir.isDirectory()) {
            log.info("Creating work directory {}.", this.workDir);
            FileUtils.forceMkdir(this.workDir);
        } else
            log.info("Working directory is {}.", this.workDir);
        // Make sure the PATRIC directory is absolute.
        if (! Strings.CS.startsWith(this.inDir, "/"))
            throw new ParseFailureException("PATRIC input directory must be absolute.");
        // Check for a filter file.
        if (this.filterFile == null) {
            log.info("All samples will be copied.");
            this.sampleSet = null;
        } else if (! this.filterFile.canRead())
            throw new FileNotFoundException("Filter file " + this.filterFile + " is not found or unreadable.");
        else {
            // Read in the list of sample IDs.
            this.sampleSet = TabbedLineReader.readSet(this.filterFile, "1");
            log.info("{} samples for download identified in filter file {}.", this.sampleSet.size(), this.filterFile);
        }
        // Insure the output directory exists.
        if (! this.outDir.isDirectory()) {
            log.info("Creating output directory {}.", this.outDir);
            FileUtils.forceMkdir(this.outDir);
        }
        // Prepare the TPM subdirectory.
        this.tpmDir = new File(this.outDir, RnaJob.TPM_DIR);
        if (! this.tpmDir.isDirectory()) {
            log.info("Creating output TPM subdirectory.");
            FileUtils.forceMkdir(this.tpmDir);
        } else if (this.clearFlag) {
            log.info("Erasing output TPM subdirectory.");
            FileUtils.cleanDirectory(this.tpmDir);
        }
        log.info("Samples will be copied to {}.", this.tpmDir);
    }

    @Override
    protected void runCommand() throws Exception {
        // Get the files already in the output directory.
        Set<String> processed = Arrays.stream(this.tpmDir.list()).collect(Collectors.toCollection(TreeSet::new));
        log.info("{} files already found in output directory.", processed.size());
        // Get all the files in the input directory, eliminating the ones already processed.
        String remoteFolder = (this.inDir + "/" + RnaJob.TPM_DIR);
        DirTask lister = new DirTask(this.workDir, this.workspace);
        Set<String> files = lister.list(remoteFolder).stream().map(x -> x.getName()).collect(Collectors.toSet());
        // Loop through the file list, eliminating files that we already processed or that are not desired by the
        // filter.
        int removeCount = 0;
        Iterator<String> iter = files.iterator();
        while (iter.hasNext()) {
            String fileName = iter.next();
            // Denote this file should be removed.  We only keep it if it has a matching sample ID
            boolean keep = false;
            // Extract the sample ID.
            Matcher m = SAMPLE_PATTERN.matcher(fileName);
            if (m.matches()) {
                // Here we are a valid sample file.
                String sampleId = m.group(1);
                // Insure it is not already copied.
                if (! processed.contains(fileName)) {
                    // Check to see if it is filtered out.
                    if (this.sampleSet == null)
                        keep = true;
                    else
                        keep = this.sampleSet.contains(sampleId);
                }
            }
            // Delete it if it is wrong.
            if (! keep) {
                iter.remove();
                removeCount++;
            }
        }
        // Now we have all the files to copy.
        log.info("{} files to copy found in PATRIC directory {}. {} removed by filtering.", files.size(), this.inDir, removeCount);
        CopyTask copy = new CopyTask(this.outDir, this.workspace);
        // If no files were removed, we perform a full copy.
        if (removeCount == 0) {
            log.info("Performing full-directory copy.");
            File[] newFiles = copy.copyRemoteFolder(remoteFolder, true);
            log.info("{} files copied.", newFiles.length);
        } else {
            // Loop through the files in the remote folder
            log.info("Performing file-by-file copy.");
            long start = System.currentTimeMillis();
            int copied = 0;
            int total = files.size();
            for (String remoteName : files) {
                String remoteFile = remoteFolder + "/" + remoteName;
                File localFile = new File(this.tpmDir, remoteName);
                copy.copyRemoteFile(remoteFile, localFile);
                processed.add(remoteName);
                if (log.isInfoEnabled()) {
                    copied++;
                    if (copied % 10 == 0) {
                        long timeLeft = (System.currentTimeMillis() - start) * (total - copied) / (copied * 1000);
                        log.info("{} files copied.  {} seconds remaining.", copied, timeLeft);
                    }
                }
            }
            log.info("{} total files now in {}.", processed.size(), this.tpmDir);
        }
    }

}
