package org.theseed.rna.erdb;

import java.util.Arrays;

import org.theseed.utils.BaseProcessor;

/**
 * Commands for utilities relating to RNA-Seq processing.
 *
 * tpm			run jobs to convert FASTQ files to TPM results
 * groups		form a master group table from individual group files
 * metaLoad		load genome metadata into an SQL database
 * download		download a set of RNA results from PATRIC and produce a metadata file for them
 * upload		process RNA results from PATRIC and add them to a database
 * featCorr		update the correlations between a genome's features in the RNA database
 * sampleCorr	output the correlations between a genome's samples in the RNA database
 * clusterLoad	refresh the cluster IDs from a sample correlation table
 * baseline		set the baseline levels for particular genome
 * measureFix	add missing production data to a measurement input file
 * measureLoad	load measurements into an SQL database
 * corrReport	produce a report on feature correlations
 * dbReport		write a database report
 * neighbors	store the neighbor genes in the database
 * xmatrix		build a achine learning input directory for a specified measurement
 */
public class App
{
    public static void main( String[] args )
    {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        BaseProcessor processor;
        // Determine the command to process.
        switch (command) {
        case "baseline" :
            processor = new GenomeBaselineProcessor();
            break;
        case "tpm" :
            processor = new RnaSeqProcessor();
            break;
        case "groups" :
            processor = new MetaGroupsProcessor();
            break;
        case "metaLoad" :
            processor = new MetaLoadProcessor();
            break;
        case "measureLoad" :
            processor = new MeasureLoadProcessor();
            break;
        case "download" :
            processor = new SampleDownloadProcessor();
            break;
        case "upload" :
            processor = new SampleUploadProcessor();
            break;
        case "sampleCorr" :
            processor = new SampleCorrelationProcessor();
            break;
        case "clusterLoad" :
            processor = new ClusterLoadProcessor();
            break;
        case "dbReport" :
            processor = new DbRnaReportProcessor();
            break;
        case "featCorr" :
            processor = new FeatureCorrelationProcessor();
            break;
        case "corrReport" :
            processor = new FeatureCorrReportProcessor();
            break;
        case "neighbors" :
            processor = new NeighborProcessor();
            break;
        case "xmatrix" :
            processor = new XMatrixProcessor();
            break;
        case "measureFix" :
            processor = new MeasureFixProcessor();
            break;
        default:
            throw new RuntimeException("Invalid command " + command);
        }
        // Process it.
        boolean ok = processor.parseCommand(newArgs);
        if (ok) {
            processor.run();
        }
    }
}
