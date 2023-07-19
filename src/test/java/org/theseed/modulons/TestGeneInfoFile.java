/**
 *
 */
package org.theseed.modulons;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipException;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.genome.Feature;
import org.theseed.genome.Genome;

/**
 * @author Bruce Parrello
 *
 */
class TestGeneInfoFile {

    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(TestGeneInfoFile.class);


    @Test
    void testMycoFile() throws ZipException, IOException {
        File geneFile = new File("data", "gene_files.zip");
        GeneInfoFile geneInfo = new GeneInfoFile(geneFile, "NC_000962");
        // This is a dwarf GTO with no contigs.
        File gtoFile = new File("data", "83332.12.gto");
        Genome genome = new Genome(gtoFile);
        // Check some features.
        GeneInfoFile.Gene gene0 = geneInfo.get("Rv0001");
        Feature peg = genome.getFeature("fig|83332.12.peg.1");
        assertThat(gene0.getLoc().getEnd(), equalTo(peg.getLocation().getEnd()));
        assertThat(gene0.getLoc().getStrand(), equalTo(peg.getLocation().getStrand()));
        assertThat(gene0.getName(), equalTo("dnaA"));
        assertThat(gene0.getOperon(), equalTo("Op1"));
        assertThat(gene0.getModulon().size(), equalTo(0));
        gene0 = geneInfo.get("Rv0002");
        peg = genome.getFeature("fig|83332.12.peg.2");
        assertThat(gene0.getLoc().getEnd(), equalTo(peg.getLocation().getEnd()));
        assertThat(gene0.getLoc().getStrand(), equalTo(peg.getLocation().getStrand()));
        assertThat(gene0.getName(), equalTo("dnaN"));
        assertThat(gene0.getOperon(), equalTo("Op2"));
        assertThat(gene0.getModulon().size(), equalTo(0));
        gene0 = geneInfo.get("Rv0008c");
        peg = genome.getFeature("fig|83332.12.peg.11");
        assertThat(gene0.getLoc().getEnd(), equalTo(peg.getLocation().getEnd()));
        assertThat(gene0.getLoc().getStrand(), equalTo(peg.getLocation().getStrand()));
        assertThat(gene0.getName(), nullValue());
        assertThat(gene0.getOperon(), equalTo("Op5"));
        assertThat(gene0.getModulon().size(), equalTo(0));
        // Verify all the pegs.
        Map<String, Set<String>> aliasMap = genome.getAliasMap();
        Collection<GeneInfoFile.Gene> genes = geneInfo.getAll();
        for (var gene : genes) {
            String geneId = gene.getId();
            var pegs = aliasMap.get(geneId);
            if (pegs == null)
                log.warn("No pegs found for {}.", geneId);
            else if (pegs.size() != 1)
                log.warn("{} pegs found for {}.", pegs.size(), geneId);
            else {
                for (String pegId : pegs) {
                    peg = genome.getFeature(pegId);
                    assertThat(pegId, gene.getLoc().isOverlapping(peg.getLocation()), equalTo(true));
                    assertThat(pegId, gene.getLoc().getStrand(), equalTo(peg.getLocation().getStrand()));
                    String name = gene.getName();
                    if (name != null && ! peg.getAliases().contains(name))
                        log.warn("Alias {} not found in {}.", name, pegId);
                }
            }
        }

    }

}
