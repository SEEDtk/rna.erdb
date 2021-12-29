/**
 *
 */
package org.theseed.reports;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import java.io.File;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.sqlite.SqliteDbConnection;

/**
 * @author Bruce Parrello
 *
 */
class TestFeatureIndex {

    @Test
    void testFeatureIndex() throws SQLException {
        File dbFile = new File("data", "rnaseqTest.db");
        DbConnection db = new SqliteDbConnection(dbFile);
        FeatureIndex fIndex = new FeatureIndex(db, "511145.183");
        assertThat(fIndex.getGenomeId(), equalTo("511145.183"));
        assertThat(fIndex.getGenomeName(), equalTo("Escherichia coli str. K-12 substr. MG1655"));
        assertThat(fIndex.getFeatureCount(), equalTo(4518));
        assertThat(fIndex.getGroupTypes(), contains("modulon", "operon", "regulon", "subsystem"));
        FeatureData featData = fIndex.getFeature(12);
        assertThat(featData.getFid(), equalTo("fig|511145.183.peg.13"));
        assertThat(featData.getGene(), equalTo("dnaK"));
        assertThat(featData.getAlias(), equalTo("b0014"));
        assertThat(featData.getAssignment(), equalTo("Chaperone protein DnaK"));
        assertThat(featData.getGroups("regulon"), contains("AR114"));
        assertThat(featData.getGroups("modulon"), contains("RpoH"));
        assertThat(featData.getGroups("operon"), contains("dnaK-tpke11-dnaJ"));
        assertThat(featData.getGroups("subsystem"), contains("Chaperones GroEL GroES and Thermosome",
                "Heat shock dnaK gene cluster extended", "Protein chaperones"));
        featData = fIndex.getFeature(3);
        assertThat(featData.getFid(), equalTo("fig|511145.183.peg.4"));
        for (int i = 0; i < 4518; i++) {
            featData = fIndex.getFeature(i);
            assertThat(String.format("peg[%d]", i), featData, not(nullValue()));
        }

    }

}
