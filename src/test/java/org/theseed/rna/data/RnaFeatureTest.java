/**
 *
 */
package org.theseed.rna.data;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.sql.SQLException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.sqlite.SqliteDbConnection;

/**
 * @author Bruce Parrello
 *
 */
class RnaFeatureTest implements RnaFeatureFilter.IParms, RnaFeatureLevelComputer.IParms {

    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(RnaFeatureTest.class);


    @Test
    void test() throws SQLException {
        File dbFile = new File("data", "rnaseqTest.db");
        try (DbConnection db = new SqliteDbConnection(dbFile)) {
            var filter = RnaFeatureFilter.Type.SUBSYSTEMS.create(this);
            var rnaMap = RnaFeature.loadFeats(db, "511145.183", filter);
            assertThat(rnaMap.containsKey("fig|511145.183.peg.1002"), equalTo(false));
            var feat = rnaMap.get("fig|511145.183.peg.1000");
            assertThat(feat, not(nullValue()));
            assertThat(feat.getBaseLine(), closeTo(61.54, 0.01));
            assertThat(feat.getIdx(), equalTo(999));
            RnaFeatureLevelComputer computer = RnaFeatureLevelComputer.Type.TRIAGE.create(this);
            assertThat(computer.compute(feat, 12.00), equalTo(-1.0));
            assertThat(computer.compute(feat, 30.00), equalTo(-1.0));
            assertThat(computer.compute(feat, 31.00), equalTo(0.0));
            assertThat(computer.compute(feat, 40.00), equalTo(0.0));
            assertThat(computer.compute(feat, 55.00), equalTo(0.0));
            assertThat(computer.compute(feat, 65.00), equalTo(0.0));
            assertThat(computer.compute(feat, 100.00), equalTo(0.0));
            assertThat(computer.compute(feat, 125.00), equalTo(1.0));
            assertThat(computer.compute(feat, 500.00), equalTo(1.0));
            assertThat(computer.compute(feat, 1000.00), equalTo(1.0));
            feat = rnaMap.get("fig|511145.183.peg.1037");
            assertThat(computer.compute(feat, 55.00), equalTo(-1.0));
            assertThat(computer.compute(feat, 65.00), equalTo(-1.0));
            assertThat(computer.compute(feat, 100.00), equalTo(0.0));
            assertThat(computer.compute(feat, 125.00), equalTo(0.0));
            assertThat(computer.compute(feat, 280.00), equalTo(1.0));
            assertThat(computer.compute(feat, 500.00), equalTo(1.0));
            computer = RnaFeatureLevelComputer.Type.IDENTITY.create(this);
            assertThat(computer.compute(feat, 12.00), equalTo(12.0));
            assertThat(computer.compute(feat, 30.00), equalTo(30.0));
            assertThat(computer.compute(feat, 31.00), equalTo(31.0));
            assertThat(computer.compute(feat, 500.00), equalTo(500.0));
            String outHeader = rnaMap.keySet().stream().map(x -> rnaMap.get(x).getName())
                    .collect(Collectors.joining("\t", "sample_id\t",
                            String.format("\t%s\t%s_type", "production", "production")));
            assertThat(outHeader, not(containsString("\t\t")));
        }
    }


}
