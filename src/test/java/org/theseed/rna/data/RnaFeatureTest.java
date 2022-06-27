/**
 *
 */
package org.theseed.rna.data;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.theseed.java.erdb.DbConnection;
import org.theseed.java.erdb.DbQuery;
import org.theseed.java.erdb.DbUpdate;
import org.theseed.java.erdb.sqlite.SqliteDbConnection;

/**
 * @author Bruce Parrello
 *
 */
class RnaFeatureTest {

    @Test
    void test() throws SQLException {
        File dbFile = new File("data", "rnaseqTest.db");
        File db2 = new File("C:\\Users\\Bruce\\Documents\\SEEDtk\\Data\\FonSyntBioThr\\RnaSeqDb", "rnaseq.db");
        try (DbConnection db = new SqliteDbConnection(dbFile); DbConnection source = new SqliteDbConnection(db2)) {
            // Copy the baselines from the input to the output.
            try (DbQuery qIn = new DbQuery(source, "Feature"); DbUpdate qOut = DbUpdate.batch(db, "Feature")) {
                qIn.select("Feature", "fig_id", "baseline");
                qOut.change("baseline");
                qOut.filter("fig_id");

            }
        }
    }
    // FIELDS
    // TODO data members for RnaFeatureTest

    // TODO constructors and methods for RnaFeatureTest
}
