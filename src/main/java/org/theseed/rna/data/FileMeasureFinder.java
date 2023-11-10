/**
 *
 */
package org.theseed.rna.data;

import java.io.IOException;
import java.util.Map;

import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;

/**
 * This measurement finder reads the measurements from a file.  The input file must be tab-delimited, with headers, and
 * sample IDs in the first column.  The measurement values (which are loaded as strings) can be in any other column,
 * but the default is the second.
 *
 * @author Bruce Parrello
 *
 */
public class FileMeasureFinder extends MeasureFinder {

    // FIELDS
    /** map from sample IDs to measurements */
    private Map<String, String> measureMap;

    /**
     * Construct a measurement finder for the specified command processor.
     *
     * @param processor		controlling command processor
     *
     * @throws IOException
     * @throws ParseFailureException
     */
    public FileMeasureFinder(IParms processor) throws IOException, ParseFailureException {
        super(processor);
        var inFile = processor.getMeasureFile();
        var mCol = processor.getMeasureColumn();
        if (inFile == null)
            throw new ParseFailureException("Measurement file name is required for type FILE.");
        this.measureMap = TabbedLineReader.readMap(inFile, "1", mCol);
    }

    @Override
    public Map<String, String> getMeasureMap() {
        return this.measureMap;
    }

}
