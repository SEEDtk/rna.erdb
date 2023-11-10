/**
 *
 */
package org.theseed.rna.data;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;

/**
 * This is the base class for computing the measurement of an RNA sample.  Currently, we only accept measurements
 * taken from a file.  The class is essentially charged with creating a map from sample IDs to output strings.
 * (The strings can be classification labels or floating-point numbers.)
 *
 * @author Bruce Parrello
 *
 */
public abstract class MeasureFinder {

    // FIELDS
    /** logging facility */
    protected static Logger log = LoggerFactory.getLogger(FileMeasureFinder.class);

    /**
     * This interface defines all the methods that must be supported by a controlling command processor.
     */
    public interface IParms {

        /**
         * @return the name of the file containing the measurement data
         */
        public File getMeasureFile();

        /**
         * @return the index (1-based) or name of the column containing the measurement data
         */
        public String getMeasureColumn();

    }

    /**
     * This enum defines the different types of measurement finders
     */
    public enum Type {
        FILE {
            @Override
            public MeasureFinder create(IParms processor) throws IOException, ParseFailureException {
                return new FileMeasureFinder(processor);
            }
        };

        public abstract MeasureFinder create(IParms processor) throws IOException, ParseFailureException;
    }

    /**
     * Construct a measurement finder for the specified command processor
     *
     * @param processor		controlling command processor containing additional data
     */
    public MeasureFinder(IParms processor) {
    }

    /**
     * @return a map of sample IDs to measurements
     */
    public abstract Map<String, String> getMeasureMap();

}
