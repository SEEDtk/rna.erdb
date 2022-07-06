/**
 *
 */
package org.theseed.rna.data;

/**
 * This is the base class for methods that compute the expression level representation for an RNA feature.
 * Among the possibilities are normalizing to a ratio of the baseline, collapsing to high/low/normal, and
 * so forth.
 *
 * @author Bruce Parrello
 *
 */
public abstract class RnaFeatureLevelComputer {

    /**
     * This interface must be supported by any command processor that creates a level computer.
     */
    public interface IParms {

    }

    /**
     * This enumerates the different types of level computers.
     */
    public static enum Type {
        IDENTITY {
            @Override
            public RnaFeatureLevelComputer create(IParms processor) {
                return new RnaFeatureLevelComputer.Null(processor);
            }
        }, TRIAGE {
            @Override
            public RnaFeatureLevelComputer create(IParms processor) {
                return new TriageRnaFeatureLevelComputer(processor);
            }
        };

        public abstract RnaFeatureLevelComputer create(IParms processor);

    }

    /**
     * Construct an RNA feature level computer.
     *
     * @param processor		controlling command processor
     */
    protected RnaFeatureLevelComputer(IParms processor) { }

    /**
     * @return the computed expression level for a feature
     *
     * @param feat		feature of interest
     * @param level		raw level number
     */
    public abstract double compute(RnaFeature feat, double level);

    /**
     * This is the simplest feature level computer.  It simply returns the input unchanged.
     * Unknown values translate to the baseline.
     */
    public static class Null extends RnaFeatureLevelComputer {

        public Null(IParms processor) {
            super(processor);
        }

        @Override
        public double compute(RnaFeature feat, double level) {
            return (Double.isFinite(level) ? level : feat.getBaseLine());
        }

    }


}
