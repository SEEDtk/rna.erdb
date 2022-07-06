/**
 *
 */
package org.theseed.rna.data;

/**
 * This is the base class for filtering features to be included in the RNA expression data output.
 *
 * @author Bruce Parrello
 *
 */
public abstract class RnaFeatureFilter {

    /**
     * This interface describes methods that must be supported by any controlling command processor.
     */
    public interface IParms {

    }

    /**
     * This enum lists the types of filtering supported.
     */
    public static enum Type {
        /** only include features in subsystems */
        SUBSYSTEMS {

            @Override
            public RnaFeatureFilter create(IParms processor) {
                return new TypeRnaFeatureFilter(processor, "subsystem");
            }

        },
        /** include all features */
        ALL {
            @Override
            public RnaFeatureFilter create(IParms processor) {
                return new RnaFeatureFilter.All(processor);
            }
        };

        /**
         * @return a feature filter of this type
         *
         * @param processor		controlling command processor
         */
        public abstract RnaFeatureFilter create(IParms processor);
    }

    /**
     * @return TRUE if the specified feature should be included in the output, else FALSE
     *
     * @param feat		feature to check
     */
    public abstract boolean include(RnaFeature feat);

    /**
     * This is a simple filter that allows everything through.
     *
     */
    public static class All extends RnaFeatureFilter {

        public All(IParms processor) { }

        @Override
        public boolean include(RnaFeature feat) {
            return true;
        }

    }


}
