/**
 *
 */
package org.theseed.rna.data;

/**
 * This level computer converts the RNA expression level to -1, 0, or +1 depending on its
 * relation to the baseline.
 *
 * @author Bruce Parrello
 *
 */
public class TriageRnaFeatureLevelComputer extends RnaFeatureLevelComputer {

    public TriageRnaFeatureLevelComputer(IParms processor) {
        super(processor);
    }

    @Override
    public double compute(RnaFeature feat, double level) {
        double retVal = 0;
        var baseline = feat.getBaseLine();
        if (level >= baseline * 2.0)
            retVal = 1.0;
        else if (level <= baseline / 2.0)
            retVal = -1.0;
        return retVal;
    }

}
