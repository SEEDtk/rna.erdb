/**
 *
 */
package org.theseed.rna.data;

/**
 * This class filters out features that are not members of at least one group of the specified type
 *
 * @author Bruce Parrello
 *
 */
public class TypeRnaFeatureFilter extends RnaFeatureFilter {

    // FIELDS
    /** type of group to include */
    private String type;

    /**
     * Construct a type feature filter for the specified type.
     *
     * @param processor		controlling command processor
     * @param type			type of group to include
     */
    public TypeRnaFeatureFilter(IParms processor, String type) {
        this.type = type;
    }

    @Override
    public boolean include(RnaFeature feat) {
        return feat.isInGroupType(type);
    }

}
