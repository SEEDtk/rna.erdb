/**
 *
 */
package org.theseed.neighbors;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a neighborhood.  It contains a fixed number of neighbbors, sorted
 * from closest to further.  When a new neighbor is presented, it is either added or discared.
 * If it is added and the neighborhood is full, the furthest neighbor will be removed.
 *
 * @author Bruce Parrello
 *
 */
public class Neighborhood {

    // FIELDS
    /** sorted list of neighbors */
    private List<Neighbor> neighbors;
    /** maximum number of neighbors */
    private int max;

    /**
     * This object represents a single neighbor and the strength of its correlation.  It
     * sorts from strongest correlation to weakest.
     */
    public class Neighbor implements Comparable<Neighbor> {

        /** ID of the neighbor */
        private String id;
        /** correlation with the neighbor */
        private double corr;

        /**
         * Create a neighbor object.
         *
         * @param nId		ID of the neighbor
         * @param nCorr		correlation with the neighbor
         */
        protected Neighbor(String nId, double nCorr) {
            this.id = nId;
            this.corr = nCorr;
        }

        @Override
        public int compareTo(Neighbor o) {
            int retVal = Double.compare(o.corr, this.corr);
            if (retVal == 0)
                retVal = this.id.compareTo(o.id);
            return retVal;
        }

        /**
         * @return the ID of the neight
         */
        public String getId() {
            return this.id;
        }

        /**
         * @return the correlation with the neighbor
         */
        public double getCorr() {
            return this.corr;
        }

    }

    /**
     * Create an empty neighborhood.
     *
     * @param nMax		maximum number of neighbors allowed
     */
    public Neighborhood(int nMax) {
        this.max = nMax;
        this.neighbors = new ArrayList<Neighbor>(nMax);
    }

    /**
     * Add a neighbor to this neighborhood.  If the neighbor is too far away, it will be
     * ignored.
     *
     * @param id	ID of the new neighbor
     * @param corr	correlation with the new neighbor
     */
    public void merge(String id, double corr) {
        // Find the location for this neighbor.
        final int n = this.neighbors.size();
        int i = 0;
        while (i < n && this.neighbors.get(i).corr > corr) i++;
        if (i >= n) {
            // Here the neighbor belongs past the end of the current neighborhood.
            // Add it if it fits.
            if (n < this.max)
                this.neighbors.add(new Neighbor(id, corr));
        } else {
            // Here the neighbor belongs in the middle of the list.
            // Insure there is room for it.
            if (n >= this.max)
                this.neighbors.remove(n - 1);
            // Insert the new neighbor.
            this.neighbors.add(i, new Neighbor(id, corr));
        }
    }

    /**
     * @return an array of the neighbors
     */
    public Neighbor[] getNeighbors() {
        return this.neighbors.stream().toArray(Neighbor[]::new);
    }

    /**
     * @return the number of neighbors
     */
    public int size() {
        return this.neighbors.size();
    }

}
