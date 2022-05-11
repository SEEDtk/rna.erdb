package org.theseed.neighbors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.Test;

class TestNeighborhood {

    @Test
    void testNeighborhood() {
        Neighborhood hood = new Neighborhood(5);
        assertThat(hood.size(), equalTo(0));
        hood.merge("B", 0.9);
        hood.merge("A", 1.0);
        hood.merge("C", 0.8);
        Neighborhood.Neighbor[] results = hood.getNeighbors();
        assertThat(results[0].getId(), equalTo("A"));
        assertThat(results[1].getId(), equalTo("B"));
        assertThat(results[2].getId(), equalTo("C"));
        assertThat(results.length, equalTo(3));
        hood.merge("F", 0.5);
        hood.merge("G", 0.4);
        results = hood.getNeighbors();
        assertThat(results[0].getId(), equalTo("A"));
        assertThat(results[1].getId(), equalTo("B"));
        assertThat(results[2].getId(), equalTo("C"));
        assertThat(results[3].getId(), equalTo("F"));
        assertThat(results[4].getId(), equalTo("G"));
        assertThat(results.length, equalTo(5));
        hood.merge("E", 0.6);
        hood.merge("D", 0.7);
        results = hood.getNeighbors();
        assertThat(results[0].getId(), equalTo("A"));
        assertThat(results[1].getId(), equalTo("B"));
        assertThat(results[2].getId(), equalTo("C"));
        assertThat(results[3].getId(), equalTo("D"));
        assertThat(results[4].getId(), equalTo("E"));
        assertThat(results.length, equalTo(5));
    }

}
