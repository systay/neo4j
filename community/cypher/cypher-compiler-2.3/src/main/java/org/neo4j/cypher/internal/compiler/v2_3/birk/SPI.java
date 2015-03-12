package org.neo4j.cypher.internal.compiler.v2_3.birk;

import org.neo4j.graphdb.GraphDatabaseService;

public class SPI {
    private final GraphDatabaseService db;

    public SPI(GraphDatabaseService db) {
        this.db = db;
    }

    public long[] getAllNodes() {
        return new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    }
}
