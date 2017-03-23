package org.neo4j.cypher.internal.compiler.v3_2.bork;

public interface WorkPackage {
    void run(QueryRun query);
}
