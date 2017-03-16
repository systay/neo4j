package org.neo4j.cypher.internal.compiler.v3_2.bork;

public interface Expression {
    Object isMatch(Morsel input, QueryRun queryRun);
}
