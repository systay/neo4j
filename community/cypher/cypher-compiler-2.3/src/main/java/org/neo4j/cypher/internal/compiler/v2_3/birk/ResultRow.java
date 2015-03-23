package org.neo4j.cypher.internal.compiler.v2_3.birk;

import org.neo4j.graphdb.Node;

public interface ResultRow
{
    Node getNodeAt(String column);
}
