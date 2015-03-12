package org.neo4j.cypher.internal.compiler.v2_3.birk;

public class ResultVisitor {
    public boolean visit(ResultRow row) {
        System.out.println(row.toString());
        return true;
    }
}
