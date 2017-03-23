package org.neo4j.cypher.internal.compiler.v3_2.bork;

import org.neo4j.kernel.api.exceptions.KernelException;

public interface Expression {
    Object evaluate(Register input, QueryRun queryRun) throws KernelException;

    default boolean isTrue(Register input, QueryRun queryRun) throws KernelException {
        Object evaluate = evaluate(input, queryRun);
        return evaluate instanceof Boolean && (boolean) evaluate;
    }
}
