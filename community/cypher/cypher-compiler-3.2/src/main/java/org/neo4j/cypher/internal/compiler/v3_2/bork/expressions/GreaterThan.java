package org.neo4j.cypher.internal.compiler.v3_2.bork.expressions;

import org.neo4j.cypher.internal.compiler.v3_2.bork.Expression;
import org.neo4j.cypher.internal.compiler.v3_2.bork.QueryRun;
import org.neo4j.cypher.internal.compiler.v3_2.bork.Register;
import org.neo4j.kernel.api.exceptions.KernelException;

public class GreaterThan implements Expression {

    private final Expression lhs;
    private final Expression rhs;

    public GreaterThan(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Object evaluate(Register input, QueryRun queryRun) throws KernelException {
        Number lVal = (Number) lhs.evaluate(input, queryRun);
        Number rVal = (Number) rhs.evaluate(input, queryRun);

        return lVal.longValue() > rVal.longValue();
    }
}
