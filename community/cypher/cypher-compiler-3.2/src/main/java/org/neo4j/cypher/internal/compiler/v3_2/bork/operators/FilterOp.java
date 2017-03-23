package org.neo4j.cypher.internal.compiler.v3_2.bork.operators;

import org.neo4j.cypher.internal.compiler.v3_2.bork.Expression;
import org.neo4j.cypher.internal.compiler.v3_2.bork.Morsel;
import org.neo4j.cypher.internal.compiler.v3_2.bork.Operator;
import org.neo4j.cypher.internal.compiler.v3_2.bork.QueryRun;
import org.neo4j.kernel.api.exceptions.KernelException;

public class FilterOp extends Operator {

    private Expression exp;

    public FilterOp(Operator lhs, Expression expression) {
        super(lhs);
        this.exp = expression;
    }

    @Override
    public boolean execute(Morsel input, QueryRun query) throws KernelException {
        while (input.next()) {
            if (!exp.isTrue(input, query)) {
                input.setInvalid();
            }
        }

        return false;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }
}
