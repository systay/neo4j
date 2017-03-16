package org.neo4j.cypher.internal.compiler.v3_2.bork;

public class FilterOp extends Operator {

    private Expression exp;

    public FilterOp(Operator lhs, Expression expression) {
        super(lhs);
        this.exp = expression;
    }

    @Override
    public boolean execute(Morsel input, QueryRun query) {
        while (input.next()) {
            boolean match = (boolean) exp.isMatch(input, query);
            if (!match) {
                input.setInvalid();
            }
        }

        return false;
    }
}
