package org.neo4j.cypher.internal.compiler.v3_2.bork.expressions;
import org.neo4j.cypher.internal.compiler.v3_2.bork.Expression;
import org.neo4j.cypher.internal.compiler.v3_2.bork.QueryRun;
import org.neo4j.cypher.internal.compiler.v3_2.bork.Register;
import org.neo4j.kernel.api.exceptions.KernelException;

public class LiteralLong extends Expression {
    @Override
    public Object evaluate(Register input, QueryRun queryRun) throws KernelException {
        return true;
    }
}
