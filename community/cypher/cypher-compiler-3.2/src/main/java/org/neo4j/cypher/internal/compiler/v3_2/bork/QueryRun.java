package org.neo4j.cypher.internal.compiler.v3_2.bork;

import org.neo4j.cypher.internal.compiler.v3_2.spi.InternalResultVisitor;
import org.neo4j.kernel.api.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class QueryRun
{
    private final Statement statement;
    private final InternalResultVisitor resultVisitor;
    public final Map<Operator, Object> state;

    public QueryRun(Statement statement, InternalResultVisitor resultVisitor, Map<Operator, Object> state) {
        this.statement = statement;
        this.resultVisitor = resultVisitor;
        this.state = state;
    }

    public QueryRun(Statement statement, InternalResultVisitor resultVisitor)
    {
        this(statement, resultVisitor, new HashMap<>());
    }

    public <T> T getOrCreate( Operator op, Supplier<T> value )
    {
        Object o = state.computeIfAbsent( op, k -> value.get() );
        return (T) o;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public InternalResultVisitor getResultVisitor() {
        return resultVisitor;
    }
}
