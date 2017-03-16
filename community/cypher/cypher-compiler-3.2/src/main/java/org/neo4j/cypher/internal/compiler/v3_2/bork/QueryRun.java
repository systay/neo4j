package org.neo4j.cypher.internal.compiler.v3_2.bork;

import org.neo4j.cypher.internal.compiler.v3_2.spi.InternalResultVisitor;
import org.neo4j.kernel.api.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/*
A PipeLine connects operators through morsels. It is
 */
public class QueryRun
{
    private final Operator operatorTree;
    private final Map<Operator,RegisterInfo> registerInfo;
    private final Statement statement;
    public final Map<Operator,Object> state = new HashMap<>();

    public QueryRun( Operator operatorTree, Map<Operator,RegisterInfo> registerInfo, Statement statement )
    {
        this.operatorTree = operatorTree;
        this.registerInfo = registerInfo;
        this.statement = statement;
    }

    public <E extends Exception> void execute( final InternalResultVisitor<E> visitor ) throws E
    {

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
}
