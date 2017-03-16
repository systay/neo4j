package org.neo4j.cypher.internal.compiler.v3_2.bork;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

public class AllNodesScanOp extends Operator
{
    private final int nodeIdx;

    public AllNodesScanOp(int nodeIdx )
    {
        this.nodeIdx = nodeIdx;
    }

    @Override
    public boolean execute(Morsel input, QueryRun query )
    {
        PrimitiveLongIterator allNodes = query.getOrCreate( this,
                () -> query.getStatement().readOperations().nodesGetAll() );

        while ( allNodes.hasNext() && input.createNext() )
        {
            long next = allNodes.next();
            input.setLongAt( nodeIdx, next );
        }

        return allNodes.hasNext();
    }
}
