package org.neo4j.cypher.internal.compiler.v2_3.birk;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class ResultRowImpl implements ResultRow
{
    private final GraphDatabaseService db;
    private Map<String, Object> results = new HashMap<>();

    public ResultRowImpl( GraphDatabaseService db )
    {
        this.db = db;
    }

    @Override
    public Node getNodeAt( String column )
    {
        long nodeId = (long) results.get( column );
        return db.getNodeById( nodeId );
    }

    public void setNodeId( String k, long id )
    {
        results.put( k, id );
    }
}
