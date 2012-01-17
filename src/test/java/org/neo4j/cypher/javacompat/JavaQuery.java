/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.javacompat;

import static org.neo4j.helpers.collection.IteratorUtil.asIterable;

import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class JavaQuery
{
    private static final String DB_PATH = "target/java-query-db";
    String resultString;
    String columnsString;
    Node resultNode;

    public static void main( String[] args )
    {
        JavaQuery javaQuery = new JavaQuery();
        javaQuery.run();
    }

    void run()
    {
        // START SNIPPET: execute
        GraphDatabaseService db = new EmbeddedGraphDatabase( DB_PATH );
        ExecutionEngine engine = new ExecutionEngine( db );
        ExecutionResult result = engine.execute( "start n=node(0) where 1=1 return n" );
        System.out.println( result );
        // END SNIPPET: execute
        // START SNIPPET: columns
        List<String> columns = result.columns();
        System.out.println( columns );
        // END SNIPPET: columns
        // START SNIPPET: items
        Iterator<Node> n_column = result.columnAs( "n" );
        for ( Node node : asIterable( n_column ) )
        {
            System.out.println( node );
            // END SNIPPET: items
            resultNode = node;
            // START SNIPPET: items
        }
        // END SNIPPET: items
        resultString = result.toString();
        columnsString = columns.toString();
        db.shutdown();
    }
}
