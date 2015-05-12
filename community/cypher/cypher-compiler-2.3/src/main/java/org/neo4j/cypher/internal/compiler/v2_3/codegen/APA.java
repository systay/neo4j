/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionMode;
import org.neo4j.cypher.internal.compiler.v2_3.TaskCloser;
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.CompiledExecutionResult;
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Id;
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result.ResultVisitor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public class APA extends CompiledExecutionResult
{
    private final ReadOperations ro;
    private final GraphDatabaseService db;
    private final Map<String, Object> params;
    private final QueryExecutionTracer tracer;
    public static Id OP3_AllNodesScan;
    public static Id OP2_Expand;
    public static Id OP1_ProduceResult;

    public APA( TaskCloser closer, Statement statement, GraphDatabaseService db,
                ExecutionMode executionMode, Supplier<InternalPlanDescription> description, QueryExecutionTracer tracer,
                Map<String, Object> params )
    {
        super( closer, statement, executionMode, description );
        this.ro = statement.readOperations();
        this.db = db;
        this.tracer = tracer;
        this.params = params;
    }

    int v4 = -1;
    private final List<String> javaColumns = Arrays.asList( "n1", "n2" );

    @Override
    public List<String> javaColumns()
    {
        return this.javaColumns;
    }

    @Override
    public <E extends Exception> void accept( final ResultVisitor<E> visitor ) throws E
    {
        final ResultRowImpl row = new ResultRowImpl();
        if ( v4 == -1 )
        {
            v4 = ro.relationshipTypeGetForName( "KNOWS" );
        }
        try
        {
            if ( v4 == -1 )
            {
                v4 = ro.relationshipTypeGetForName( "KNOWS" );
            }
            PrimitiveLongIterator v1Iter = ro.nodesGetAll();
            while ( v1Iter.hasNext() )
            {
                final long v1 = v1Iter.next();
                RelationshipIterator v2Iter = ro.nodeGetRelationships( v1, Direction.INCOMING, v4 );
                while ( v2Iter.hasNext() )
                {
                    final long v2 = v2Iter.next();
                    long v3;
                    {
                        RelationshipDataExtractor rel = new RelationshipDataExtractor();
                        ro.relationshipVisit( v2, rel );
                        v3 = rel.startNode();

                    }
                    row.set( "n1", db.getNodeById( v3 ) );
                    row.set( "n2", db.getNodeById( v1 ) );
                    try ( QueryExecutionEvent event_OP1_ProduceResult = tracer.executeOperator(
                            OP1_ProduceResult ) )
                    {
                        if ( !visitor.visit( row ) )
                        {
                            success();
                            return;
                        }
                        event_OP1_ProduceResult.row();
                    }
                }

            }
            success();
        }
        catch ( org.neo4j.kernel.api.exceptions.KernelException e )
        {
            throw new org.neo4j.cypher.internal.compiler.v2_3.CypherExecutionException(
                    e.getUserMessage( new org.neo4j.kernel.api.StatementTokenNameLookup( ro ) ), e );
        }
        finally
        {
            close();
        }
    }
}
