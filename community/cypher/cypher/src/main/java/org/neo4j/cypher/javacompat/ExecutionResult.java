/**
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
package org.neo4j.cypher.javacompat;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.cypher.CypherException;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import scala.collection.JavaConversions;

/**
 * Holds Cypher query result sets, in tabular form. Each row of the result is a map
 * of column name to result object. Each column name correlates directly
 * with the terms used in the "return" clause of the Cypher query.
 * The result objects could be {@link org.neo4j.graphdb.Node Nodes},
 * {@link org.neo4j.graphdb.Relationship Relationships} or java primitives.
 *
 *
 * Either iterate directly over the ExecutionResult to retrieve each row of the result
 * set, or use <code>columnAs()</code> to access a single column with result objects
 * cast to a type.
 *
 * @deprecated See {@link org.neo4j.graphdb.Result}, and use
 * {@link org.neo4j.graphdb.GraphDatabaseService#execute(String, Map)} instead.
 */
@Deprecated
public class ExecutionResult implements ResourceIterable<Map<String,Object>>, Result
{
    private final org.neo4j.cypher.ExtendedExecutionResult inner;
    private final ResourceIterator<Map<String, Object>> iter;

    /**
     * Constructor used by the Cypher framework. End-users should not
     * create an ExecutionResult directly, but instead use the result
     * returned from calling {@link ExecutionEngine#execute(String)}.
     */
    public ExecutionResult( org.neo4j.cypher.ExtendedExecutionResult projection)
    {
        inner = projection;
        iter = inner.javaIterator();
    }

    /**
     * Returns an iterator with the result objects from a single column of the result set. This method is best used for
     * single column results.
     *
     * <p><b>To ensure that any resources, including transactions bound to it, are properly closed, the iterator must
     * either be fully exhausted, or the {@link org.neo4j.graphdb.ResourceIterator#close() close()} method must be
     * called.</b></p>
     *
     * @param n exact name of the column, as it appeared in the original query
     * @param <T> desired type cast for the result objects
     * @return an iterator of the result objects, possibly empty
     * @throws ClassCastException when the result object can not be cast to the requested type
     * @throws org.neo4j.graphdb.NotFoundException when the column name does not appear in the original query
     */
    @Override
    public <T> ResourceIterator<T> columnAs( String n )
    {
        // this method is both a legacy method, and a method on Result,
        // prefer conforming to the new API and convert exceptions
        try
        {
            return new ExceptionConversion<>( inner.<T>javaColumnAs( n ) );
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    @Override
    public QueryExecutionType getQueryExecutionType()
    {
        try
        {
            return inner.executionType();
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    /**
     * The exact names used to represent each column in the result set.
     *
     * @return List of the column names.
     */
    @Override
    public List<String> columns()
    {
        // this method is both a legacy method, and a method on Result,
        // prefer conforming to the new API and convert exceptions
        try
        {
            return inner.javaColumns();
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    @Override
    public String toString()
    {
        return inner.toString(); // legacy method - don't convert exceptions...
    }

    /**
     * Provides a textual representation of the query result.
     * <p><b>
     * The execution result represented by this object will be consumed in its entirety after this method is called.
     * Calling any of the other iterating methods on it should not be expected to return any results.
     * </b></p>
     * @return Returns the execution result
     */
    public String dumpToString()
    {
        return inner.dumpToString(); // legacy method - don't convert exceptions...
    }

    /**
     * Returns statistics about this result.
     * @return statistics about this result
     */
    @Override
    public QueryStatistics getQueryStatistics()
    {
        try
        {
            return new QueryStatistics( inner.queryStatistics() );
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    /**
     * Returns a string representation of the query plan used to produce this result.
     * @return a string representation of the query plan used to produce this result.
     */
    public PlanDescription executionPlanDescription()
    {
        return inner.executionPlanDescription().asJava(); // legacy method - don't convert exceptions...
    }

    public void toString( PrintWriter writer )
    {
        inner.dumpToString( writer ); // legacy method - don't convert exceptions...
    }

    /**
     * Returns an iterator over the <i>return</i> clause of the query. The format is a map that has as keys the names
     * of the columns or their explicit names (set via 'as') and the value is the calculated value. Each iterator item
     * is one row of the query result.
     *
     * <p><b>To ensure that any resources, including transactions bound to it, are properly closed, the iterator must
     * either be fully exhausted, or the {@link org.neo4j.graphdb.ResourceIterator#close() close()} method must be
     * called.</b></p>
     *
     * @return An iterator over the result of the query as a map from projected column name to value
     */
    @Override
    public ResourceIterator<Map<String, Object>> iterator()
    {
        return iter; // legacy method - don't convert exceptions...
    }

    @Override
    public boolean hasNext()
    {
        try
        {
            return iter.hasNext();
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    @Override
    public Map<String, Object> next()
    {
        try
        {
            return iter.next();
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            iter.close();
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    @Override
    public ExecutionPlanDescription getExecutionPlanDescription()
    {
        try
        {
            return new Description( inner.executionPlanDescription() );
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    @Override
    public String resultAsString()
    {
        try
        {
            return dumpToString();
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    @Override
    public void writeAsStringTo( PrintWriter writer )
    {
        try
        {
            toString( writer );
        }
        catch ( CypherException e )
        {
            throw converted( e );
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accept( ResultVisitor visitor )
    {
        final MapResultRow row = new MapResultRow();
        for ( boolean cont = true; cont && iter.hasNext(); cont = visitor.visit( row ) )
        {
            row.setMap( iter.next() );
        }
    }

    @Override
    public Iterable<Notification> getNotifications()
    {
        return JavaConversions.asJavaIterable( inner.notifications() );
    }

    private static class ExceptionConversion<T> implements ResourceIterator<T>
    {
        private final ResourceIterator<T> inner;

        public ExceptionConversion( ResourceIterator<T> inner )
        {
            this.inner = inner;
        }

        @Override
        public void close()
        {
            try
            {
                inner.close();
            }
            catch ( CypherException e )
            {
                throw converted( e );
            }
        }

        @Override
        public boolean hasNext()
        {
            try
            {
                return inner.hasNext();
            }
            catch ( CypherException e )
            {
                throw converted( e );
            }
        }

        @Override
        public T next()
        {
            try
            {
                return inner.next();
            }
            catch ( CypherException e )
            {
                throw converted( e );
            }
        }

        @Override
        public void remove()
        {
            try
            {
                inner.remove();
            }
            catch ( CypherException e )
            {
                throw converted( e );
            }
        }
    }

    private static QueryExecutionException converted( CypherException e )
    {
        return new QueryExecutionKernelException( e ).asUserException();
    }

    private static class MapResultRow implements ResultRow
    {
        private Map<String, Object> map;

        private <T> T get( String key, Class<T> type )
        {
            Object value = map.get( key );
            if ( value == null && !map.containsKey( key ) )
            {
                throw new IllegalArgumentException( "No column \"" + key + "\" exists" );
            }
            try
            {
                return type.cast( value );
            }
            catch ( ClassCastException e )
            {
                throw new NoSuchElementException( "The current item in column \"" + key + "\" is not a " +
                        type.getSimpleName() );
            }
        }

        @Override
        public Object get( String key )
        {
            return get( key, Object.class );
        }

        @Override
        public Node getNode( String key )
        {
            return get( key, Node.class );
        }

        @Override
        public Relationship getRelationship( String key )
        {
            return get( key, Relationship.class );
        }

        @Override
        public String getString( String key )
        {
            return get( key, String.class );
        }

        @Override
        public Long getLong( String key )
        {
            return get( key, Long.class );
        }

        @Override
        public Double getDouble( String key )
        {
            return get( key, Double.class );
        }

        @Override
        public Boolean getBoolean( String key )
        {
            return get( key, Boolean.class );
        }

        @Override
        public Path getPath( String key )
        {
            return get( key, Path.class );
        }

        public void setMap( Map<String, Object> map )
        {
            this.map = map;
        }
    }
}
