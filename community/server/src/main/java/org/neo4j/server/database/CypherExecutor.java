/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.database;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.rest.web.ServerQuerySession;

public class CypherExecutor extends LifecycleAdapter
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();

    private final Database database;
    private ExecutionEngine executionEngine;
    private TransactionalContextFactory contextFactory;

    public CypherExecutor( Database database )
    {
        this.database = database;
    }

    public ExecutionEngine getExecutionEngine()
    {
        return executionEngine;
    }

    @Override
    public void start() throws Throwable
    {
        DependencyResolver dependencyResolver = database.getGraph().getDependencyResolver();
        this.executionEngine = (ExecutionEngine) dependencyResolver.resolveDependency( QueryExecutionEngine.class );
        this.contextFactory = new Neo4jTransactionalContextFactory(
            dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ),
            locker
        );
    }

    @Override
    public void stop() throws Throwable
    {
        this.executionEngine = null;
        this.contextFactory = null;
    }

    public QuerySession createSession( String query, Map<String, Object> parameters, HttpServletRequest request )
    {
        TransactionalContext context = contextFactory.newContext( ServerQuerySession.describe( request ),
            KernelTransaction.Type.implicit,
            AccessMode.Static.FULL,
            query,
            parameters
        );
        return new ServerQuerySession( request, context );
    }
}
