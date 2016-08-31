package org.neo4j.kernel.impl.query;

import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class Neo4jTransactionalContextFactory implements TransactionalContextFactory<Neo4jTransactionalContext>
{
    private final GraphDatabaseQueryService queryService;
    private final Supplier<Statement> statementSupplier;
    private final ThreadToStatementContextBridge txBridge;
    private final PropertyContainerLocker locker;
    private final DbmsOperations.Factory dbmsOpsFactory;

    public Neo4jTransactionalContextFactory( GraphDatabaseFacade.SPI spi, PropertyContainerLocker locker )
    {
        this.queryService = spi.queryService();
        this.locker = locker;
        DependencyResolver dependencyResolver = queryService.getDependencyResolver();
        this.txBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        this.dbmsOpsFactory = dependencyResolver.resolveDependency( DbmsOperations.Factory.class );
        this.statementSupplier = spi::currentStatement;
    }

    public Neo4jTransactionalContextFactory( GraphDatabaseQueryService queryService, PropertyContainerLocker locker )
    {
        this.queryService = queryService;
        this.locker = locker;
        DependencyResolver dependencyResolver = queryService.getDependencyResolver();
        this.txBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        this.dbmsOpsFactory = dependencyResolver.resolveDependency( DbmsOperations.Factory.class );
        this.statementSupplier = this.txBridge;
    }

    @Override
    public Neo4jTransactionalContext newContext(
        QuerySourceDescriptor descriptor,
        KernelTransaction.Type type,
        AccessMode accessMode,
        String queryText,
        Map<String,Object> queryParameters
    )
    {
        InternalTransaction transaction = queryService.beginTransaction( type, accessMode );
        return newContext( descriptor, transaction, queryText, queryParameters );
    }

    @Override
    public Neo4jTransactionalContext newContext(
        QuerySourceDescriptor descriptor,
        InternalTransaction tx,
        String queryText,
        Map<String,Object> queryParameters
    )
    {
        Statement statement = txBridge.get();
        ExecutingQuery executingQuery = statement.queryRegistration().startQueryExecution( queryText, queryParameters );
        return new Neo4jTransactionalContext(
            queryService,
            tx,
            statementSupplier.get(),
            executingQuery,
            locker,
            txBridge,
            dbmsOpsFactory
        );
    }
}
