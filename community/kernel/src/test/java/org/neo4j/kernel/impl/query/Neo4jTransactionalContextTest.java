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
package org.neo4j.kernel.impl.query;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;

import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistryOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.dbms.DbmsOperations;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class Neo4jTransactionalContextTest
{
    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void neverStopsExecutingQueryDuringCommitAndRestartTx()
    {
        // Given
        GraphDatabaseQueryService graph = mock( GraphDatabaseQueryService.class );
        InternalTransaction initialTransaction = mock( InternalTransaction.class );
        KernelTransaction initialKTX = mock( KernelTransaction.class );
        Statement initialStatement = mock( Statement.class );
        QueryRegistryOperations initialQueryRegistry = mock( QueryRegistryOperations.class );
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        PropertyContainerLocker locker = null;
        ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );
        DbmsOperations.Factory dbmsOperationsFactory = null;

        KernelTransaction secondKTX = mock( KernelTransaction.class );
        InternalTransaction secondTransaction = mock( InternalTransaction.class );
        Statement secondStatement = mock( Statement.class );
        QueryRegistryOperations secondQueryRegistry = mock( QueryRegistryOperations.class );

        when( executingQuery.queryText() ).thenReturn( "X" );
        when( executingQuery.queryParameters() ).thenReturn( Collections.emptyMap() );
        when( initialStatement.queryRegistration() ).thenReturn( initialQueryRegistry );
        when( graph.beginTransaction( null, null ) ).thenReturn( secondTransaction );
        when( txBridge.getKernelTransactionBoundToThisThread( true ) ).thenReturn( initialKTX, secondKTX );
        when( txBridge.get() ).thenReturn( secondStatement );
        when( secondStatement.queryRegistration() ).thenReturn( secondQueryRegistry );

        Neo4jTransactionalContext context = new Neo4jTransactionalContext(
                graph, initialTransaction, initialStatement, executingQuery,
                locker, txBridge, dbmsOperationsFactory
        );

        // When
        context.commitAndRestartTx();

        // Then
        Object[] mocks =
                {txBridge, initialTransaction, initialQueryRegistry, initialKTX, secondQueryRegistry, secondKTX};
        InOrder order = Mockito.inOrder( mocks );

        // (1) Unbind old
        order.verify( txBridge ).getKernelTransactionBoundToThisThread( true );
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (2) Register and unbind new
        order.verify( txBridge ).get();
        order.verify( secondQueryRegistry ).registerExecutingQuery( executingQuery );
        order.verify( txBridge ).getKernelTransactionBoundToThisThread( true );
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (3) Rebind, unregister, and close old
        order.verify( txBridge ).bindTransactionToCurrentThread( initialKTX );
        order.verify( initialQueryRegistry ).unregisterExecutingQuery( executingQuery );
        order.verify( initialTransaction ).success();
        order.verify( initialTransaction ).close();
        order.verify( txBridge ).unbindTransactionFromCurrentThread();

        // (4) Rebind new
        order.verify( txBridge ).bindTransactionToCurrentThread( secondKTX );
        verifyNoMoreInteractions( mocks );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void rollsBackNewlyCreatedTransactionIfTerminationDetectedOnCloseDuringPeriodicCommit()
    {
        // Given
        GraphDatabaseQueryService graph = mock( GraphDatabaseQueryService.class );
        InternalTransaction initialTransaction = mock( InternalTransaction.class );
        KernelTransaction initialKTX = mock( KernelTransaction.class );
        KernelTransaction.Type transactionType = null;
        AccessMode transactionMode = null;
        Statement initialStatement = mock( Statement.class );
        QueryRegistryOperations initialQueryRegistry = mock( QueryRegistryOperations.class );
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        PropertyContainerLocker locker = null;
        ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );
        DbmsOperations.Factory dbmsOperationsFactory = null;

        KernelTransaction secondKTX = mock( KernelTransaction.class );
        InternalTransaction secondTransaction = mock( InternalTransaction.class );
        Statement secondStatement = mock( Statement.class );
        QueryRegistryOperations secondQueryRegistry = mock( QueryRegistryOperations.class );

        when( executingQuery.queryText() ).thenReturn( "X" );
        when( executingQuery.queryParameters() ).thenReturn( Collections.emptyMap() );
        Mockito.doThrow( RuntimeException.class ).when( initialTransaction ).close();
        when( initialStatement.queryRegistration() ).thenReturn( initialQueryRegistry );
        when( graph.beginTransaction( transactionType, transactionMode ) ).thenReturn( secondTransaction );
        when( txBridge.getKernelTransactionBoundToThisThread( true ) ).thenReturn( initialKTX, secondKTX );
        when( txBridge.get() ).thenReturn( secondStatement );
        when( secondStatement.queryRegistration() ).thenReturn( secondQueryRegistry );

        Neo4jTransactionalContext context = new Neo4jTransactionalContext(
            graph, initialTransaction, initialStatement, executingQuery, locker, txBridge, dbmsOperationsFactory
        );

        // When
        try
        {
            context.commitAndRestartTx();
            throw new AssertionError( "Expected RuntimeException to be thrown" );
        }
        catch (RuntimeException e)
        {
            // Then
            Object[] mocks =
                    {txBridge, initialTransaction, initialQueryRegistry, initialKTX,
                            secondQueryRegistry, secondKTX, secondTransaction};
            InOrder order = Mockito.inOrder( mocks );

            // (1) Unbind old
            order.verify( txBridge ).getKernelTransactionBoundToThisThread( true );
            order.verify( txBridge ).unbindTransactionFromCurrentThread();

            // (2) Register and unbind new
            order.verify( txBridge ).get();
            order.verify( secondQueryRegistry ).registerExecutingQuery( executingQuery );
            order.verify( txBridge ).getKernelTransactionBoundToThisThread( true );
            order.verify( txBridge ).unbindTransactionFromCurrentThread();

            // (3) Rebind, unregister, and close old
            order.verify( txBridge ).bindTransactionToCurrentThread( initialKTX );
            order.verify( initialQueryRegistry ).unregisterExecutingQuery( executingQuery );
            order.verify( initialTransaction ).success();
            order.verify( initialTransaction ).close();
            order.verify( txBridge ).bindTransactionToCurrentThread( secondKTX );
            order.verify( secondTransaction ).failure();
            order.verify( secondTransaction ).close();
            order.verify( txBridge ).unbindTransactionFromCurrentThread();

            verifyNoMoreInteractions( mocks );
        }
    }
}
