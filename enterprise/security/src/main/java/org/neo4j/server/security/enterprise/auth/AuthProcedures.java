/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.kernel.impl.api.security.OverriddenAccessMode.getUsernameFromAccessMode;
import static org.neo4j.procedure.Procedure.Mode.DBMS;

public class AuthProcedures
{
    @Context
    public AuthSubject authSubject;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public KernelTransaction tx;

    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser(
            @Name( "username" ) String username,
            @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true"  ) boolean requirePasswordChange
    )
            throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        adminSubject.getUserManager().newUser( username, password, requirePasswordChange );
    }

    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changeUserPassword", mode = DBMS )
    public void changeUserPassword(
            @Name( "username" ) String username,
            @Name( "newPassword" ) String newPassword,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange
    )
            throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject enterpriseSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        if ( enterpriseSubject.hasUsername( username ) )
        {
            enterpriseSubject.setPassword( newPassword, requirePasswordChange );
        }
        else if ( !enterpriseSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        else
        {
            enterpriseSubject.getUserManager().setUserPassword( username, newPassword, requirePasswordChange );
            terminateTransactionsForValidUser( username );
            terminateConnectionsForValidUser( username );
        }
    }

    @Description( "Assign a role to the user." )
    @Procedure( name = "dbms.security.addRoleToUser", mode = DBMS )
    public void addRoleToUser(@Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws IOException, InvalidArgumentsException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        adminSubject.getUserManager().addRoleToUser( roleName, username );
    }

    @Description( "Unassign a role from the user." )
    @Procedure( name = "dbms.security.removeRoleFromUser", mode = DBMS )
    public void removeRoleFromUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        if ( adminSubject.hasUsername( username ) && roleName.equals( PredefinedRolesBuilder.ADMIN ) )
        {
            throw new InvalidArgumentsException( "Removing yourself (user '" + username +
                    "') from the admin role is not allowed." );
        }
        adminSubject.getUserManager().removeRoleFromUser( roleName, username );
    }

    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = DBMS )
    public void deleteUser( @Name( "username" ) String username ) throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        if ( adminSubject.hasUsername( username ) )
        {
            throw new InvalidArgumentsException( "Deleting yourself (user '" + username +
                    "') is not allowed." );
        }
        adminSubject.getUserManager().deleteUser( username );
        terminateTransactionsForValidUser( username );
        terminateConnectionsForValidUser( username );
    }

    @Description( "Suspend the specified user." )
    @Procedure( name = "dbms.security.suspendUser", mode = DBMS )
    public void suspendUser( @Name( "username" ) String username ) throws IOException, InvalidArgumentsException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        if ( adminSubject.hasUsername( username ) )
        {
            throw new InvalidArgumentsException( "Suspending yourself (user '" + username +
                    "') is not allowed." );
        }
        adminSubject.getUserManager().suspendUser( username );
        terminateTransactionsForValidUser( username );
        terminateConnectionsForValidUser( username );
    }

    @Description( "Activate a suspended user." )
    @Procedure( name = "dbms.security.activateUser", mode = DBMS )
    public void activateUser(
            @Name( "username" ) String username,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange
    ) throws IOException, InvalidArgumentsException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        if ( adminSubject.hasUsername( username ) )
        {
            throw new InvalidArgumentsException( "Activating yourself (user '" + username +
                    "') is not allowed." );
        }
        adminSubject.getUserManager().activateUser( username, requirePasswordChange );
    }

    @Description( "Show the current user." )
    @Procedure( name = "dbms.security.showCurrentUser", mode = DBMS )
    public Stream<UserResult> showCurrentUser() throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject enterpriseSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        EnterpriseUserManager userManager = enterpriseSubject.getUserManager();
        return Stream.of( new UserResult( enterpriseSubject.username(),
                userManager.getRoleNamesForUser( enterpriseSubject.username() ),
                userManager.getUser( enterpriseSubject.username() ).getFlags() ) );
    }

    @Description( "List all local users." )
    @Procedure( name = "dbms.security.listUsers", mode = DBMS )
    public Stream<UserResult> listUsers() throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        EnterpriseUserManager userManager = adminSubject.getUserManager();
        Set<String> users = userManager.getAllUsernames();
        List<UserResult> results = new ArrayList<>();
        for ( String u : users )
        {
            results.add(
                    new UserResult( u, userManager.getRoleNamesForUser( u ), userManager.getUser( u ).getFlags() ) );
        }
        return results.stream();
    }

    @Description( "List all available roles." )
    @Procedure( name = "dbms.security.listRoles", mode = DBMS )
    public Stream<RoleResult> listRoles() throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        EnterpriseUserManager userManager = adminSubject.getUserManager();
        Set<String> roles = userManager.getAllRoleNames();
        List<RoleResult> results = new ArrayList<>();
        for ( String r : roles )
        {
            results.add( new RoleResult( r, userManager.getUsernamesForRole( r ) ) );
        }
        return results.stream();
    }

    @Description( "List all roles assigned to the specified user." )
    @Procedure( name = "dbms.security.listRolesForUser", mode = DBMS )
    public Stream<StringResult> listRolesForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject subject = ensureSelfOrAdminAuthSubject( username );
        return subject.getUserManager().getRoleNamesForUser( username ).stream().map( StringResult::new );
    }

    @Description( "List all users currently assigned the specified role." )
    @Procedure( name = "dbms.security.listUsersForRole", mode = DBMS )
    public Stream<StringResult> listUsersForRole( @Name( "roleName" ) String roleName )
            throws InvalidArgumentsException, IOException
    {
        StandardEnterpriseAuthSubject adminSubject = ensureAdminAuthSubject();
        return adminSubject.getUserManager().getUsernamesForRole( roleName ).stream().map( StringResult::new );
    }

    @Description( "Create a new role." )
    @Procedure( name = "dbms.security.createRole", mode = DBMS)
    public void createRole( @Name( "roleName" ) String roleName )
        throws InvalidArgumentsException, IOException
    {
        ensureAdminAuthSubject().getUserManager().newRole( roleName );
    }

    @Description( "Delete the specified role. Any role assignments will be removed." )
    @Procedure( name = "dbms.security.deleteRole", mode = DBMS)
    public void deleteRole( @Name( "roleName" ) String roleName )
        throws InvalidArgumentsException, IOException
    {
        ensureAdminAuthSubject().getUserManager().deleteRole( roleName );
    }

    // ----------------- helpers ---------------------

    private void terminateTransactionsForValidUser( String username )
    {
        getActiveTransactions()
                .stream()
                .filter( tx ->
                    getUsernameFromAccessMode( tx.mode() ).equals( username ) &&
                    !tx.isUnderlyingTransaction( this.tx )
                ).forEach( tx -> tx.markForTermination( Status.Transaction.Terminated ) );
    }

    private void terminateConnectionsForValidUser( String username )
    {
        getBoltConnectionTracker().getActiveConnections( username ).forEach( ManagedBoltStateMachine::terminate );
    }

    private Set<KernelTransactionHandle> getActiveTransactions()
    {
        return graph.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
    }

    private BoltConnectionTracker getBoltConnectionTracker()
    {
        return graph.getDependencyResolver().resolveDependency( BoltConnectionTracker.class );
    }

    private StandardEnterpriseAuthSubject ensureAdminAuthSubject()
    {
        StandardEnterpriseAuthSubject enterpriseAuthSubject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        if ( !enterpriseAuthSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        return enterpriseAuthSubject;
    }

    private StandardEnterpriseAuthSubject ensureSelfOrAdminAuthSubject( String username )
            throws InvalidArgumentsException
    {
        StandardEnterpriseAuthSubject subject = StandardEnterpriseAuthSubject.castOrFail( authSubject );
        subject.getUserManager().getUser( username );

        if ( subject.isAdmin() || subject.hasUsername( username ) )
        {
            return subject;
        }
        throw new AuthorizationViolationException( PERMISSION_DENIED );
    }

    public static class StringResult
    {
        public final String value;

        StringResult( String value )
        {
            this.value = value;
        }
    }

    public static class UserResult
    {
        public final String username;
        public final List<String> roles;
        public final List<String> flags;

        UserResult( String username, Set<String> roles, Iterable<String> flags )
        {
            this.username = username;
            this.roles = new ArrayList<>();
            this.roles.addAll( roles );
            this.flags = new ArrayList<>();
            for ( String f : flags ) {this.flags.add( f );}
        }
    }

    public static class RoleResult
    {
        public final String role;
        public final List<String> users;

        RoleResult( String role, Set<String> users )
        {
            this.role = role;
            this.users = new ArrayList<>();
            this.users.addAll( users );
        }
    }
}
