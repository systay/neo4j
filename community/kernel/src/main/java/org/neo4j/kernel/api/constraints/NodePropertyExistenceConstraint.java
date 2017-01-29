/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.constraints;

import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;

/**
 * Description of constraint enforcing nodes to contain a certain property.
 */
public class NodePropertyExistenceConstraint extends NodePropertyConstraint
{
    public NodePropertyExistenceConstraint( NodePropertyDescriptor descriptor )
    {
        super( descriptor );
    }

    @Override
    public void added( ChangeVisitor visitor ) throws CreateConstraintFailureException
    {
        visitor.visitAddedNodePropertyExistenceConstraint( this );
    }

    @Override
    public void removed( ChangeVisitor visitor )
    {
        visitor.visitRemovedNodePropertyExistenceConstraint( this );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        String labelName = labelName( tokenNameLookup );
        String boundIdentifier = labelName.toLowerCase();
        return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT exists(%s.%s)",
                boundIdentifier, labelName, boundIdentifier,
                descriptor.propertyNameText( tokenNameLookup ) );
    }

    @Override
    public String toString()
    {
        return String.format( "CONSTRAINT ON ( n:label[%s] ) ASSERT exists(n.property[%s])",
                descriptor.getLabelId(), descriptor.propertyIdText() );
    }
}