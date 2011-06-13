/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.commands


abstract sealed class ReturnItem(val identifier:Identifier)

case class EntityOutput(name: String) extends ReturnItem(NodeIdentifier(name))  // todo relationship-entity-type
case class PropertyOutput(entityName:String, propName:String) extends ReturnItem(PropertyIdentifier(entityName,propName))
case class NullablePropertyOutput(entityName:String, propName:String) extends ReturnItem(PropertyIdentifier(entityName,propName))

abstract sealed class AggregationItem(ident:String) extends ReturnItem(AggregationIdentifier(ident))

case class Count(variable:String) extends AggregationItem(variable)