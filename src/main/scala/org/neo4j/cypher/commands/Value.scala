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

import org.neo4j.graphdb.{Relationship, PropertyContainer}

abstract sealed class Value {
  def value(m: Map[String, Any]): Any
}

case class Literal(v: Any) extends Value {
  def value(m: Map[String, Any]) = v
}

case class PropertyValue(identifier: String, property: String) extends Value {
  def value(m: Map[String, Any]): Any = m(identifier).asInstanceOf[PropertyContainer].getProperty(property)
}

case class RelationshipTypeValue(identifier:String) extends Value {
  def value(m: Map[String, Any]): Any = m(identifier).asInstanceOf[Relationship].getType.toString
}

