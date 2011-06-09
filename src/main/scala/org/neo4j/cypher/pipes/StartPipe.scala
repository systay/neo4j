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
package org.neo4j.cypher.pipes

import org.neo4j.cypher.SymbolTable
import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 21:00 
 */

class StartPipe[T <: PropertyContainer](name: String, source: Iterable[T]) extends Pipe {
  def columns: List[String] = List(name)


  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach((x) => {
      f(Map(name -> x))
    })
  }

  def prepare(symbolTable: SymbolTable) {
    source match {
      case nodes: Iterable[Node] => symbolTable.registerNode(name)
      case relationships: Iterable[Relationship] => symbolTable.registerRelationship(name)
    }
  }
}