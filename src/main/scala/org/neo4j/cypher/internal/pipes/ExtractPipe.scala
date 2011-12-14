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
package org.neo4j.cypher.internal.pipes

import java.lang.String
import org.neo4j.cypher.commands._
import collection.Seq
import collection.immutable.Map
import org.neo4j.cypher.symbols.{SymbolTable, Identifier}

//This class will extract properties and other stuff to make the maps
//easy to work with for other pipes
class ExtractPipe(source: Pipe, val returnItems: Seq[ReturnItem]) extends PipeWithSource(source) {
  def dependencies = returnItems.flatMap(_.dependencies)

  type MapTransformer = Map[String, Any] => Map[String, Any]

  def getSymbolType(item: ReturnItem): Identifier = item.identifier

  val symbols: SymbolTable = source.symbols.add(returnItems.map(_.identifier):_*)


  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = {
    source.createResults(params).map(row => {
      val projection: Map[String, Any] = returnItems.map( returnItem =>returnItem.columnName -> returnItem(row) ).toMap
      projection ++ row
    })
  }

  override def executionPlan(): String = source.executionPlan() + "\r\nExtract([" + source.symbols.keys.mkString(",") + "] => [" + returnItems.map(_.columnName).mkString(", ") + "])"
}

