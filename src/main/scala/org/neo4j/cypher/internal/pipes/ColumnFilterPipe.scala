/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import collection.Seq
import org.neo4j.cypher.internal.commands.ReturnItem

class ColumnFilterPipe(source: Pipe, val returnItems: Seq[ReturnItem])
  extends PipeWithSource(source) {
  val returnItemNames = returnItems.map(_.columnName)
  val symbols = source.symbols.filter(returnItemNames: _*)


  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] =
    source.createResults(params).map(_.filterKeys(returnItemNames.contains))

  override def executionPlan(): String = {
    source.executionPlan() + "\r\n" + "ColumnFilter([" + source.symbols.keys + "] => [" + returnItemNames.mkString(",") + "])"
  }

  def dependencies = Seq()
}