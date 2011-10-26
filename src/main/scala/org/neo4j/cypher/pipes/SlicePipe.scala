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
import org.neo4j.cypher.commands.Value

class SlicePipe(source:Pipe, skip:Option[Value], limit:Option[Value]) extends Pipe {
  val symbols: SymbolTable = source.symbols

  //TODO: Make this nicer. I'm sure it's expensive and silly.
  def foreach[U](f: (Map[String, Any]) => U) {
    val first: Map[String, Any] = source.head

    val slicedResult = (skip, limit) match {
      case (None, None) => source
      case (Some(x), None) => source.drop(x(first).asInstanceOf[Int])
      case (None, Some(x)) => source.take(x(first).asInstanceOf[Int])
      case (Some(startAt), Some(count)) => {
        val start = startAt(first).asInstanceOf[Int]
        source.slice(start, start + count(first).asInstanceOf[Int])
      }
    }

    slicedResult.foreach(f)
  }
}