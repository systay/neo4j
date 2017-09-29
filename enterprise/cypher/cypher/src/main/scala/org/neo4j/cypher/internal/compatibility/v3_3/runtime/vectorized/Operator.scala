/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized

import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

trait Operator {
  def operate(input: Morsel, context: QueryContext, state: QueryState): Morsel
  def init(state: QueryState, context: QueryContext)
}

sealed trait ExecutionState
object Init extends ExecutionState
object Done extends ExecutionState
case class Continue (morsel: Morsel, atRow: Int) extends ExecutionState

class QueryState(val operatorState: mutable.Map[Operator, AnyRef] = mutable.Map[Operator, AnyRef](),
                 val params: MapValue,
                 val inQueue: mutable.Map[Operator, AnyRef] = mutable.Map[Operator, AnyRef]())