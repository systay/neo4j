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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.operators

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized._
import org.neo4j.cypher.internal.spi.v3_3.QueryContext

class AllNodeScanOperator(longsPerRow: Int, refsPerRow: Int, offset: Int) extends LeafOperator {

  override def init(state: QueryState, context: QueryContext): Unit = {
    state.operatorState(this) = context.nodeOps.allPrimitive
  }

  def operate(source: Continuation,
              data: Morsel,
              context: QueryContext,
              state: QueryState): (ReturnType, Continuation) = {
    val nodeIterator = source match {
      case Init =>
        context.nodeOps.allPrimitive
      case ContinueWithSource(it) =>
        it.asInstanceOf[PrimitiveLongIterator]
    }

    val longs: Array[Long] = data.longs

    var processedRows = 0
    while (nodeIterator.hasNext && processedRows < data.validRows) {
      longs(processedRows * longsPerRow + offset) = nodeIterator.next()
      processedRows += 1
    }

    data.validRows = processedRows

    val next = if (nodeIterator.hasNext)
      ContinueWithSource(nodeIterator)
    else
      Done


    (MorselType, next)
  }
}