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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.{Morsel, Operator, QueryState}
import org.neo4j.cypher.internal.frontend.v3_3.InternalException
import org.neo4j.cypher.internal.spi.v3_3.QueryContext

class AllNodeScanOperator(longsPerRow: Int, refsPerRow: Int, offset: Int) extends Operator {

  override def init(state: QueryState, context: QueryContext): Unit = {
    state.operatorState(this) = context.nodeOps.allPrimitive
  }

  override def operate(data: Morsel, context: QueryContext, state: QueryState): Morsel = {
    val nodeIterator = state.operatorState.getOrElse(this,
      throw new InternalException("Operator not initiatied correctly")).asInstanceOf[PrimitiveLongIterator]

    val longs: Array[Long] = data.longs

    var processedRows = 0
    while (nodeIterator.hasNext && processedRows < data.rows) {
      longs(processedRows * longsPerRow + offset) = nodeIterator.next()
      processedRows += 1
    }

    data.moreDataToCome = nodeIterator.hasNext
    data.rows = processedRows

    data
  }
}
