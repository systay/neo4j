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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized._
import org.neo4j.cypher.internal.spi.v3_3.QueryContext

/*
Takes an input morsel and compacts all rows to the beginning of it, only keeping the rows that match a predicate
 */
class FilterOperator(pipeline: PipelineInformation, predicate: Predicate) extends Operator {

  override def init(state: QueryState, context: QueryContext): Unit = {}

  override def operate(morsel: Morsel, context: QueryContext, state: QueryState): Morsel = {
    var readingPos = 0
    var writingPos = 0
    val longCount = pipeline.numberOfLongs
    val refCount = pipeline.numberOfReferences
    val currentRow = new MorselExecutionContext(morsel, longCount, refCount, 0)
    val longs = morsel.longs
    val objects = morsel.refs
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    while (readingPos < morsel.rows) {
      System.arraycopy(morsel.longs, readingPos * longCount, longs, longCount * writingPos, longCount)
      System.arraycopy(morsel.refs, readingPos * refCount, objects, refCount * writingPos, refCount)

      if (predicate.isTrue(currentRow)(state = queryState)) {
        writingPos += 1
      }
      readingPos += 1
      currentRow.currentRow = readingPos
    }

    morsel.rows = writingPos
    morsel
  }
}
