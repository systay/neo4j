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
package org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection

case class ExpandAllRegisterPipe(source: Pipe, from: Int, to: Int, rel: Int, dir: SemanticDirection, types: LazyTypes)
                                (val id: Id = new Id)
                                (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RegisterPipe {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input flatMap {
      row =>
        val nodeId = row.getLong(from)
        if (nodeId == -1)
          None
        else {
          val rels = state.query.getRelationshipsForIds(nodeId, dir, types.types(state.query))
          rels map { r =>
            val newRow = row.clone()
            newRow.setLong(rel, r.getId)
            newRow.setLong(to, r.getOtherNodeId(nodeId))
            newRow
          }
        }
    }
  }
}
