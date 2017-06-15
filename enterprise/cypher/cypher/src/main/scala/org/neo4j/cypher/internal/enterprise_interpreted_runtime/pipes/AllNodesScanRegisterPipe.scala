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
package org.neo4j.cypher.internal.enterprise_interpreted_runtime.pipes


import org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.RegisterAllocations
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{PipeMonitor, QueryState}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.graphdb.Node

case class AllNodesScanRegisterPipe(offset: Int, registers: RegisterAllocations)(val id: Id = new Id)
                                   (implicit pipeMonitor: PipeMonitor) extends RegisterPipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    state.query.nodeOps.all.map { (n: Node) =>
      val ctx = ExecutionContext(sizeOfLongs = registers.numberOfLongs, sizeOfRefs = registers.numberOfObjects)
      ctx.setLong(offset, n.getId)
      ctx
    }
  }

  override def monitor: PipeMonitor = pipeMonitor
}
