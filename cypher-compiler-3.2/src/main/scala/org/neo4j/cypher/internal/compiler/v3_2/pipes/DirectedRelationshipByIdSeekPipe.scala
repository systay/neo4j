/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.helpers.ListSupport
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id

case class DirectedRelationshipByIdSeekPipe(ident: String, relIdExpr: SeekArgs, toNode: String, fromNode: String)
                                           (val id: Id = new Id)
                                           (implicit pipeMonitor: PipeMonitor)
  extends Pipe
  with ListSupport {

  relIdExpr.registerOwningPipe(this)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val ctx = state.createOrGetInitialContext()
    val relIds = relIdExpr.expressions(ctx, state).flatMap(Option(_))
    new DirectedRelationshipIdSeekIterator(ident, fromNode, toNode, ctx, state.query.relationshipOps, relIds.iterator)
  }

  def monitor = pipeMonitor
 }
