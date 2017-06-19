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
package org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.KeyToken
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.spi.v3_3.QueryContext

case class GetDegree(offset: Int, typ: Option[KeyToken], direction: SemanticDirection) extends Expression {

  private val getDegree: (QueryContext, Long) => Long =
    typ match {
      // No reltype specified means all relationships, no matter type
      case None =>
        (qtx, node) => qtx.nodeGetDegree(node, direction)

      case Some(t) =>
        (qtx, node) =>
          val maybeInt: Option[Int] = t.getOptId(qtx)
          if(maybeInt.isEmpty)
            0
          else
            qtx.nodeGetDegree(node, direction, maybeInt.get)
    }

  override def rewrite(f: (Expression) => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq.empty

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val nodeId = ctx.getLong(offset)
    if (nodeId == -1)
      null
    else
      getDegree(state.query, nodeId)
  }

  override def symbolTableDependencies: Set[String] = Set.empty
}
