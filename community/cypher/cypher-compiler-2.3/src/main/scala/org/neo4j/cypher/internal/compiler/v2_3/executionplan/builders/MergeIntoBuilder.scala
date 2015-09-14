/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.commands.{Pattern, RelatedTo}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.MergePatternAction
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{MergeIntoPipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable

class MergeIntoBuilder extends PlanBuilder {
  override def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    plan.query.updates.exists {
      case Unsolved(merge@MergePatternAction(patterns, _, _, _, _)) => canWorkWith(patterns, plan.pipe.symbols)
    }


  override def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
    val p: RelatedTo = plan.query.updates.collectFirst {
      case Unsolved(merge@MergePatternAction(patterns, _, _, _, _))
        if canWorkWith(patterns, plan.pipe.symbols) => patterns.head.asInstanceOf[RelatedTo]
    }.get

    val props: Map[KeyToken, Expression] = Map.empty
    val onCreateProps: Map[KeyToken, Expression] = Map.empty
    val onMatchProps: Map[KeyToken, Expression] = Map.empty
    val mergePipe = MergeIntoPipe(plan.pipe, p.left.name, p.relName, p.right.name, p.direction, p.relTypes.head, props, Map.empty, Map.empty)()

    plan.copy(
      pipe = mergePipe
    )
  }

  private def canWorkWith(p: Seq[Pattern], s: SymbolTable): Boolean =
    p.size == 1 &&
      p.forall {
        case r: RelatedTo => s.hasIdentifierNamed(r.left.name) && s.hasIdentifierNamed(r.right.name)
        case _ => false
      }
}
