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
package org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime

import org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.pipes.{AllNodesScanRegisterPipe, ProduceResultsRegisterPipe}
import org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.{expressions => runtimeExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{expressions => commandsExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.Pipe
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.frontend.v3_3.{PlannerName, SemanticTable}
import org.neo4j.cypher.internal.ir.v3_3.IdName

class RegisterPipeBuilder(monitors: Monitors,
                          recurse: LogicalPlan => Pipe,
                          readOnly: Boolean,
                          idMap: Map[LogicalPlan, Id],
                          expressionConverters: ExpressionConverters)
                         (implicit context: PipeExecutionBuilderContext,
                          planContext: PlanContext)
  extends ActualPipeBuilder(monitors, recurse, readOnly, idMap, expressionConverters)(context, planContext) {

  private val ctx = context.asInstanceOf[RegisterPipeExecutionBuilderContext]

  override def build(plan: LogicalPlan, id: Id): Pipe = {
    val registerAllocations = ctx.registerAllocation(plan)
    plan match {
      case AllNodesScan(IdName(ident), _) =>
        AllNodesScanRegisterPipe(registerAllocations.getLongOffsetFor(ident), registerAllocations)(id)
      case _ => super.build(plan, id)
    }
  }

  private def expressionForSlot(key: String, slot: Slot, table: SemanticTable): commandsExpressions.Expression =
    slot match {
      case LongSlot(offset) if table.isNode(key) => runtimeExpressions.NodeFromRegister(offset)
      case RefSlot(offset) => runtimeExpressions.ValueFromRegister(offset)
    }

  override def build(plan: LogicalPlan, source: Pipe, id: Id): Pipe = {
    val registerAllocations = ctx.registerAllocation(plan)
    plan match {

      case _: FindShortestPaths | _: Expand =>
        throw new RegisterAllocationFailed(s"${plan.productPrefix} not supported in the enterprise interpreted runtime")

      case ProduceResult(columns, _) =>
        val expressions = columns map {
          key => key -> expressionForSlot(key, registerAllocations.slots(key), context.semanticTable)
        }
        ProduceResultsRegisterPipe(source, expressions)(id)

      case _ =>
        val rewriter = new RegisterExpressionRewriter(ctx.semanticTable, ctx.registerAllocation(plan))
        val registeredPlan = plan.endoRewrite(rewriter)
        super.build(registeredPlan, source, id)
    }
  }

  override def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe, id: Id): Pipe =
    super.build(plan, lhs, rhs, id)

}

class RegisterPipeExecutionBuilderContext(cardinality: Metrics.CardinalityModel, semanticTable: SemanticTable,
                                          plannerName: PlannerName, val registerAllocation: Map[LogicalPlan, RegisterAllocations])
  extends PipeExecutionBuilderContext(cardinality, semanticTable, plannerName)

case class RegisterPipeBuilderFactory() extends PipeBuilderFactory {
  override def apply(monitors: Monitors,
                     recurse: (LogicalPlan) => Pipe,
                     readOnly: Boolean,
                     idMap: Map[LogicalPlan, Id],
                     expressionConverters: ExpressionConverters)
                    (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder =
    new RegisterPipeBuilder(monitors, recurse, readOnly, idMap, expressionConverters)(context, planContext)
}
