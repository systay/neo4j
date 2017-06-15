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

import org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.RegisterPipeBuilder.turnVariableInRegisterRead
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{Expression => CommandsExpression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{FilterPipe, Pipe}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ActualPipeBuilder, PipeBuilder, PipeBuilderFactory, PipeExecutionBuilderContext}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{AllNodesScan, LogicalPlan, Selection}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.enterprise_interpreted_runtime.pipes.AllNodesScanRegisterPipe
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Property, PropertyKeyName, Variable, Expression => ASTExpression}
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.frontend.v3_3.{PlannerName, Rewriter, SemanticTable, bottomUp, inSequence}
import org.neo4j.cypher.internal.ir.v3_3.IdName
import org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.RegisterPipeBuilder.turnVariableInRegisterRead

case class RegisterPipeBuilderFactory() extends PipeBuilderFactory {
  override def apply(monitors: Monitors,
                     recurse: (LogicalPlan) => Pipe,
                     readOnly: Boolean,
                     idMap: Map[LogicalPlan, Id],
                     expressionRewriter: Rewriter)
                    (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder =
    new RegisterPipeBuilder(monitors, recurse, readOnly, idMap, expressionRewriter)(context, planContext)
}

class NodeProperty(offset: Int, propertyKeyName: PropertyKeyName) {

}

object RegisterPipeBuilder {
  def turnVariableInRegisterRead(in: Rewriter, semanticTable: SemanticTable): Rewriter = inSequence(in, RegisterRewriter)

  val RegisterRewriter = new Rewriter {
    private val instance = bottomUp(Rewriter.lift {
      case Property(Variable(ident), propertyKey) => ???
    })

    override def apply(that: AnyRef): AnyRef = instance.apply(that)

  }
}

class RegisterPipeBuilder(monitors: Monitors,
                          recurse: LogicalPlan => Pipe,
                          readOnly: Boolean,
                          idMap: Map[LogicalPlan, Id],
                          rewriteExpressions: Rewriter)
                         (implicit context: PipeExecutionBuilderContext,
                          planContext: PlanContext)
  extends ActualPipeBuilder(monitors, recurse, readOnly, idMap, turnVariableInRegisterRead(rewriteExpressions, context.semanticTable))(context, planContext) {

  private val ctx = context.asInstanceOf[RegisterPipeExecutionBuilderContext]

  override def build(plan: LogicalPlan): Pipe = {
    val id = idMap.getOrElse(plan, new Id)
    val registerAllocations = ctx.registerAllocation(plan)
    plan match {
      case AllNodesScan(IdName(ident), _) =>
        AllNodesScanRegisterPipe(registerAllocations.getLongOffsetFor(ident), registerAllocations)(id)
      case _ => super.build(plan)
    }
  }

  private def rewriteExpressions(in: ASTExpression): ASTExpression = in

  override def build(plan: LogicalPlan, source: Pipe): Pipe = {
    val id = idMap.getOrElse(plan, new Id)

    val registerAllocations = ctx.registerAllocation(plan)
    plan match {
      case _ => super.build(plan, source)
    }
  }

  override def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = super.build(plan, lhs, rhs)

}

class RegisterPipeExecutionBuilderContext(cardinality: Metrics.CardinalityModel, semanticTable: SemanticTable,
                                          plannerName: PlannerName, val registerAllocation: Map[LogicalPlan, RegisterAllocations])
  extends PipeExecutionBuilderContext(cardinality, semanticTable, plannerName)