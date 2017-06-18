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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{Expression => CommandsExpression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{expressions => commandsExpressions, predicates => commandsPredicates}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.Pipe
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{AllNodesScan, FindShortestPaths, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.enterprise_interpreted_runtime.pipes.AllNodesScanRegisterPipe
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Property, PropertyKeyName, Variable, Expression => ASTExpression}
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, PlannerName, Rewriter, SemanticCheck, SemanticCheckResult, SemanticTable, bottomUp}
import org.neo4j.cypher.internal.ir.v3_3.IdName

case class RegisterPipeBuilderFactory() extends PipeBuilderFactory {
  override def apply(monitors: Monitors,
                     recurse: (LogicalPlan) => Pipe,
                     readOnly: Boolean,
                     idMap: Map[LogicalPlan, Id],
                     expressionConverters: ExpressionConverters)
                    (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder =
    new RegisterPipeBuilder(monitors, recurse, readOnly, idMap, expressionConverters)(context, planContext)
}

case class NodeProperty(offset: Int, propertyKeyName: PropertyKeyName)(val position: InputPosition) extends ASTExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class RelationshipProperty(offset: Int, propertyKeyName: PropertyKeyName)(val position: InputPosition) extends ASTExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}

class RegisterExpressionRewriter(semanticTable: SemanticTable, registerAllocations: RegisterAllocations)
  extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val instance = bottomUp(Rewriter.lift {
    case p@Property(Variable(ident), propertyKey) if semanticTable.isNode(ident) => NodeProperty(registerAllocations.getLongOffsetFor(ident), propertyKey)(p.position)
    case p@Property(Variable(ident), propertyKey) if semanticTable.isRelationship(ident) => RelationshipProperty(registerAllocations.getLongOffsetFor(ident), propertyKey)(p.position)
    case Property(Variable(_), _) => ???
  })
}

class RegisterPipeBuilder(monitors: Monitors,
                          recurse: LogicalPlan => Pipe,
                          readOnly: Boolean,
                          idMap: Map[LogicalPlan, Id],
                          expressionConverters: ExpressionConverters)
                         (implicit context: PipeExecutionBuilderContext,
                          planContext: PlanContext)
  extends ActualPipeBuilder(monitors, recurse, readOnly, idMap, expressionConverters)(context, planContext) {

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

      case _: FindShortestPaths =>
        throw new RegisterAllocationFailed("Shortest path not supported in the enterprise interpreted runtime")

      case _ =>
        val rewriter = new RegisterExpressionRewriter(ctx.semanticTable, ctx.registerAllocation(plan))
        val registeredPlan = plan.endoRewrite(rewriter)
        val result = super.build(registeredPlan, source)
        result
    }
  }

  override def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = super.build(plan, lhs, rhs)

}

class RegisterPipeExecutionBuilderContext(cardinality: Metrics.CardinalityModel, semanticTable: SemanticTable,
                                          plannerName: PlannerName, val registerAllocation: Map[LogicalPlan, RegisterAllocations])
  extends PipeExecutionBuilderContext(cardinality, semanticTable, plannerName)