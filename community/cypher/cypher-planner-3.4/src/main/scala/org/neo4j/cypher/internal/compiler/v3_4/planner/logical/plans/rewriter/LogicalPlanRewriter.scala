/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_4.phases.{CompilerContext, LogicalPlanState}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Types.{FloatType, IntegerType, NullType}
import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.{TypeExpectations, TypeTable}
import org.neo4j.cypher.internal.frontend.v3_4.phases.{Condition, Phase}
import org.neo4j.cypher.internal.util.v3_4.{InputPosition, Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions.{Add, Expression, Null}

/*
 * Rewriters that live here are required to adhere to the contract of
 * receiving a valid plan and producing a valid plan. It should be possible
 * to disable any and all of these rewriters, and still produce correct behavior.
 */
case class PlanRewriter(rewriterSequencer: String => RewriterStepSequencer) extends LogicalPlanRewriter {
  override def description: String = "optimize logical plans using heuristic rewriting"

  override def postConditions: Set[Condition] = Set.empty

  override def instance(context: CompilerContext) = fixedPoint(rewriterSequencer("LogicalPlanRewriter")(
    fuseSelections,
    unnestApply,
    cleanUpEager,
    simplifyPredicates,
    unnestOptional,
    predicateRemovalThroughJoins,
    removeIdenticalPlans,
    pruningVarExpander,
    useTop,
    simplifySelections
  ).rewriter)
}

trait LogicalPlanRewriter extends Phase[CompilerContext, LogicalPlanState, LogicalPlanState] {
  override def phase: CompilationPhase = LOGICAL_PLANNING

  def instance(context: CompilerContext): Rewriter

  override def process(from: LogicalPlanState, context: CompilerContext): LogicalPlanState = {
    val rewritten = from.logicalPlan.endoRewrite(instance(context))
    rewritten.assignIds() // This should be the only place where ids are assigned.
    from.copy(maybeLogicalPlan = Some(rewritten))
  }
}

case class AddGradualTyping(typeTable: TypeTable, typeExpectations: TypeExpectations) extends Rewriter {

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case a@Add(lhs, rhs) if typeTable.get(a) == Set(IntegerType) => AddIntegers(lhs, rhs)(a.position)
    case a@Add(lhs, rhs) if typeTable.get(a) == Set(FloatType) => AddFloats(lhs, rhs)(a.position)
    case a: Expression if typeTable.get(a) == Set(NullType) => Null()(a.position)
    case a@Add(lhs, rhs) => AddDynamic(lhs, rhs)(a.position)
  })

  override def apply(input: AnyRef) = instance.apply(input)
}

case class AddIntegers(expression: Expression, expression1: Expression)(val position: InputPosition) extends Expression
case class AddFloats(expression: Expression, expression1: Expression)(val position: InputPosition) extends Expression
case class AddDynamic(expression: Expression, expression1: Expression)(val position: InputPosition) extends Expression