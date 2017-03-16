package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.bork.PipeLine.Direction
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_2.phases.{CompilationContains, CompilationState, CompilerContext}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.bork.{PipeLine, RegisterAllocator}
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_2.phases.Phase

object BuildInterpreted2ExecutionPlan extends Phase[CompilerContext, CompilationState, CompilationState] {
  override def phase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: CompilationState, context: CompilerContext): CompilationState = {
    val logicalPlan = from.logicalPlan
    val calculate: Map[(LogicalPlan, Direction), PipeLine] = RegisterAllocator.calculate(logicalPlan)

    from
  }
}
