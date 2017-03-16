package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.bork.PipeLine
import org.neo4j.cypher.internal.compiler.v3_2.bork.PipeLine.Direction
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan

class RealExecutionEngine {
  def execute(executionPlan: Operator, apa: Map[(LogicalPlan, Direction), PipeLine]): ExecutionPlan = ???
}