package org.neo4j.cypher.internal.compatibility.v3_3.runtime


import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{NestedPipeExpression, Pipe}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.compiler.v3_3.{ast => compilerAst}

class NestedPipeExpressionBuilder(buildPipesFromLogicalPlan: LogicalPlan => Pipe) extends Rewriter {
  private val instance = bottomUp(Rewriter.lift {
    case expr@compilerAst.NestedPlanExpression(patternPlan, expression) =>
      val pipe = buildPipesFromLogicalPlan(patternPlan)
      val result = NestedPipeExpression(pipe, expression)(expr.position)
      result
  })

  override def apply(that: AnyRef): AnyRef = instance.apply(that)
}
