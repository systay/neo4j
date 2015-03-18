package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_3.ast.Expression

case class ProduceResult(columns: List[String], inner: LogicalPlan) extends LogicalPlan {
  val lhs = Some(inner)

  def solved = inner.solved

  def availableSymbols = inner.availableSymbols

  def rhs = None

  def mapExpressions(f: (Set[IdName], Expression) => Expression) =
    copy(inner = inner.mapExpressions(f))
}
