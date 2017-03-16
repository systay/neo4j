package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.{LogicalPlan, TreeBuilder}
import org.neo4j.cypher.internal.ir.v3_2.IdName

class LogicalPlan2Operator(idMap: Map[IdName, Slot]) extends TreeBuilder[Operator] {

  override def create(plan: LogicalPlan): Operator = {
    val result = super.create(plan)
    result.becomeParent()
    result
  }

  override protected def build(plan: LogicalPlan): Operator = plan match {
    case plans.AllNodesScan(IdName(id), _) =>
      new AllNodesScanOp(0)
  }

  override protected def build(plan: LogicalPlan, source: Operator): Operator = ???

  override protected def build(plan: LogicalPlan, lhs: Operator, rhs: Operator): Operator = ???
}
