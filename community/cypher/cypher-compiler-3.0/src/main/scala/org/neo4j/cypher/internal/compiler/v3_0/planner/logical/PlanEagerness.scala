package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.{QueryGraph, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{NodeLogicalLeafPlan, LogicalPlan}

case class PlanEagerness(inner: LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan])
  extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {

  override def apply(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val thisRead: QueryGraph = if (head) {
      val unstableLeaves = lhs.leaves.collect {
        case n: NodeLogicalLeafPlan => n.idName
      }

      val originalQG = plannerQuery.queryGraph.readQG
      if (unstableLeaves.isEmpty)
        originalQG
      else {
        val stableLeaf = unstableLeaves.head
        originalQG.withoutPatternNode(stableLeaf)
      }

    } else plannerQuery.queryGraph.readQG


    val futureWrites = plannerQuery.allQueryGraphs.map(_.writeQG)

    val newLhs = if (futureWrites.exists(qg => qg.overlaps(thisRead)))
      context.logicalPlanProducer.planEager(lhs)
    else lhs

    newLhs
  }
}
