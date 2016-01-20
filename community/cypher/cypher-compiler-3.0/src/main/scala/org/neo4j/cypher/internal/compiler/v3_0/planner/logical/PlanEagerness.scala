package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.{QueryGraph, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{NodeLogicalLeafPlan, LogicalPlan}

case class PlanEagerness(inner: LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan])
  extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {

  override def apply(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = {
    /*
     * Eagerness pass 1 -- protect already planned reads against future writes
     */

    val thisRead: QueryGraph = if (head) {
      // The first leaf node is always reading through a stable iterator.
      // We will only consider this analysis for all other node iterators.
      val leaves = lhs.leaves.collect {
        case n: NodeLogicalLeafPlan => n.idName
      }

      val originalQG = plannerQuery.queryGraph.readQG
      if (leaves.isEmpty) // the query did not start with a read, possibly CREATE () ...
        originalQG
      else {
        val stableLeaf = leaves.head
        originalQG.withoutPatternNode(stableLeaf)
      }

    } else plannerQuery.queryGraph.readQG


    val futureWrites = plannerQuery.allQueryGraphs.map(_.writeQG)

    val newLhs = if (futureWrites.exists(qg => qg.overlaps(thisRead)))
      context.logicalPlanProducer.planEager(lhs)
    else lhs

    newLhs

    // call inner

    /*
     * Eagerness pass 2 -- protect future reads against writes planned by inner
     */

    // check conflicts between results from inner and plannerQuery.tail.getOrElse(empty).allQueryGraphs.map(_.readQG)
  }
}
