package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{LogicalPlan, NodeLogicalLeafPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.{PlannerQuery, Read}

case class PlanEagerness(planUpdates: LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan])
  extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {

  override def apply(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val thisRead: Read = readQGWithoutStableNodeVariable(plannerQuery, lhs, head)

    val allUpdates = plannerQuery.allQueryGraphs.map(_.updates)

    val containsMerge = plannerQuery.queryGraph.containsMerge
    // Merge needs to see it's own changes, so if the conflict is only between the reads and writes of MERGE,
    // no need to protect
    val futureUpdates = if (containsMerge)
      allUpdates.tail
    else
      allUpdates

    val thisReadOverlapsFutureWrite = futureUpdates.exists(_ overlaps thisRead)

    val newLhs = if (thisReadOverlapsFutureWrite && !containsMerge)
      context.logicalPlanProducer.planEager(lhs)
    else
      lhs

    val updatePlan = planUpdates(plannerQuery, newLhs, head)

    val thisWrite = plannerQuery.queryGraph.updates

    val futureReads = plannerQuery.allQueryGraphs.tail.map(_.reads)

    if (futureReads.exists(thisWrite.overlaps) || (thisReadOverlapsFutureWrite && containsMerge))
      context.logicalPlanProducer.planEager(updatePlan)
    else
      updatePlan
  }

  private def readQGWithoutStableNodeVariable(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean): Read = {
    val originalQG = plannerQuery.queryGraph
    val graph = lhs.leftMost match {
      case n: NodeLogicalLeafPlan if head => originalQG.withoutPatternNode(n.idName)
      case _ => originalQG
    }
    graph.reads
  }
}
