package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{LogicalPlan, NodeLogicalLeafPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.{QueryGraph, PlannerQuery, Read}

case class PlanEagerness(planUpdates: LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan])
  extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {

  override def apply(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val containsMerge = plannerQuery.queryGraph.containsMerge

    //--------------
    // Plan eagerness before updates (on lhs)
    // (I) Protect reads from future writes
    val thisReadOverlapsFutureWrites = readOverlapsFutureWrites(plannerQuery, lhs, head, containsMerge)

    // NOTE: In case this is a merge, eagerness has to be planned on top of updates, and not on lhs (see (III) below)
    val newLhs = if (thisReadOverlapsFutureWrites && !containsMerge)
      context.logicalPlanProducer.planEager(lhs)
    else
      lhs

    //--------------
    // Plan updates
    val updatePlan = planUpdates(plannerQuery, newLhs, head)

    //--------------
    // Plan eagerness after updates
    // (II) Protect writes from future reads
    val thisWriteOverlapsFutureReads = writeOverlapsFutureReads(plannerQuery, head)
    // (III) Protect merge reads from future writes
    val thisMergeReadOverlapsFutureWrites = containsMerge && thisReadOverlapsFutureWrites

    if (thisWriteOverlapsFutureReads || thisMergeReadOverlapsFutureWrites)
      context.logicalPlanProducer.planEager(updatePlan)
    else
      updatePlan
  }

  private def readOverlapsFutureWrites(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean,
                                       containsMerge: Boolean): Boolean = {

    val thisRead: Read = readQGWithoutStableNodeVariable(plannerQuery, lhs, head)
    val allUpdates = plannerQuery.allQueryGraphs.map(_.updates)

    // Merge needs to see it's own changes, so if the conflict is only between the reads and writes of MERGE,
    // we should not be eager
    val futureUpdates = if (containsMerge)
      allUpdates.tail
    else
      allUpdates

    futureUpdates.exists(_ overlaps thisRead)
  }

  private def writeOverlapsFutureReads(plannerQuery: PlannerQuery, head: Boolean): Boolean = {
    val queryGraph = plannerQuery.queryGraph
    if (head && queryGraph.writeOnly)
      false // No conflict if the first query graph only contains writes
    else {
      val thisWrite = queryGraph.updates
      val futureReads = plannerQuery.allQueryGraphs.tail.map(_.reads)

      val knownFutureConflict = futureReads.exists(thisWrite.overlaps)
      val deleteAndLaterMerge = queryGraph.containsDelete && plannerQuery.allQueryGraphs.exists(_.containsMerge)
      knownFutureConflict || deleteAndLaterMerge
    }
  }

  private def readQGWithoutStableNodeVariable(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean): Read = {
    val originalQG = plannerQuery.queryGraph
    val graph = lhs.leftMost match {
      case n: NodeLogicalLeafPlan if head && includesAllPredicates(originalQG, n)  => originalQG.withoutPatternNode(n.idName)
      case _ => originalQG
    }
    graph.reads
  }

  def includesAllPredicates(originalQG: QueryGraph, n: NodeLogicalLeafPlan): Boolean = {
    def p(qg: QueryGraph) = qg.selections.predicatesGiven(qg.argumentIds + n.idName).toSet

    val a = p(n.solved.lastQueryGraph)
    val b = p(originalQG)
    a == b
  }
}
