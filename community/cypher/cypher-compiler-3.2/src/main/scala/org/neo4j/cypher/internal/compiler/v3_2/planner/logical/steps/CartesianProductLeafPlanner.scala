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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_2.planner._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{LeafPlanner, LogicalPlanningContext}
import org.neo4j.cypher.internal.ir.v3_2.Cardinality

/*
This class will produce cartesian products of all leafs that have very low (<2) cardinalities
 */
case class CartesianProductLeafPlanner(inner: Seq[LeafPlanner]) extends LeafPlanner {

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {

    def cardinality(lp: LogicalPlan): Cardinality =
      context.cardinality.apply(lp.solved, context.input, context.semanticTable)

    val original = qg.allCoveredIds

    val lowCardinalityPlans = inner.
      // Use inner planners to produce all inner leaf plans
      flatMap(planner => planner(qg)).
      // Group plans by solved variables
      groupBy(plan => original -- plan.solved.lastQueryGraph.allCoveredIds).
      // For each grouping, find the lowest cardinality plan
      values.flatMap(plans =>
      plans.
        map(p => p -> cardinality(p)).
        filter(_._2 < Cardinality(2)).
        sortBy(_._2).
        headOption.
        map(_._1))

    for {
      i <- 2 to lowCardinalityPlans.size
      plans <- lowCardinalityPlans.toList.combinations(i)
    } yield plans.reduce[LogicalPlan] {
      case (lhs, rhs) =>
        context.logicalPlanProducer.planCartesianProduct(lhs, rhs)
    }
    //    val logicalPlans: Seq[LogicalPlan] = inner.flatMap(planner => planner(qg)).filter {
    //      lp =>
    //        val cardinality: Cardinality = context.cardinality.apply(lp.solved, context.input, context.semanticTable)
    //        cardinality < Cardinality(2)
    //    }
    //
    //
    //    val plans: Map[Set[IdName], Seq[LogicalPlan]] = logicalPlans.groupBy(plan => original -- plan.solved.lastQueryGraph.allCoveredIds)
    //
    //    val combine1: Seq[Seq[LogicalPlan]] = combine(plans.values)
  }

  private def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x; b <- y) yield a :+ b
    }


}
