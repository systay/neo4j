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
package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan

object Pipeliner {


  def calculate(root: LogicalPlan): Pipeline = {

    var current: Option[LogicalPlan] = Some(root)
    val builder = new PipeLineBuilder()
    while(current.nonEmpty) {
      root match {
        case plans.AllNodesScan(x, args) =>
          val offset = builder.addLong(x)
          builder.addOperator(ir.AllNodesScan(offset))
      }
      current = current.get.rhs
    }

    builder.build()

//    def populate(plan: LogicalPlan) = {
//      var current = plan
//      while (!current.isLeaf) {
//        planStack.push(current)
//        current = current.lhs.getOrElse(
//          throw new InternalException("This must not be! Found plan that clams to be leaf but has no LHS"))
//      }
//      comingFrom = current
//      planStack.push(current)
//    }

//    populate(root)
//
//    while (planStack.nonEmpty) {
//      val current = planStack.pop()
//      (current, current.lhs, current.rhs) match {
//        case (_: plans.Expand, _, None) => ???
//        case (_: plans.VarExpand, _, None) => ???
//        case (_: plans.PruningVarExpand, _, None) => ???
//
//        case (_, _, None) =>
//      }
//
//      comingFrom = current
//    }

  }
}
