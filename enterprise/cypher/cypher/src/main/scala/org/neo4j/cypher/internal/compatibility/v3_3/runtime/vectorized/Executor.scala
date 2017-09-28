/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{LogicalPlan, LogicalPlanId, TreeBuilder}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.Result
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

class Executor(lane: Pipeline,
               pipelineInformation: Map[LogicalPlanId, PipelineInformation],
               queryContext: QueryContext,
               params: MapValue) {
  def accept[E <: Exception](visitor: Result.ResultVisitor[E]): Unit = {
    val queryState = new QueryState(params = params)
    lane.init(queryState, queryContext)

    val morsel = Morsel.create(lane.slotInformation, 654)
    val resultRow = new MorselResultRow(null, 0, lane.slotInformation, queryContext)

    while(morsel.moreDataToCome) {
      val data = lane.operate(morsel, queryContext, queryState)

      resultRow.morsel = data

      (0 until data.rows) foreach { position =>
        resultRow.currentPos = position
        visitor.visit(resultRow)
      }
    }
  }

  /*
  Returns a map that can be used to look up parents of plans
   */
  private def parents(logicalPlan: LogicalPlan): Map[LogicalPlanId, LogicalPlanId] = {
    val mapBuilder = new mutable.HashMap[LogicalPlanId, LogicalPlanId]()

    def add(parent: LogicalPlan): Unit = {
      parent.lhs.foreach(x => mapBuilder.put(x.assignedId, parent.assignedId))
      parent.rhs.foreach(x => mapBuilder.put(x.assignedId, parent.assignedId))
    }

    val builder = new TreeBuilder[Unit] {
      override protected def build(plan: LogicalPlan): Unit = add(plan)

      override protected def build(plan: LogicalPlan, source: Unit): Unit = add(plan)

      override protected def build(plan: LogicalPlan, lhs: Unit, rhs: Unit): Unit = add(plan)
    }

    builder.create(logicalPlan)

    mapBuilder.toMap

  }

}

case class Pipeline(operators: Seq[Operator], slotInformation: PipelineInformation, dependencies: Set[Pipeline]) {
  def init(queryState: QueryState, queryContext: QueryContext): Unit = {
    operators.foreach(_.init(queryState, queryContext))
  }

  def operate(input: Morsel, context: QueryContext, state: QueryState): Morsel = {
    operators.foldLeft(input) {
      case (morsel, operator) =>
        operator.operate(morsel, context, state)
    }
  }

}