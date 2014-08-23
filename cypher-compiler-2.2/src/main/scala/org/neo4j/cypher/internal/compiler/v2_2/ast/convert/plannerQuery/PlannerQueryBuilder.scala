/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_2.ast.PatternExpression
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryHorizon, QueryGraph, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.helpers.CollectionSupport


case class PlannerQueryBuilder(private val q: PlannerQuery, patternExprTable: Map[PatternExpression, QueryGraph])
  extends CollectionSupport {
  def addPatternExpressions(expressions: PatternExpression*): PlannerQueryBuilder =
    copy(patternExprTable = patternExprTable ++ expressions.map(x => x -> x.asQueryGraph))

  def addPatternExpressions(expressions: Set[PatternExpression]): PlannerQueryBuilder =
    addPatternExpressions(expressions.toSeq:_*)

  def updateGraph(f: QueryGraph => QueryGraph): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.updateGraph(f)))

  def withHorizon(horizon: QueryHorizon): PlannerQueryBuilder =
    copy(q = q.updateTailOrSelf(_.withHorizon(horizon)))

  def withTail(newTail: PlannerQuery): PlannerQueryBuilder = {
    copy(q = q.updateTailOrSelf(_.withTail(newTail)))
  }

  def currentlyAvailableIdentifiers: Set[IdName] =
    currentPlannerQuery.graph.coveredIds

  def currentPlannerQuery: PlannerQuery = {
    var current = q
    while(current.tail.nonEmpty) {
      current = current.tail.get
    }
    current
  }

  def build(): PlannerQuery = {

    def fixArgumentIdsOnOptionalMatch(plannerQuery: PlannerQuery): PlannerQuery = {
      val optionalMatches = plannerQuery.graph.optionalMatches
      val (_, newOptionalMatches) = optionalMatches.foldMap(plannerQuery.graph.coveredIds) { case (args, qg) =>
        (args ++ qg.allCoveredIds, qg.withArgumentIds(args intersect qg.allCoveredIds))
      }
      plannerQuery
        .updateGraph(_.withOptionalMatches(newOptionalMatches))
        .updateTail(fixArgumentIdsOnOptionalMatch)
    }

    fixArgumentIdsOnOptionalMatch(q)
  }
}

object PlannerQueryBuilder {
  val empty = new PlannerQueryBuilder(PlannerQuery.empty, Map.empty)
}
