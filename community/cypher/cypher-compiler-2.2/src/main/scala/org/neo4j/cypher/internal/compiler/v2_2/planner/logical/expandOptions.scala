/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.{Identifier, FilterScope, AllIterablePredicate}
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_2.planner.RichQueryGraph._

case object expandOptions extends PlanProducer with CollectionSupport {

  def apply(qg: QueryGraph, cache: PlanTable): Seq[LogicalPlan] = {
    qg.combinations(qg.size - 1).flatMap {
      subQG =>
        val missingRel = (qg.patternRelationships -- subQG.patternRelationships).head
        val startsFromSubQG = subQG.coveredIds.contains(missingRel.nodes._1)
        if (startsFromSubQG) {
          val from = missingRel.nodes._1
          val to = missingRel.nodes._2
          createLogicalPlan(cache, subQG, qg, to, from, missingRel, missingRel.dir)
        } else {
          val from = missingRel.nodes._2
          val to = missingRel.nodes._1
          createLogicalPlan(cache, subQG, qg, to, from, missingRel, missingRel.dir.reverse())
        }
    }
  }

  private def createLogicalPlan(cache: PlanTable, subQG: QueryGraph, qg: QueryGraph,
                                to: IdName, from: IdName, r: PatternRelationship, dir: Direction): Option[LogicalPlan] = {
    val overlapping = subQG.coveredIds.contains(to)
    val mode = if (overlapping) ExpandInto else ExpandAll

    cache.get(subQG) map {
      innerPlan =>
        r.length match {
          case SimplePatternLength =>
            planSimpleExpand(innerPlan, from, dir, to, r, mode)

          case _:VarPatternLength =>
            val availablePredicates = qg.selections.predicatesGiven(innerPlan.availableSymbols + r.name)
            val (predicates, allPredicates) = availablePredicates.collect {
              case all@AllIterablePredicate(FilterScope(identifier, Some(innerPredicate)), relId@Identifier(r.name.name))
                if identifier == relId || !innerPredicate.dependencies(relId) =>
                (identifier, innerPredicate) -> all
            }.unzip
            planVarExpand(innerPlan, from, dir, to, r, predicates, allPredicates, mode)
        }

    }
  }
}
