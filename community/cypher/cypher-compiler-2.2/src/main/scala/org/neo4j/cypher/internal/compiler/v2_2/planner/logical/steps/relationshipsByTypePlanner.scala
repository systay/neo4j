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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.SimplePatternLength
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CandidateList, LogicalPlanningContext, LeafPlanner}
import org.neo4j.graphdb.Direction

object relationshipsByTypePlanner extends LeafPlanner {
  def apply(input: QueryGraph)(implicit context: LogicalPlanningContext): CandidateList = {
    implicit val semanticTable = context.semanticTable
    val validPatternRels = input.patternRelationships.filter {
      r => r.types.size == 1 && r.length == SimplePatternLength
    }

    val plans = validPatternRels.map{
      case r =>
        val (sNode, eNode) = if (r.dir == Direction.OUTGOING) r.nodes else r.nodes.swap
        LogicalPlanProducer.planRelationshipsByTypeScan(sNode, eNode, r, r.types.head.either, input.argumentIds)
    }

    CandidateList(plans.toSeq)
  }
}
