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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.PreparedQuery
import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class LegacyVsNewPipeBuilder(oldBuilder: PipeBuilder,
                             newBuilder: PipeBuilder,
                             monitor: NewLogicalPlanSuccessRateMonitor) extends PipeBuilder {
  def producePlan(inputQuery: PreparedQuery, planContext: PlanContext): PipeInfo = {
    val queryText = inputQuery.queryText
    val statement = inputQuery.statement
    try {
      monitor.newQuerySeen(queryText, statement)

      // Temporary measure, to save time compiling update queries
      if (containsUpdateClause(statement)) {
        throw new CantHandleQueryException("Ronja does not handle update queries yet.")
      }

      if (containsPlainVarLengthPattern(statement)) {
        throw new CantHandleQueryException("Ronja does not handle var length queries yet.")
      }

      newBuilder.producePlan(inputQuery, planContext)
    } catch {
      case e: CantHandleQueryException =>
        monitor.unableToHandleQuery(queryText, statement, e)
        oldBuilder.producePlan(inputQuery, planContext)
    }
  }

  private def containsPlainVarLengthPattern(node: ASTNode): Boolean = node.treeFold(false) {
    // only traverse expressions in node patterns in shortest path
    case sp: ShortestPaths =>
      (acc, children) =>
        acc || sp.element.exists { case node: NodePattern => containsPlainVarLengthPattern(node) }

    // check relationship patterns
    case rel: RelationshipPattern =>
      (acc, children) => if (acc || !rel.isSingleLength) true else children(false)

    // bail out early
    case _ =>
      (acc, children) => if (acc) true else children(false)
  }

  private def containsUpdateClause(s: Statement) = s.exists {
    case _: UpdateClause => true
  }
}
