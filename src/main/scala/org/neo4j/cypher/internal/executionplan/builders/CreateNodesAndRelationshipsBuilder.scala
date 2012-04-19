/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.mutation.{CreateRelationshipAction, CreateNodeAction}
import org.neo4j.cypher.internal.commands.{StartItem, CreateRelationshipStartItem, CreateNodeStartItem}
import org.neo4j.cypher.internal.pipes.{Pipe, ExecuteUpdateCommandsPipe, TransactionStartPipe}


class CreateNodesAndRelationshipsBuilder(db: GraphDatabaseService) extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val mutatingQueryTokens = q.start.filter(applicableTo(plan.pipe))
    val mutatingStartItems = mutatingQueryTokens.map(_.token)

    val commands = mutatingStartItems.map {
      case CreateNodeStartItem(id, properties) => CreateNodeAction(id, properties, db)
      case CreateRelationshipStartItem(id, from, to, typ, properties) => CreateRelationshipAction(id, from, to, typ, properties)
    }

    val p = if (plan.containsTransaction) {
      plan.pipe
    } else {
      new TransactionStartPipe(plan.pipe, db)
    }

    val resultPipe = new ExecuteUpdateCommandsPipe(p, db, commands)
    val resultQuery = q.start.filterNot(mutatingQueryTokens.contains) ++ mutatingQueryTokens.map(_.solve)

    plan.copy(query = q.copy(start = resultQuery), pipe = resultPipe, containsTransaction = true)
  }

  def applicableTo(pipe:Pipe)(start:QueryToken[StartItem]) = start match {
    case Unsolved(x: CreateNodeStartItem) => pipe.symbols.satisfies(x.dependencies.toSeq)
    case Unsolved(x: CreateRelationshipStartItem) => pipe.symbols.satisfies(x.dependencies.toSeq)
    case _ => false
  }

  override def missingDependencies(plan: ExecutionPlanInProgress) = plan.query.start.flatMap {
    case Unsolved(x: CreateNodeStartItem) => plan.pipe.symbols.missingDependencies(x.dependencies.toSeq)
    case Unsolved(x: CreateRelationshipStartItem) => plan.pipe.symbols.missingDependencies(x.dependencies.toSeq)
    case _ => Seq()
  }.map(_.name)

  def canWorkWith(plan: ExecutionPlanInProgress) = plan.query.start.exists( applicableTo(plan.pipe))

  def priority = PlanBuilder.Mutation
}