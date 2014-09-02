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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans

import java.lang.reflect.Method

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.Foldable._
import org.neo4j.cypher.internal.compiler.v2_2.docbuilders.internalDocBuilder
import org.neo4j.cypher.internal.compiler.v2_2.planner.PlannerQuery

/*
A LogicalPlan is an algebraic query, which is represented by a query tree whose leaves are database relations and
non-leaf nodes are algebraic operators like selections, projections, and joins. An intermediate node indicates the
application of the corresponding operator on the relations generated by its children, the result of which is then sent
further up. Thus, the edges of a tree represent data flow from bottom to top, i.e., from the leaves, which correspond
to data in the database, to the root, which is the final operator producing the query answer. */
abstract class LogicalPlan extends Product with internalDocBuilder.AsPrettyToString {
  def lhs: Option[LogicalPlan]
  def rhs: Option[LogicalPlan]
  def solved: PlannerQuery
  def availableSymbols: Set[IdName]

  def updateSolved(newSolved: PlannerQuery): LogicalPlan = {
    val arguments = this.children.toList :+ newSolved
    try {
      copyConstructor.invoke(this, arguments: _*).asInstanceOf[this.type]
    } catch {
      case e: IllegalArgumentException if e.getMessage.startsWith("wrong number of arguments") =>
        throw new InternalException("Logical plans need to be case classes, and have the PlannerQuery in a separate constructor")
    }
  }

  lazy val copyConstructor: Method = this.getClass.getMethods.find(_.getName == "copy").get

  def updateSolved(f: PlannerQuery => PlannerQuery): LogicalPlan =
    updateSolved(f(solved))
}

abstract class LogicalLeafPlan extends LogicalPlan {
  final val lhs = None
  final val rhs = None
}

final case class IdName(name: String)
