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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_1.ast.{RelTypeName, Expression}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext

/*
A LogicalPlan is an algebraic query, which is represented by a query tree whose leaves are database relations and
non-leaf nodes are algebraic operators like selections, projections, and joins. An intermediate node indicates the
application of the corresponding operator on the relations generated by its children, the result of which is then sent
further up. Thus, the edges of a tree represent data flow from bottom to top, i.e., from the leaves, which correspond
to data in the database, to the root, which is the final operator producing the query answer. */
abstract class LogicalPlan extends Product {
  def lhs: Option[LogicalPlan]
  def rhs: Option[LogicalPlan]

  def solvedPatterns: Seq[PatternRelationship]
  def solvedPredicates: Seq[Expression]

  def coveredIds: Set[IdName]

  final def isCoveredBy(otherIds: Set[IdName]) = (coveredIds -- otherIds).isEmpty
  final def covers(other: LogicalPlan): Boolean = other.isCoveredBy(coveredIds)

  def toTreeString = toVerboseTreeString(None)

  def toVerboseTreeString(optContext: Option[LogicalPlanContext]): String = {
    val metrics = optContext match {
      case Some(context) => s"(cost ${context.cost(this)}/cardinality ${context.cardinality(this)})"
      case None => ""
    }

    productPrefix + coveredIds.map(_.name).mkString("[", ",", "]") + s"$metrics->" +
    productIterator.filterNot(_.isInstanceOf[LogicalPlan]).mkString("(", ", ", ")") +
    lhs.map { plan => "\nleft - " + plan.toTreeString }.map(indent).getOrElse("") +
    rhs.map { plan => "\nright- " + plan.toTreeString }.map(indent).getOrElse("")
  }

  private def indent(s: String): String = s.lines.map {
    case t => "       " + t
  }.mkString("\n")

  override def toString = "\n" + toTreeString
}

abstract class LogicalLeafPlan extends LogicalPlan {
  final val lhs = None
  final val rhs = None
}

final case class IdName(name: String) extends AnyVal

final case class PatternRelationship(name: IdName, nodes: (IdName, IdName), dir: Direction, types: Seq[RelTypeName], length: PatternLength = PatternLength()) {
  def directionRelativeTo(node: IdName): Direction = if (node == nodes._1) dir else dir.reverse()

  def otherSide(node: IdName) = if (node == nodes._1) nodes._2 else nodes._1
}

case class PatternLength(min: Int = 1, max: Option[Int] = Some(1))

