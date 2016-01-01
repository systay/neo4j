/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import java.lang.reflect.Method

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningFunction2, QueryPlannerConfiguration}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.frontend.v2_3.Foldable._
import org.neo4j.cypher.internal.frontend.v2_3.Rewritable._
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Expression, Identifier}
import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, Rewritable}

/*
A LogicalPlan is an algebraic query, which is represented by a query tree whose leaves are database relations and
non-leaf nodes are algebraic operators like selections, projections, and joins. An intermediate node indicates the
application of the corresponding operator on the relations generated by its children, the result of which is then sent
further up. Thus, the edges of a tree represent data flow from bottom to top, i.e., from the leaves, which correspond
to data in the database, to the root, which is the final operator producing the query answer. */
abstract class LogicalPlan
  extends Product
  with Strictness
  with Rewritable
  with PageDocFormatting {

  self =>

//  with ToPrettyString[LogicalPlan] {

//  def toDefaultPrettyString(formatter: DocFormatter) =
//    toPrettyString(formatter)(InternalDocHandler.docGen)

  def lhs: Option[LogicalPlan]
  def rhs: Option[LogicalPlan]
  def solved: PlannerQuery with CardinalityEstimation
  def availableSymbols: Set[IdName]

  def leafs: Seq[LogicalPlan] = this.treeFold(Seq.empty[LogicalPlan]) {
    case plan: LogicalPlan
      if plan.lhs.isEmpty && plan.rhs.isEmpty => (acc, r) => r(acc :+ plan)
  }

  def updateSolved(newSolved: PlannerQuery with CardinalityEstimation): LogicalPlan = {
    val arguments = this.children.toList :+ newSolved
    try {
      copyConstructor.invoke(this, arguments: _*).asInstanceOf[this.type]
    } catch {
      case e: IllegalArgumentException if e.getMessage.startsWith("wrong number of arguments") =>
        throw new InternalException("Logical plans need to be case classes, and have the PlannerQuery in a separate constructor")
    }
  }

  def copyPlan(): LogicalPlan = {
    try {
      val arguments = this.children.toList :+ solved
      copyConstructor.invoke(this, arguments: _*).asInstanceOf[this.type]
    } catch {
      case e: IllegalArgumentException if e.getMessage.startsWith("wrong number of arguments") =>
        throw new InternalException("Logical plans need to be case classes, and have the PlannerQuery in a separate constructor", e)
    }
  }

  lazy val copyConstructor: Method = this.getClass.getMethods.find(_.getName == "copy").get

  def updateSolved(f: PlannerQuery with CardinalityEstimation => PlannerQuery with CardinalityEstimation): LogicalPlan =
    updateSolved(f(solved))

  def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.children)
      this
    else {
      val constructor = this.copyConstructor
      val params = constructor.getParameterTypes
      val args = children.toVector
      if ((params.length == args.length + 1) && params.last.isAssignableFrom(classOf[PlannerQuery]))
        constructor.invoke(this, args :+ this.solved: _*).asInstanceOf[this.type]
      else
        constructor.invoke(this, args: _*).asInstanceOf[this.type]
    }

  def mapExpressions(f: (Set[IdName], Expression) => Expression): LogicalPlan

  def debugId: String = f"0x${hashCode()}%08x"
}

trait LogicalPlanWithoutExpressions {
  self: LogicalPlan =>

  override def mapExpressions(f: (Set[IdName], Expression) => Expression): LogicalPlan = self
}

abstract class LogicalLeafPlan extends LogicalPlan with LazyLogicalPlan {
  final val lhs = None
  final val rhs = None
  def argumentIds: Set[IdName]
}

object LogicalLeafPlan {
  type Finder = LogicalPlanningFunction2[QueryPlannerConfiguration, QueryGraph, Set[LogicalPlan]]
}

final case class IdName(name: String) extends PageDocFormatting // with ToPrettyString[IdName] {
//  def toDefaultPrettyString(formatter: DocFormatter) =
//    toPrettyString(formatter)(InternalDocHandler.docGen)

object IdName {
  implicit val byName = Ordering[String].on[IdName](_.name)

  def fromIdentifier(identifier: Identifier) = IdName(identifier.name)
}
