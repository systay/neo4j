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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans

import java.lang.reflect.Method

import org.neo4j.cypher.internal.compiler.v3_0.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v3_0.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v3_0.Foldable._
import org.neo4j.cypher.internal.frontend.v3_0.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, Variable}
import org.neo4j.cypher.internal.frontend.v3_0.{InternalException, Rewritable}

/*
A LogicalPlan is an algebraic query, which is represented by a query tree whose leaves are database relations and
non-leaf nodes are algebraic operators like selections, projections, and joins. An intermediate node indicates the
application of the corresponding operator on the relations generated by its children, the result of which is then sent
further up. Thus, the edges of a tree represent data flow from bottom to top, i.e., from the leaves, which correspond
to data in the database, to the root, which is the final operator producing the query answer. */
abstract class LogicalPlan
  extends Product
  with Strictness
  with Rewritable {

  self =>

  def lhs: Option[LogicalPlan]
  def rhs: Option[LogicalPlan]
  def solved: PlannerQuery with CardinalityEstimation
  def availableSymbols: Set[IdName]

  def leaves: Seq[LogicalPlan] = this.treeFold(Seq.empty[LogicalPlan]) {
    case plan: LogicalPlan
      if plan.lhs.isEmpty && plan.rhs.isEmpty => acc => (acc :+ plan, Some(identity))
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

  def isLeaf: Boolean = lhs.isEmpty && rhs.isEmpty

  override def toString = {
    val children = lhs.toSeq ++ rhs.toSeq
    val nonChildFields = productIterator.filterNot(children.contains).mkString(", ")
    val l = lhs.map(p => indent("LHS -> " + p) + "\n").getOrElse("")
    val r = rhs.map(p => indent("RHS -> " + p) + "\n").getOrElse("")

    val childrenText = if (l+r == "") "{}" else s"""{
                                               |$l$r}""".stripMargin

    s"""$productPrefix($nonChildFields) $childrenText""".stripMargin
  }

  def satisfiesExpressionDependencies(e: Expression) = e.dependencies.map(IdName.fromVariable).forall(availableSymbols.contains)

  private def indent(in: String) = {
    in.split("\n").map("  " + _).mkString("\n")
  }

  def debugId: String = f"0x${hashCode()}%08x"
}

abstract class LogicalLeafPlan extends LogicalPlan with LazyLogicalPlan {
  final val lhs = None
  final val rhs = None
  def argumentIds: Set[IdName]
}

abstract class NodeLogicalLeafPlan extends LogicalLeafPlan {
  def idName: IdName
}

abstract class IndexLeafPlan extends NodeLogicalLeafPlan {
  def valueExpr: QueryExpression[Expression]
}

final case class IdName(name: String)

object IdName {
  implicit val byName = Ordering[String].on[IdName](_.name)

  def fromVariable(variable: Variable) = IdName(variable.name)
}
