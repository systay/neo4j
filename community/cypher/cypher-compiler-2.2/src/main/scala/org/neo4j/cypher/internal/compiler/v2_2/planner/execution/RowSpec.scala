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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan

object RowSpec {
  def from(plan: LogicalPlan) = {
    val qg = plan.solved.lastQueryGraph
    val nodes = qg.patternNodes.map(_.name)
    val relationships = qg.patternRelationships.map(_.name.name).toSet
    val other = plan.availableSymbols.map(_.name) -- nodes -- relationships
    RowSpec(nodes.toSeq, relationships.toSeq, other.toSeq)
  }
}

case class RowSpec(nodes: Seq[String] = Seq.empty,
                   relationships: Seq[String] = Seq.empty,
                   other: Seq[String] = Seq.empty) {

  private lazy val lookup: Map[String, RowIndex] =
    nodes.zipWithIndex.map { case (k,i) => k -> NodeIndex(i) }.toMap ++
    relationships.zipWithIndex.map { case (k,i) => k -> RelIndex(i) }.toMap ++
    other.zipWithIndex.map { case (k,i) => k -> OtherIndex(i) }.toMap

  private def checkIsKnown(i: Int, name: String): Int = if (i == -1) throw new NoSuchElementException(name) else i

  def indexToNode(k: String) = checkIsKnown(nodes.indexOf(k), k)
  def indexToRel(k: String) = checkIsKnown(relationships.indexOf(k), k)
  def indexTo(k: String): Option[RowIndex] = lookup.get(k)
  def size: Int = lookup.size

  override def toString: String = s"RowSpec(n=[${nodes.mkString(",")}],r=[${relationships.mkString(",")}],o=[${other.mkString(",")}])"

  def elements: Seq[(String, RowIndex)] = lookup.toSeq
}

sealed trait RowIndex
case class NodeIndex(i: Int) extends RowIndex
case class RelIndex(i: Int) extends RowIndex
case class OtherIndex(i: Int) extends RowIndex
