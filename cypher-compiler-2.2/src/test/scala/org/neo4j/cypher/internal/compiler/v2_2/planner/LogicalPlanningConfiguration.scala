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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cost, _}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

trait LogicalPlanningConfiguration {
  def computeSemanticTable: SemanticTable
  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, semanticTable: SemanticTable): Metrics.CardinalityModel
  def costModel(cardinality: CardinalityModel): PartialFunction[LogicalPlan, Cost]
  def graphStatistics: GraphStatistics
  def indexes: Set[(String, String)]
  def uniqueIndexes: Set[(String, String)]
  def labelCardinality: Map[String, Cardinality]
  def knownLabels: Set[String]
  def qg: QueryGraph

  protected def mapCardinality(pf: PartialFunction[LogicalPlan, Double]): PartialFunction[LogicalPlan, Cardinality] = pf.andThen(Cardinality.apply)
}

trait LogicalPlanningConfigurationAdHocSemanticTable {
  self: LogicalPlanningConfiguration =>

  override def computeSemanticTable: SemanticTable = {
    val table = SemanticTable()
    def addLabelIfUnknown(labelName: String) =
      if (!table.resolvedLabelIds.contains(labelName))
        table.resolvedLabelIds.put(labelName, LabelId(table.resolvedLabelIds.size))

    indexes.foreach { case (label, property) =>
      addLabelIfUnknown(label)
      table.resolvedPropertyKeyNames.put(property, PropertyKeyId(table.resolvedPropertyKeyNames.size))
    }
    uniqueIndexes.foreach { case (label, property) =>
      addLabelIfUnknown(label)
      table.resolvedPropertyKeyNames.put(property, PropertyKeyId(table.resolvedPropertyKeyNames.size))
    }
    labelCardinality.keys.foreach(addLabelIfUnknown)
    knownLabels.foreach(addLabelIfUnknown)
    table
  }
}
