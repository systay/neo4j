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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.helpers.CachedFunction
import Metrics._
import org.neo4j.cypher.internal.compiler.v2_1.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_1.planner.SemanticTable

case class CachedMetricsFactory(metricsFactory: MetricsFactory) extends MetricsFactory {
  def newSelectivityEstimator(statistics: GraphStatistics, semanticTable: SemanticTable) =
    CachedFunction.byIdentity(metricsFactory.newSelectivityEstimator(statistics, semanticTable))

  def newCardinalityEstimator(statistics: GraphStatistics, selectivity: SelectivityModel, semanticTable: SemanticTable) =
    CachedFunction.byIdentity(metricsFactory.newCardinalityEstimator(statistics, selectivity, semanticTable))

  def newCostModel(cardinality: CardinalityModel) =
    CachedFunction.byIdentity(metricsFactory.newCostModel(cardinality))
}
