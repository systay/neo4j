/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, IdName, PlannerQuery}
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen

/**
  * AntiConditionalApply works like ConditionalApply, but with reversed condition.
  *
  * for ( leftRow <- left ) {
  *   if ( !condition( leftRow ) ) {
  *     produce leftRow
  *   } else {
  *     right.setArgument( leftRow )
  *     for ( rightRow <- right ) {
  *       produce rightRow
  *     }
  *   }
  * }
  */
case class AntiConditionalApply(left: LogicalPlan, right: LogicalPlan, items: Seq[IdName])(val solved: PlannerQuery with CardinalityEstimation)(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with LazyLogicalPlan {

  override val lhs = Some(left)
  override val rhs = Some(right)

  override def availableSymbols: Set[IdName] = left.availableSymbols ++ right.availableSymbols ++ items
}
