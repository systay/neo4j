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
package org.neo4j.cypher.internal.compiler.v3_2.planner

import org.neo4j.cypher.internal.ir.v3_2.helpers.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.ir.v3_2.{IdName, Predicate, Selections}



object unsolvedPreds extends ((Selections, LogicalPlan) => Seq[Expression]) {
  def apply(s: Selections, l: LogicalPlan): Seq[Expression] =
    s.scalarPredicatesGiven(l.availableSymbols)
    .filterNot(predicate => l.solved.exists(_.queryGraph.selections.contains(predicate)))
}


