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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan

import scala.collection.mutable

// TODO: Make immutable
class ExhaustivePlanTable extends (Set[Solvable] => Option[LogicalPlan]) {
  private var table = new mutable.HashMap[Set[Solvable], LogicalPlan]()

  def head = table.head._2

  def apply(solved: Set[Solvable]): Option[LogicalPlan] = table.get(solved)

  def put(solved: Set[Solvable], plan: LogicalPlan): Unit = {
    table.put(solved, plan)
  }

  def remove(solved: Set[Solvable]): Unit = {
    table.remove(solved)
  }
}
