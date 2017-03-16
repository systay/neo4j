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
package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.{AllNodesScan, Expand, NodeByLabelScan, NodeHashJoin}
import org.neo4j.cypher.internal.compiler.v3_2.bork.{Long, PipeLine, RegisterAllocator}
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.ast.LabelName
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2.{Cardinality, CardinalityEstimation, IdName, PlannerQuery}

class RegisterAllocatorTest extends CypherFunSuite {
  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))

  test("single all nodes scan") {
    val plan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val calculate = RegisterAllocator.calculate(plan)

    val expectedPipeLine = new PipeLine(Map(IdName("x") -> Long(0)), 1, 0)

    calculate should equal(Map(
      (plan, PipeLine.In) -> expectedPipeLine,
      (plan, PipeLine.Out) -> expectedPipeLine
    ))
  }

  test("all nodes scan followed by expand") {
    // (x)-[r]->(y)
    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val expand = Expand(allNodesScan, IdName("x"), SemanticDirection.OUTGOING, Seq.empty, IdName("y"), IdName("r"))(solved)

    val calculate = RegisterAllocator.calculate(expand)

    val expectedPipeLine = new PipeLine(
      Map(
        IdName("x") -> Long(0),
        IdName("r") -> Long(1),
        IdName("y") -> Long(2)),
      numberOfLongs = 3, numberOfObjs = 0)

    calculate should equal(Map(
      (allNodesScan, PipeLine.In) -> expectedPipeLine,
      (allNodesScan, PipeLine.Out) -> expectedPipeLine,
      (expand, PipeLine.In) -> expectedPipeLine,
      (expand, PipeLine.Out) -> expectedPipeLine
    ))
  }

  test("two labels scans joined by a hashjoin") {
    // (x)-[r]->(y)
    val labelA = NodeByLabelScan(IdName("x"), LabelName("A")(null), Set.empty)(solved)
    val labelB = NodeByLabelScan(IdName("x"), LabelName("B")(null), Set.empty)(solved)
    val join = NodeHashJoin(Set(IdName("x")), labelA, labelB)(solved)

    val calculate = RegisterAllocator.calculate(join)

    val pipeLines = new PipeLine(
      Map(
        IdName("x") -> Long(0)),
      numberOfLongs = 1, numberOfObjs = 0)

    calculate should equal(Map(
      (labelA, PipeLine.In) -> pipeLines,
      (labelA, PipeLine.Out) -> pipeLines,
      (labelB, PipeLine.In) -> pipeLines,
      (labelB, PipeLine.Out) -> pipeLines,
      (join, PipeLine.In) -> pipeLines,
      (join, PipeLine.Out) -> pipeLines
    ))
  }
}
