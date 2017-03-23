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

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.{AllNodesScan, Selection}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2.{Cardinality, CardinalityEstimation, IdName, PlannerQuery}

class PipelinerTest extends CypherFunSuite {
  val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))

  test("single all nodes scan") {
    val plan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val calculate = Pipeliner.calculate(plan)

    val expectedPipeLine = Pipeline(1, 0, Seq(ir.AllNodesScan(0)), Seq.empty)

    calculate should equal(expectedPipeLine)
  }

  test("single all nodes scan followed by filter") {
    val scan = AllNodesScan(IdName("x"), Set.empty)(solved)
    val filter = Selection(Seq(IdName("x"), Set.empty)(solved)
    val calculate = Pipeliner.calculate(plan)

    val expectedPipeLine = Pipeline(1, 0, Seq(ir.AllNodesScan(0)), Seq.empty)

    calculate should equal(expectedPipeLine)
  }

//  test("all nodes scan followed by expand") {
//    // (x)-[r]->(y)
//    val allNodesScan = AllNodesScan(IdName("x"), Set.empty)(solved)
//    val expand = Expand(allNodesScan, IdName("x"), SemanticDirection.OUTGOING, Seq.empty, IdName("y"), IdName("r"))(solved)
//
//    val calculate = Pipeliner.calculate(expand)
//
//    val expectedPipeLine = new PipeLine(
//      Map(
//        IdName("x") -> new LongSlot(0),
//        IdName("r") -> new LongSlot(1),
//        IdName("y") -> new LongSlot(2)),
//      numberOfLongs = 3, numberOfReferences = 0)
//
//    calculate should equal(Map(
//      (allNodesScan, PipeLine.In) -> expectedPipeLine,
//      (allNodesScan, PipeLine.Out) -> expectedPipeLine,
//      (expand, PipeLine.In) -> expectedPipeLine,
//      (expand, PipeLine.Out) -> expectedPipeLine
//    ))
//  }

//  test("two labels scans joined by a hashjoin") {
//    // (x)-[r]->(y)
//    val labelA = NodeByLabelScan(IdName("x"), LabelName("A")(null), Set.empty)(solved)
//    val labelB = NodeByLabelScan(IdName("x"), LabelName("B")(null), Set.empty)(solved)
//    val join = NodeHashJoin(Set(IdName("x")), labelA, labelB)(solved)
//
//    val calculate = Pipeliner.calculate(join)
//
//    val pipeLines = new PipeLine(
//      Map(
//        IdName("x") -> new LongSlot(0)),
//      numberOfLongs = 1, numberOfReferences = 0)
//
//    calculate should equal(Map(
//      (labelA, PipeLine.In) -> pipeLines,
//      (labelA, PipeLine.Out) -> pipeLines,
//      (labelB, PipeLine.In) -> pipeLines,
//      (labelB, PipeLine.Out) -> pipeLines,
//      (join, PipeLine.In) -> pipeLines,
//      (join, PipeLine.Out) -> pipeLines
//    ))
//  }
}
