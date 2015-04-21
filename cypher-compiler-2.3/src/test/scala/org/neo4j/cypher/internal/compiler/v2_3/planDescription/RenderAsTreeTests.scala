/*
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
package org.neo4j.cypher.internal.compiler.v2_3.planDescription

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{SingleRowPipe, Pipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite

class RenderAsTreeTests extends CypherFunSuite {

  test("single node is represented nicely") {
    renderAsTree(PlanDescriptionImpl(new Id, "NAME", NoChildren, Seq.empty, Set())) should equal(
      "NAME")
  }

  test("node feeding from other node") {

    val leaf = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq.empty, Set())
    val plan = leaf.andThen(new Id, "ROOT", Set())

    renderAsTree(plan) should equal(
      """ROOT
        |  |
        |  +LEAF""".stripMargin)
  }

  test("node feeding from two nodes") {

    val leaf1 = PlanDescriptionImpl(new Id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF2", NoChildren, Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(leaf1, leaf2), Seq.empty, Set())

    renderAsTree(plan) should equal(
      """ROOT
        |  |
        |  +LEAF1
        |  |
        |  +LEAF2""".stripMargin)
  }

  test("node feeding of node that is feeding of node") {

    val leaf = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq.empty, Set())
    val intermediate = PlanDescriptionImpl(new Id, "INTERMEDIATE", SingleChild(leaf), Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", SingleChild(intermediate), Seq.empty, Set())
    val tree = renderAsTree(plan)

    tree should equal(
      """ROOT
        |  |
        |  +INTERMEDIATE
        |    |
        |    +LEAF""".stripMargin)
  }

  test("root with two auto named nodes") {

    val leaf1 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("a"))
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("b"))
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val tree = renderAsTree(plan)

    tree should equal(
      """ROOT
        |  |
        |  +LEAF(0)
        |  |
        |  +LEAF(1)""".stripMargin)
  }

  test("root with two leafs, one of which is deep") {

    val leaf1 = PlanDescriptionImpl(new Id, "LEAF1", NoChildren, Seq.empty, Set())
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF2", NoChildren, Seq.empty, Set())
    val leaf3 = PlanDescriptionImpl(new Id, "LEAF3", NoChildren, Seq.empty, Set())
    val intermediate = PlanDescriptionImpl(new Id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(intermediate, leaf3), Seq.empty, Set())
    renderAsTree(plan) should equal(
"""ROOT
  |
  +INTERMEDIATE
  |  |
  |  +LEAF1
  |  |
  |  +LEAF2
  |
  +LEAF3""")
  }

  test("root with two intermediate nodes coming from four leaf nodes") {

    val leaf1 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("a"))
    val leaf2 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("b"))
    val leaf3 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("c"))
    val leaf4 = PlanDescriptionImpl(new Id, "LEAF", NoChildren, Seq(), Set("d"))
    val intermediate1 = PlanDescriptionImpl(new Id, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty, Set())
    val intermediate2 = PlanDescriptionImpl(new Id, "INTERMEDIATE", TwoChildren(leaf3, leaf4), Seq.empty, Set())
    val plan = PlanDescriptionImpl(new Id, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty, Set())
    renderAsTree(plan) should equal(
      """ROOT
        |  |
        |  +INTERMEDIATE(0)
        |  |  |
        |  |  +LEAF(0)
        |  |  |
        |  |  +LEAF(1)
        |  |
        |  +INTERMEDIATE(1)
        |     |
        |     +LEAF(2)
        |     |
        |     +LEAF(3)""".stripMargin)
  }
}
