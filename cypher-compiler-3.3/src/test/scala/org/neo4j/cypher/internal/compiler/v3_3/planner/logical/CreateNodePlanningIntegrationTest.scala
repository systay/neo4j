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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_3.logical.plans._

class CreateNodePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan single create") {
    planFor("CREATE (a)")._2 should equal(
      EmptyResult(
        CreateNode(SingleRow()(solved), "a", Seq.empty, None)(solved))(solved)
    )
  }

  test("should plan for multiple creates") {
    planFor("CREATE (a), (b), (c)")._2 should equal(
      EmptyResult(
        CreateNode(
          CreateNode(
            CreateNode(SingleRow()(solved), "a", Seq.empty, None)(solved),
            "b", Seq.empty, None)(solved),
          "c", Seq.empty, None)(solved))
        (solved)
    )
  }

  test("should plan for multiple creates via multiple statements") {
    planFor("CREATE (a) CREATE (b) CREATE (c)")._2 should equal(
      EmptyResult(
        CreateNode(
          CreateNode(
            CreateNode(SingleRow()(solved), "a", Seq.empty, None)(solved),
            "b", Seq.empty, None)(solved),
          "c", Seq.empty, None)(solved))
        (solved)
    )
  }

  test("should plan single create with return") {
    planFor("CREATE (a) return a")._2 should equal(
        CreateNode(SingleRow()(solved), "a", Seq.empty, None)(solved)
    )
  }

  test("should plan create with labels") {
    planFor("CREATE (a:A:B)")._2 should equal(
      EmptyResult(
        CreateNode(SingleRow()(solved), "a", Seq(lblName("A"), lblName("B")), None)(solved))(solved)
    )
  }

  test("should plan create with properties") {

    planFor("CREATE (a {prop: 42})")._2 should equal(
      EmptyResult(
        CreateNode(SingleRow()(solved), "a", Seq.empty,
          Some(
            MapExpression(Seq((PropertyKeyName("prop")(pos), SignedDecimalIntegerLiteral("42")(pos))))(pos)
          )
        )(solved)
      )(solved)
    )
  }

  test("should plan match and create") {
    planFor("MATCH (a) CREATE (b)")._2 should equal(
      EmptyResult(
          CreateNode(AllNodesScan("a", Set.empty)(solved), "b", Seq.empty, None)(solved)
      )(solved)
    )
  }

  test("should plan create in tail") {
    planFor("MATCH (a) CREATE (b) WITH * MATCH(c) CREATE (d)")._2 should equal(
      EmptyResult(
        CreateNode(
          Eager(
            Apply(
              Eager(
                CreateNode(
                  AllNodesScan("a", Set.empty)(solved),
                  "b", Seq.empty, None)(solved)
              )(solved),
              AllNodesScan("c", Set("a", "b"))(solved)
            )(solved)
          )(solved),
          "d", Seq.empty, None)(solved))(solved)
    )
  }
}
