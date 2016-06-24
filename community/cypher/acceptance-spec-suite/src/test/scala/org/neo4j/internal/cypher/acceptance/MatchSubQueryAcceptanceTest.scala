/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._

class MatchSubQueryAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("simplest match subquery works as expected") {
    val result = executeWithCostPlannerOnly("MATCH { RETURN 1 as x } RETURN x")
                                           //01234567890123456789012345678901

    result.toList should equal(List(Map("x" -> 1)))
  }

  test("correlated through value join in the subquery") {
    val a1 = createLabeledNode(Map("x" -> 1), "A")
    val a2 = createLabeledNode(Map("x" -> 2), "A")
    val b1 = createLabeledNode(Map("x" -> 1), "B")
    val b2 = createLabeledNode(Map("x" -> 2), "B")

    val result = executeWithCostPlannerOnly(
      """MATCH (a:A)
        |MATCH {
        |  MATCH (b:B) WHERE a.x = b.id
        |  RETURN b
        |}
        |RETURN a, b
      """.stripMargin)


    result.toSet should equal(Set(Map("a" -> a1, "b" -> b1), Map("a" -> a2, "b" -> b2)))
  }

  test("correlated through value join on the outside") {
    val a1 = createLabeledNode(Map("x" -> 1), "A")
    val a2 = createLabeledNode(Map("x" -> 2), "A")
    val b1 = createLabeledNode(Map("x" -> 1), "B")
    val b2 = createLabeledNode(Map("x" -> 2), "B")

    val result = executeWithCostPlannerOnly(
      """MATCH (a:A)
        |MATCH {
        |  MATCH (b:B)
        |  RETURN b
        |}
        |WHERE a.x = b.id
        |RETURN a, b
      """.stripMargin)


    result.toSet should equal(Set(Map("a" -> a1, "b" -> b1), Map("a" -> a2, "b" -> b2)))
  }

  test("correlated through pattern matching in the subquery") {
    val a1 = createLabeledNode("A")
    val a2 = createLabeledNode("A")
    val b1 = createLabeledNode("B")
    val b2 = createLabeledNode("B")
    relate(a1, b1)
    relate(a2, b2)

    val result = executeWithCostPlannerOnly(
      """MATCH (a:A)
        |MATCH {
        |  MATCH (a)-->(b:B)
        |  RETURN b
        |}
        |RETURN a, b
      """.stripMargin)


    result.toSet should equal(Set(Map("a" -> a1, "b" -> b1), Map("a" -> a2, "b" -> b2)))
  }

  test("correlated through pattern matching on the outside") {
    val a1 = createLabeledNode("A")
    val a2 = createLabeledNode("A")
    val b1 = createLabeledNode("B")
    val b2 = createLabeledNode("B")
    relate(a1, b1)
    relate(a2, b2)

    val result = executeWithCostPlannerOnly(
      """MATCH (a:A)
        |MATCH {
        |  MATCH (b:B)
        |  RETURN b
        |}
        |MATCH (a)-->(b)
        |RETURN a, b
      """.stripMargin)


    result.toSet should equal(Set(Map("a" -> a1, "b" -> b1), Map("a" -> a2, "b" -> b2)))
  }

  test("limit UNION") {
    (0 to 10) foreach (x => createLabeledNode(Map("value" -> x), "A"))
    (0 to 10) foreach (x => createLabeledNode(Map("value" -> x), "B"))

    val result = executeWithCostPlannerOnly(
      """MATCH {
        |  MATCH (a:A)
        |  RETURN a AS x
        |  UNION
        |  MATCH (b:B)
        |  RETURN b as x
        |}
        |RETURN x:A, x:B, x.value ORDER BY x.value LIMIT 4
      """.stripMargin)


    result.toSet should equal(List(
      Map("x:A" -> true, "x:B" -> false, "x.value" -> 0),
      Map("x:A" -> true, "x:B" -> false, "x.value" -> 1),
      Map("x:A" -> true, "x:B" -> false, "x.value" -> 2),
      Map("x:A" -> true, "x:B" -> false, "x.value" -> 3)
    ))
  }

  test("optional match subquery that returns nothing still yields a single null-row") {
    val result = executeWithCostPlannerOnly(
      """OPTIONAL MATCH {
        |  MATCH (a:DoesNotExist)
        |  RETURN a
        |}
        |RETURN a
      """.stripMargin)

    result should equal(List(Map("a" -> null)))
  }

  test("does not allow for updates in subquery") {
//    intercept[CypherException](
      executeWithCostPlannerOnly(
      """MATCH (a:A)
         MATCH {
           CREATE ()
         }
         RETURN a
      """.stripMargin)
//    )
  }
}
