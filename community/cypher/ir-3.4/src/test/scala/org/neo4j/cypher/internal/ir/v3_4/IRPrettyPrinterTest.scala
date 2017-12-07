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
package org.neo4j.cypher.internal.ir.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.ast.AscSortItem
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_4.expressions._

class IRPrettyPrinterTest extends CypherFunSuite {
  private val pos = InputPosition(0, 0, 0)
  private val aId = IdName("a")
  private val bId = IdName("b")
  private val cId = IdName("c")
  private val xId = IdName("x")
  private val r = IdName("r")
  private val typeT = RelTypeName("T")(null)
  private val prop = Property(Variable("a")(null), PropertyKeyName("prop")(null))(null)
  private val propBar = Property(Variable("a")(null), PropertyKeyName("bar")(null))(null)
  private val r1 = PatternRelationship(r, (aId, bId), OUTGOING, Seq.empty, SimplePatternLength)

  test("minimal qg with only pattern nodes") {
    val qg = QueryGraph.empty.withPatternNodes(Set(aId, bId, cId))

    prettify(qg) should equal("QueryGraph { Nodes: ['a', 'b', 'c'] }")
  }

  test("minimal qg with only arguments") {
    val qg = QueryGraph.empty.withArgumentIds(Set(aId, bId, cId))

    prettify(qg) should equal("QueryGraph { Arguments: ['a', 'b', 'c'] }")
  }

  test("pattern nodes and relationships") {
    val qg = QueryGraph.empty.
      withPatternNodes(Set(aId, bId)).
      withPatternRelationships(Set(r1))

    prettify(qg) should equal("QueryGraph { Nodes: ['a', 'b'] Edges: [('a')-['r']->('b')] }")
  }

  test("pattern nodes and variable length relationships") {
    val qg = QueryGraph.empty.
      withPatternNodes(Set(aId, bId)).
      withPatternRelationships(Set(PatternRelationship(r, (aId, bId), OUTGOING, Seq(typeT), VarPatternLength(1, Some(8)))))

    prettify(qg) should equal("QueryGraph { Nodes: ['a', 'b'] Edges: [('a')-['r':T*1..8]->('b')] }")
  }

  test("predicates") {
    val lit1 = SignedDecimalIntegerLiteral("1")(null)
    val qg = QueryGraph.empty.
      withPatternNodes(Set(aId, bId)).
      addPredicates(Equals(prop, lit1)(null))

    prettify(qg) should equal("QueryGraph { Nodes: ['a', 'b'] Predicates: [a.prop = 1] }")
  }

  test("optional relationship") {
    val optionalQg = QueryGraph.empty.
      withPatternNodes(Set(aId, bId)).
      withArgumentIds(Set(aId)).
      withPatternRelationships(Set(r1))

    val qg = QueryGraph.empty.
      withPatternNodes(Set(aId)).
      withAddedOptionalMatch(optionalQg)

    prettify(qg) should equal(
      """QueryGraph {
        |    Nodes: ['a']
        |    Optional matches: [
        |        QueryGraph { Arguments: ['a'] Nodes: ['a', 'b'] Edges: [('a')-['r']->('b')] }]
        |}""".stripMargin)
  }

  test("simple qg with projection") {
    // MATCH (a) RETURN a.foo AS x, a.bar as y
    val graph = QueryGraph.empty.withPatternNodes(Set(IdName("a")))
    val horizon = RegularQueryProjection(Map("x" -> prop, "y" -> propBar))
    val ir = RegularPlannerQuery(graph, horizon)
    prettify(ir) should equal(
      """PlannerQuery[
        |    - QueryGraph { Nodes: ['a'] }
        |    - Projection { a.prop AS x, a.bar AS y }
        |]""".stripMargin
    )
  }

  test("projection with shuffle and aggregations") {
    // MATCH (a) WITH a, count(*) as x ORDER BY a.prop LIMIT 10 SKIP 20 MATCH (a)-[r1]->(b) RETURN DISTINCT a, b
    val graph1 = QueryGraph.empty.withPatternNodes(Set(IdName("a")))
    val graph2 = QueryGraph.empty.
      withArgumentIds(Set(aId, xId)).
      withPatternNodes(Set(IdName("a"), IdName("b")))
    val horizon1 = AggregatingQueryProjection(groupingExpressions = Map("a" -> Variable("a")(pos)), aggregationExpressions = Map("x" -> CountStar()(pos)))
    val shuffle = QueryShuffle.
      empty.
      withLimitExpression(SignedDecimalIntegerLiteral("10")(pos)).
      withSkipExpression(SignedDecimalIntegerLiteral("20")(pos)).
      withSortItems(Seq(AscSortItem(prop)(pos)))
    val horizon2 = RegularQueryProjection(Map("x" -> prop, "y" -> propBar), shuffle)
    val ir = RegularPlannerQuery(graph1, horizon1, tail = Some(RegularPlannerQuery(graph2, horizon2)))

    prettify(ir) should equal(
      """PlannerQuery[
        |    - QueryGraph { Nodes: ['a'] }
        |    - Aggregate { count(*) AS x } Group By { a AS a }
        |    - QueryGraph { Arguments: ['a', 'x'] Nodes: ['a', 'b'] }
        |    - Projection { a.prop AS x, a.bar AS y }
        |    - QueryShuffle: LIMIT 10 SKIP 20 ORDER BY [a.prop ASC]
        |]""".stripMargin
    )
  }

  private def prettify(qg: QueryGraph): String = {
    val doc = IRPrettyPrinter.show(qg)
    IRPrettyPrinter.pretty(doc, 120).layout
  }
  private def prettify(pq: PlannerQuery): String = {
    val doc = IRPrettyPrinter.showAll(pq)
    IRPrettyPrinter.pretty(doc, 120).layout
  }
}
