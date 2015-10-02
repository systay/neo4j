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
package org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_0.Rewriter
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class DesugarTreeProjectionTest extends CypherFunSuite with RewriteTest {

  test("rewrites tree projections") {
    assertRewrite(
      "match n return n{k:42} as x",
      "match n return {k: 42} as x")

    assertRewrite(
      "match n return n{.id} as x",
      "match n return {id: n.id} as x")

    assertRewrite(
      "with '42' as existing match n return n{existing} as x",
      "with '42' as existing match n return {existing: existing} as x")

    assertRewrite(
      "match n return n{.foo,.bar,.baz} as x",
      "match n return {foo: n.foo, bar: n.bar, baz: n.baz} as x")

    assertRewrite(
      """match n, m
        |return n{
        | .foo,
        | .bar,
        | inner: m {
        |   .baz,
        |   .apa
        | }
        |} as x""".stripMargin,

      """match n, m
        |return {
        | foo: n.foo,
        | bar: n.bar,
        | inner: {
        |   baz: m.baz,
        |   apa: m.apa
        | }
        |} as x""".stripMargin)
  }
  override def rewriterUnderTest: Rewriter = desugarTreeProjection
}
