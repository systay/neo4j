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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3.ast.{Equals, Identifier, _}
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite

class NormalizeEqualsArgumentOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("a.prop = b.prop rewritten to: a.prop = b.prop") {
    val lhs: Expression = Property(ident("a"), PropertyKeyName("prop")_)_
    val rhs: Expression = Property(ident("b"), PropertyKeyName("prop")_)_

    val input: Expression = Equals(lhs, rhs)_

    normalizeEqualsArgumentOrder(input) should equal(input)
  }

  test("12 = a.prop rewritten to: a.prop = 12") {
    val lhs: Expression = SignedDecimalIntegerLiteral("12")_
    val rhs: Expression = Property(ident("a"), PropertyKeyName("prop")_)_

    val input: Expression = Equals(lhs, rhs)_
    val expected: Expression = Equals(rhs, lhs)_

    normalizeEqualsArgumentOrder(input) should equal(expected)
  }

  test("id(a) = id(b) rewritten to: id(a) = id(b)") {
    val lhs: Expression = id("a")
    val rhs: Expression = id("b")

    val input: Expression = Equals(lhs, rhs)_

    normalizeEqualsArgumentOrder(input) should equal(input)
  }

  test("23 = id(a) rewritten to: id(a) = 23") {
    val lhs: Expression = SignedDecimalIntegerLiteral("12")_
    val rhs: Expression = id("a")

    val input: Expression = Equals(lhs, rhs)_
    val expected: Expression = Equals(rhs, lhs)_

    normalizeEqualsArgumentOrder(input) should equal(expected)
  }

  test("a.prop = id(b) rewritten to: id(b) = a.prop") {
    val lhs: Expression = Property(ident("a"), PropertyKeyName("prop")_)_
    val rhs: Expression = id("b")

    val input: Expression = Equals(rhs, lhs)_

    normalizeEqualsArgumentOrder(input) should equal(input)
  }

  test("id(a) = b.prop rewritten to: id(a) = b.prop") {
    val lhs: Expression = id("a")
    val rhs: Expression = Property(ident("b"), PropertyKeyName("prop")_)_

    val input: Expression = Equals(lhs, rhs)_

    normalizeEqualsArgumentOrder(input) should equal(input)
  }

  private def id(name: String): FunctionInvocation =
    FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(Identifier(name)(pos)))(pos)
}
