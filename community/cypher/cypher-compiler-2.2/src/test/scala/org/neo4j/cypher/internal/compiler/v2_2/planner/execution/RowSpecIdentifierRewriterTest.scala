/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Identifier, Literal, Expression}

class RowSpecIdentifierRewriterTest extends CypherFunSuite {
  test("does not rewrite expression without identifier") {
    val e: Expression = Literal("1")
    val rewriter = RowSpecIdentifierRewriter(RowSpec(nodes = Seq("a")))
    val result = e.rewrite(rewriter)

    result should equal(e)
  }

  test("expression returning node gets rewritten as it should") {
    val e: Expression = Identifier("a")
    val rewriter = RowSpecIdentifierRewriter(RowSpec(nodes = Seq("a")))
    val result = e.rewrite(rewriter)

    result should equal(NodeIdentifier(0))
  }

  test("expression returning relationship gets rewritten as it should") {
    val e: Expression = Identifier("a")
    val rewriter = RowSpecIdentifierRewriter(RowSpec(relationships = Seq("a")))
    val result = e.rewrite(rewriter)

    result should equal(RelationshipIdentifier(0))
  }

  test("expression returning anyref gets rewritten as it should") {
    val e: Expression = Identifier("a")
    val rewriter = RowSpecIdentifierRewriter(RowSpec(other = Seq("a")))
    val result = e.rewrite(rewriter)

    result should equal(AnyRefIdentifier(0))
  }
}
