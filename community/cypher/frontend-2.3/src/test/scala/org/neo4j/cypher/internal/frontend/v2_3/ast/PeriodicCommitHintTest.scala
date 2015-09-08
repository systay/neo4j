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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticState}

class PeriodicCommitHintTest extends CypherFunSuite  {
  test("negative values should fail") {
    // Given
    val input = "-1"
    val pos = DummyPosition(54)
    val value: SignedIntegerLiteral = SignedDecimalIntegerLiteral(input)
    value.setPos(pos)
    val hint = PeriodicCommitHint(Some(value))

    // When
    val result = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === s"Commit size error - expected positive value larger than zero, got ${input}")
    assert(result.errors.head.position === pos)
  }

  test("no periodic commit size is ok") {
    // Given
    val hint = PeriodicCommitHint(None)

    // When
    val result = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 0)
  }

  test("positive values are OK") {
    // Given
    val input = "1"
    val value: SignedIntegerLiteral = SignedDecimalIntegerLiteral(input)
    val hint = PeriodicCommitHint(Some(value))

    // When
    val result = hint.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 0)
  }

  test("queries with periodic commit and no updates are not OK") {
    // Given USING PERIODIC COMMIT RETURN "Hello World!"

    val value: SignedIntegerLiteral = SignedDecimalIntegerLiteral("1")
    val periodicCommitPos = DummyPosition(12)
    val hint = PeriodicCommitHint(Some(value)).setPos(periodicCommitPos)
    val literal: StringLiteral = StringLiteral("Hello world!")
    val returnItem = UnaliasedReturnItem(literal, "Hello world!")
    val returnItems = ReturnItems(includeExisting = false, Seq(returnItem))
    val returns: Return = Return(false, returnItems, None, None, None)
    val queryPart = SingleQuery(Seq(returns))
    val query = Query(Some(hint), queryPart)

    // When
    val result = query.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === "Cannot use periodic commit in a non-updating query")
    assert(result.errors.head.position === periodicCommitPos)
  }

  test("queries with periodic commit and updates are OK") {

    // Given USING PERIODIC COMMIT CREATE ()

    val value: SignedIntegerLiteral = SignedDecimalIntegerLiteral("1")
    val hint = PeriodicCommitHint(Some(value))
    val nodePattern = NodePattern(None,Seq.empty,None, false)
    val pattern = Pattern(Seq(EveryPath(nodePattern)))
    val create = Create(pattern)
    val queryPart = SingleQuery(Seq(create))
    val query = Query(Some(hint), queryPart)

    // When
    val result = query.semanticCheck(SemanticState.clean)

    // Then
    assert(result.errors.size === 0)
  }
}
