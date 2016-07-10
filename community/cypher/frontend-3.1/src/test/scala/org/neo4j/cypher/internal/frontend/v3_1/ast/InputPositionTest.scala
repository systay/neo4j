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
package org.neo4j.cypher.internal.frontend.v3_1.ast

import org.neo4j.cypher.internal.frontend.v3_1.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class InputPositionTest extends CypherFunSuite {
  val pos0 = DummyPosition(0)
  val pos1 = DummyPosition(1)
  val pos2 = DummyPosition(2)
  val pos3 = DummyPosition(3)

  test("when position is set, it can be retrieved") {
    val variable = Variable("a")
    variable.markThisAsRoot()
    variable.position.update(pos0)
    variable.position() should equal(pos0)
  }

  test("when child AST object is lacking position, it will be filled in by the surrounding objects") {
    val v1 = Variable("a")
    val v2 = Variable("b")
    val eq = Equals(v1, v2)
    eq.markThisAsRoot()
    eq.position.update(pos0)
    v1.position.update(pos1)

    eq.position() should equal(pos0)
    v1.position() should equal(pos1)
    v2.position() should equal(pos0)
  }

  test("an intermediate object without position should not overwrite it's children") {
    // Given an Add-object without position
    // v1 + 2 = v2
    val v1 = Variable("a")
    val v2 = Variable("b")
    val literal = SignedDecimalIntegerLiteral("2") // This object will not have a position set
    val add = Add(v1, literal)
    val eq = Equals(add, v2)

    eq.markThisAsRoot()
    eq.position.update(pos0)
    v1.position.update(pos1)
    v2.position.update(pos2)
    literal.position.update(pos3)

    // The Add-object should now have the same position as the surrounding object (Equals in this case)
    eq.position() should equal(pos0)
    v1.position() should equal(pos1)
    v2.position() should equal(pos2)
    add.position() should equal(pos0)
    literal.position() should equal(pos3)
  }
}
