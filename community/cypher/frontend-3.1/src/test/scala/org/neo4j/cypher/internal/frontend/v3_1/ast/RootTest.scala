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

class RootTest extends CypherFunSuite {
  val pos0 = DummyPosition(0)
  val pos1 = DummyPosition(1)

  test("setting the root should cascade down to all children") {
    val v1 = Variable("a")
    val v2 = Variable("b")
    val eq = Equals(v1, v2)
    eq.markThisAsRoot()

    eq.root() should equal(eq)
    v1.root() should equal(eq)
    v2.root() should equal(eq)
  }
}
