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

import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_1.{DummyPosition, InputPosition}

class AtomTest extends CypherFunSuite {
  val pos0 = DummyPosition(0)
  val pos1 = DummyPosition(1)
  val pos2 = DummyPosition(2)

  test("update overwrites values as long as they have not been seen") {
    val v = Atom.atom[InputPosition]
    v.update(pos0)
    v.update(pos1)
    v.update(pos2)
    v() should equal(pos2)
  }

  test("stops updating atoms after their value has been seen") {
    val v = Atom.atom[InputPosition]
    v.update(pos0)
    v()
    v.update(pos1)
    v.update(pos2)
    v() should equal(pos0)
  }

  test("does not set values if a value already exists") {
    val v1 = Atom.atom[InputPosition]
    val v2 = Atom.atom[InputPosition]
    v1.update(pos0)
    v2.update(pos1)
    v1.copyToIfNotSet(v2)

    v2() should equal(pos1)
  }
}
