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
package org.neo4j.cypher.internal.compiler.v2_0

import org.neo4j.cypher.internal.commons.CypherTestSuite

object FoldableTest {
  trait Exp extends Foldable
  case class Val(int: Int) extends Exp
  case class Add(lhs: Exp, rhs: Exp) extends Exp
  case class Sum(args: Seq[Exp]) extends Exp
}

class FoldableTest extends CypherTestSuite {
  import FoldableTest._

  test("should fold value depth first over object tree") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.fold(50) {
      case Val(x) => acc => acc + x
    }

    assert(result === 200)
  }
}
