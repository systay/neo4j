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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Test
import org.scalatest.Assertions
import org.junit.Assert._
import scala.collection.immutable.SortedSet

class CollectionIndexTest extends Assertions {
  val dummyCollection = DummyExpression(
    TypeSet(CTCollection(CTNode), CTNode, CTCollection(CTString)),
    DummyToken(2,3))

  @Test
  def shouldReturnCollectionInnerTypesOfExpression() {
    val index = CollectionIndex(dummyCollection,
      SignedIntegerLiteral("1", DummyToken(5,6)),
      DummyToken(4, 8))

    val result = index.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(), result.errors)
    assertEquals(Set(CTNode, CTString), index.types(result.state))
  }

  @Test
  def shouldRaiseErrorIfIndexingByFraction() {
    val index = CollectionIndex(dummyCollection,
      DoubleLiteral("1.3", DummyToken(5,6)),
      DummyToken(4, 8))

    val result = index.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assertEquals(Seq(SemanticError("Type mismatch: expected Integer or Long but was Double", index.idx.token, SortedSet(index.idx.token))), result.errors)
  }
}
