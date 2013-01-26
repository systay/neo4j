/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands._
import expressions.{Literal, HeadFunction, Identifier}
import org.neo4j.cypher.internal.mutation.{RelationshipEndpoint, CreateRelationship, CreateNode}

class CreateNodesAndRelationshipsBuilderTest extends BuilderTest {

  val builder = new CreateNodesAndRelationshipsBuilder(null)

  @Test
  def does_not_offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeById("s", 0))))

    assertFalse("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def does_offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(CreateNodeStartItem(CreateNode("r", Map(), Literal(Seq.empty))))))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def full_path() {
    val q = PartiallySolvedQuery().copy(start = Seq(
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r1",
        RelationshipEndpoint(Identifier("a"), Map(), Literal(Seq.empty), true),
        RelationshipEndpoint(Identifier("  UNNAMED1"), Map(), Literal(Seq.empty), true), "KNOWS", Map()))),
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r2",
        RelationshipEndpoint(Identifier("b"), Map(), Literal(Seq.empty), true),
        RelationshipEndpoint(Identifier("  UNNAMED1"), Map(), Literal(Seq.empty), true), "LOVES", Map())))))


    val startPipe = createPipe(Seq("a", "b"))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(startPipe, q)))
  }

  @Test
  def single_relationship_missing_nodes() {
    val q = PartiallySolvedQuery().copy(start = Seq(
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r",
        RelationshipEndpoint(Identifier("a"), Map(), Literal(Seq.empty), true),
        RelationshipEndpoint(Identifier("b"), Map(), Literal(Seq.empty), true), "LOVES", Map())))))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def single_relationship_missing_nodes_with_expression() {
    val q = PartiallySolvedQuery().copy(updates = Seq(
      Unsolved(CreateRelationship("r",
        RelationshipEndpoint(HeadFunction(Identifier("p")), Map(), Literal(Seq.empty), true),
        RelationshipEndpoint(Identifier("b"), Map(), Literal(Seq.empty), true), "LOVES", Map()))))

    assertFalse("Should not be able to build on this", builder.canWorkWith(plan(q)))
  }
}