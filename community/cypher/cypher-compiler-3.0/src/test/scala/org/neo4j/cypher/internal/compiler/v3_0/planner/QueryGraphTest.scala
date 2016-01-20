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
package org.neo4j.cypher.internal.compiler.v3_0.planner

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast.{AstConstructionTestSupport, RelTypeName}
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class QueryGraphTest extends CypherFunSuite with AstConstructionTestSupport with LogicalPlanConstructionTestSupport  {
  test("returns no pattern relationships when the query graph doesn't contain any") {
    val rels: Set[PatternRelationship] = Set.empty
    val qg = QueryGraph(patternRelationships = rels)

    qg.findRelationshipsEndingOn('x) shouldBe empty
  }

  test("finds single pattern relationship") {
    val r = PatternRelationship('r, ('a, 'b), SemanticDirection.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r))

    qg.findRelationshipsEndingOn('x) shouldBe empty
    qg.findRelationshipsEndingOn('a) should equal(Set(r))
    qg.findRelationshipsEndingOn('b) should equal(Set(r))
  }

  test("finds multiple pattern relationship") {
    val r = PatternRelationship('r, ('a, 'b), SemanticDirection.BOTH, Seq.empty, SimplePatternLength)
    val r2 = PatternRelationship(IdName("r2"), ('b, 'c), SemanticDirection.BOTH, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(r, r2))

    qg.findRelationshipsEndingOn('x) shouldBe empty
    qg.findRelationshipsEndingOn('a) should equal(Set(r))
    qg.findRelationshipsEndingOn('b) should equal(Set(r, r2))
    qg.findRelationshipsEndingOn('c) should equal(Set(r2))
    qg.readQG should equal(qg)
    qg.writeQG should equal(QueryGraph.empty)
  }

  test("extracts write patterns from single node merge QG") {
    val readQG = QueryGraph(patternNodes = Set('a))
    val mergeNode = MergeNodePattern(CreateNodePattern('a, Seq.empty, None), readQG, Seq.empty, Seq.empty)
    val qg = QueryGraph(mutatingPatterns = Seq(mergeNode))

    qg.readQG should equal(readQG)
    qg.writeQG should equal(QueryGraph(mutatingPatterns = Seq(CreateNodePattern('a, Seq.empty, None))))
  }

  test("extracts write patterns from single relationship merge QG") {
    val relationship = PatternRelationship('r, ('a, 'b), SemanticDirection.OUTGOING, Seq(RelTypeName("T")(pos)), SimplePatternLength)
    val readQG = QueryGraph(patternNodes = Set('a), argumentIds = Set('b), patternRelationships = Set(relationship))
    val createNodes: Seq[CreateNodePattern] = Seq(CreateNodePattern('a, Seq.empty, None))
    val createRels = Seq(CreateRelationshipPattern('r, 'a, RelTypeName("T")(pos), 'b, None, SemanticDirection.OUTGOING))

    val mergeNode = MergeRelationshipPattern(createNodes, createRels, readQG, Seq.empty, Seq.empty)
    val qg = QueryGraph(mutatingPatterns = Seq(mergeNode))

    qg.readQG should equal(readQG)
    qg.writeQG should equal(QueryGraph(mutatingPatterns = createNodes ++ createRels))
  }

  test("QG with both reads and writes is split up accordingly") {
    val createNodes: Seq[CreateNodePattern] = Seq(CreateNodePattern('a, Seq.empty, None))
    val qg = QueryGraph(mutatingPatterns = createNodes, patternNodes = Set('b))

    qg.readQG should equal(QueryGraph(patternNodes = Set('b)))
    qg.writeQG should equal(QueryGraph(mutatingPatterns = createNodes))
  }
}
