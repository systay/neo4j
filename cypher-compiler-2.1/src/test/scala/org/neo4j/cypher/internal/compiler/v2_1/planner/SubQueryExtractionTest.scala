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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{SimplePatternLength, IdName, PatternRelationship}
//import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, HasLabels, LabelName, RelTypeName}

class SubQueryExtractionTest extends CypherFunSuite with LogicalPlanningTestSupport {

  import SimpleQueryGraphBuilder.SubQueryExtraction.extractQueryGraph

  val aNode: ast.NodePattern = ast.NodePattern(Some(ast.Identifier("a")(pos)), Seq.empty, None, naked = false) _
  val bNode: ast.NodePattern = ast.NodePattern(Some(ast.Identifier("b")(pos)), Seq.empty, None, naked = false) _
  val unnamedIdentifier: ast.Identifier = ast.Identifier("  UNNAMED1")_
  val anonymousNode: ast.NodePattern = ast.NodePattern(Some(unnamedIdentifier), Seq.empty, None, naked = false) _
  val rRel: ast.RelationshipPattern = ast.RelationshipPattern(Some(ast.Identifier("r")(pos)), false, Seq.empty, None, None, Direction.OUTGOING) _
  val TYP: ast.RelTypeName = ast.RelTypeName("TYP")(None) _

  val rRelWithType: ast.RelationshipPattern = rRel.copy(types = Seq(TYP)) _
  val planRel = PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val planRelWithType = PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq(TYP), SimplePatternLength)

  test("(a)-[r]->(b)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, bNode)

    // When
    val qg = extractQueryGraph(patternExpression)

    // Then
    qg.projections should equal(Map.empty)
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRel))
    qg.argumentIds should equal(Set(IdName("a"), IdName("r"), IdName("b")))
  }

  test("(a)-[r:TYP]->(b)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRelWithType, bNode)

    // When
    val qg = extractQueryGraph(patternExpression)

    // Then
    qg.projections should equal(Map.empty)
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRelWithType))
    qg.argumentIds should equal(Set(IdName("a"), IdName("r"), IdName("b")))
  }

  test("(a)-[r]->(  UNNAMED1)") {
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, anonymousNode)

    // When
    val qg = extractQueryGraph(patternExpression)

    // Then
    qg.projections should equal(Map.empty)
    qg.selections should equal(Selections())
    qg.patternRelationships should equal(Set(planRel.copy(nodes = (IdName("a"), IdName("  UNNAMED1")))))
    qg.argumentIds should equal(Set(IdName("a"), IdName("r")))
  }

  test("(a)-[r]->(b:Label)") {
    val labelName: ast.LabelName = ast.LabelName("Label")() _
    // Given
    val patternExpression = createPatternExpression(aNode, rRel, bNode.copy(labels = Seq(labelName))(pos))

    // When
    val qg = extractQueryGraph(patternExpression)

    // Then
    qg.projections should equal(Map.empty)
    val predicate: ast.HasLabels = ast.HasLabels(ast.Identifier("b")(pos), Seq(labelName)) _
    qg.selections should equal(Selections(Set(Predicate(Set(IdName("b")), predicate))))
    qg.patternRelationships should equal(Set(planRel))
    qg.argumentIds should equal(Set(IdName("a"), IdName("r"), IdName("b")))
  }

  def createPatternExpression(n1: ast.NodePattern, r: ast.RelationshipPattern, n2: ast.NodePattern): ast.PatternExpression =
    ast.PatternExpression(ast.RelationshipsPattern(ast.RelationshipChain(n1, r, n2) _) _)
}
