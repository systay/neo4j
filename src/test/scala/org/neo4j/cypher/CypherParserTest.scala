/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.commands._
import org.junit.Assert._
import org.neo4j.graphdb.Direction
import org.junit.Test

class CypherParserTest {
  def testQuery(query: String, expectedQuery: Query) {
    val parser = new CypherParser()
    val executionTree = parser.parse(query)

    assertEquals(expectedQuery, executionTree)
  }

  @Test def shouldParseEasiestPossibleQuery() {
    testQuery(
      "start s = (1) return s",
      Query(
        Return(EntityOutput("s")),
        Start(NodeById("s", 1))))
  }

  @Test def sourceIsAnIndex() {
    testQuery(
      """start a = (index, key, "value") return a""",
      Query(
        Return(EntityOutput("a")),
        Start(NodeByIndex("a", "index", "key", "value"))))
  }

  @Test def shouldParseEasiestPossibleRelationshipQuery() {
    testQuery(
      "start s = <1> return s",
      Query(
        Return(EntityOutput("s")),
        Start(RelationshipById("s", 1))))
  }

  @Test def sourceIsARelationshipIndex() {
    testQuery(
      """start a = <index, key, "value"> return a""",
      Query(
        Return(EntityOutput("a")),
        Start(RelationshipByIndex("a", "index", "key", "value"))))
  }


  @Test def keywordsShouldBeCaseInsensitive() {
    testQuery(
      "START start = (1) RETURN start",
      Query(
        Return(EntityOutput("start")),
        Start(NodeById("start", 1))))
  }

  @Test def shouldParseMultipleNodes() {
    testQuery(
      "start s = (1,2,3) return s",
      Query(
        Return(EntityOutput("s")),
        Start(NodeById("s", 1, 2, 3))))
  }

  @Test def shouldParseMultipleInputs() {
    testQuery(
      "start a = (1), b = (2) return a,b",
      Query(
        Return(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1), NodeById("b", 2))))
  }

  @Test def shouldFilterOnProp() {
    testQuery(
      "start a = (1) where a.name = \"andres\" return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Equals(PropertyValue("a", "name"), Literal("andres"))))
  }

  @Test def shouldHandleNot() {
    testQuery(
      "start a = (1) where not(a.name = \"andres\") return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Not(Equals(PropertyValue("a", "name"), Literal("andres")))))
  }

  @Test def shouldHandleNotEqualTo() {
    testQuery(
      "start a = (1) where a.name <> \"andres\" return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Not(Equals(PropertyValue("a", "name"), Literal("andres")))))
  }

  @Test def shouldHandleLessThan() {
    testQuery(
      "start a = (1) where a.name < \"andres\" return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        LessThan(PropertyValue("a", "name"), Literal("andres"))))
  }

  @Test def shouldHandleGreaterThan() {
    testQuery(
      "start a = (1) where a.name > \"andres\" return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        GreaterThan(PropertyValue("a", "name"), Literal("andres"))))
  }

  @Test def shouldHandleLessThanOrEqual() {
    testQuery(
      "start a = (1) where a.name <= \"andres\" return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        LessThanOrEqual(PropertyValue("a", "name"), Literal("andres"))))
  }

  @Test def shouldHandleRegularComparison() {
    testQuery(
      "start a = (1) where \"Andres\" =~ /And.*/ return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        RegularExpression(Literal("Andres"), "And.*"))
    )
  }

  @Test def shouldHandleGreaterThanOrEqual() {
    testQuery(
      "start a = (1) where a.name >= \"andres\" return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        GreaterThanOrEqual(PropertyValue("a", "name"), Literal("andres"))))
  }


  @Test def booleanLiterals() {
    testQuery(
      "start a = (1) where true = false return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Equals(Literal(true), Literal(false))))
  }

  @Test def shouldFilterOnNumericProp() {
    testQuery(
      "start a = (1) where 35 = a.age return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Equals(Literal(35), PropertyValue("a", "age"))))
  }

  @Test def multipleFilters() {
    testQuery(
      "start a = (1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Or(
          Equals(PropertyValue("a", "name"), Literal("andres")),
          Equals(PropertyValue("a", "name"), Literal("mattias")))))
  }

  @Test def relatedTo() {
    testQuery(
      "start a = (1) match (a) -[:KNOWS]-> (b) return a, b",
      Query(
        Return(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, Some("KNOWS"), Direction.OUTGOING))))
  }

  @Test def relatedToWithoutRelType() {
    testQuery(
      "start a = (1) match (a) --> (b) return a, b",
      Query(
        Return(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, None, Direction.OUTGOING))))
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    testQuery(
      "start a = (1) match (a) -[r]-> (b) return r",
      Query(
        Return(EntityOutput("r")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", Some("r"), None, Direction.OUTGOING))))
  }

  @Test def relatedToTheOtherWay() {
    testQuery(
      "start a = (1) match (a) <-[:KNOWS]- (b) return a, b",
      Query(
        Return(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, Some("KNOWS"), Direction.INCOMING))))
  }

  @Test def shouldOutputVariables() {
    testQuery(
      "start a = (1) return a.name",
      Query(
        Return(PropertyOutput("a", "name")),
        Start(NodeById("a", 1))))
  }

  @Test def shouldHandleAndClauses() {
    testQuery(
      "start a = (1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query(
        Return(PropertyOutput("a", "name")),
        Start(NodeById("a", 1)),
        And(
          Equals(PropertyValue("a", "name"), Literal("andres")),
          Equals(PropertyValue("a", "lastname"), Literal("taylor")))))
  }

  @Test def relatedToWithRelationOutput() {
    testQuery(
      "start a = (1) match (a) -[rel,:KNOWS]-> (b) return rel",
      Query(
        Return(EntityOutput("rel")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", "rel", "KNOWS", Direction.OUTGOING))))
  }

  @Test def relatedToWithoutEndName() {
    testQuery(
      "start a = (1) match (a) -[:MARRIED]-> () return a",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "___NODE1", None, Some("MARRIED"), Direction.OUTGOING))))
  }

  @Test def relatedInTwoSteps() {
    testQuery(
      "start a = (1) match (a) -[:KNOWS]-> (b) -[:FRIEND]-> (c) return c",
      Query(
        Return(EntityOutput("c")),
        Start(NodeById("a", 1)),
        Match(
          RelatedTo("a", "b", None, Some("KNOWS"), Direction.OUTGOING),
          RelatedTo("b", "c", None, Some("FRIEND"), Direction.OUTGOING))))
  }

  @Test def djangoRelationshipType() {
    testQuery(
      "start a = (1) match (a) -[:`<<KNOWS>>`]-> (b) return c",
      Query(
        Return(EntityOutput("c")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, Some("<<KNOWS>>"), Direction.OUTGOING))))
  }

  @Test def countTheNumberOfHits() {
    testQuery(
      "start a = (1) match (a) --> (b) return a, b, count(*)",
      Query(
        Return(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, None, Direction.OUTGOING)),
        Aggregation(Count("*"))))
  }

  @Test def singleColumnSorting() {
    testQuery(
      "start a = (1) return a order by a.name",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Sort(SortItem(PropertyOutput("a", "name"), true))))
  }

  @Test def shouldHandleTwoSortColumns() {
    testQuery(
      "start a = (1) return a order by a.name, a.age",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Sort(
          SortItem(PropertyOutput("a", "name"), true),
          SortItem(PropertyOutput("a", "age"), true))))
  }

  @Test def shouldHandleTwoSortColumnsAscending() {
    testQuery(
      "start a = (1) return a order by a.name ASCENDING, a.age ASC",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Sort(
          SortItem(PropertyOutput("a", "name"), true),
          SortItem(PropertyOutput("a", "age"), true))))
  }

  @Test def orderByDescending() {
    testQuery(
      "start a = (1) return a order by a.name DESCENDING",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Sort(
          SortItem(PropertyOutput("a", "name"), false))))
  }

  @Test def orderByDesc() {
    testQuery(
      "start a = (1) return a order by a.name desc",
      Query(
        Return(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Sort(
          SortItem(PropertyOutput("a", "name"), false))))
  }

  @Test def nullableProperty() {
    testQuery(
      "start a = (1) return a.name?",
      Query(
        Return(NullablePropertyOutput("a", "name")),
        Start(NodeById("a", 1))))
  }

  @Test def nestedBooleanOperatorsAndParentesis() {
    testQuery(
      """start n = (1,2,3) where (n.animal = "monkey" and n.food = "banana") or (n.animal = "cow" and n.food="grass") return n""",
      Query(
        Return(EntityOutput("n")),
        Start(NodeById("n", 1, 2, 3)),
        Or(
          And(
            Equals(PropertyValue("n", "animal"), Literal("monkey")),
            Equals(PropertyValue("n", "food"), Literal("banana"))),
          And(
            Equals(PropertyValue("n", "animal"), Literal("cow")),
            Equals(PropertyValue("n", "food"), Literal("grass"))))))
  }

  @Test def limit5() {
    testQuery(
      "start n=(1) return n limit 5",
      Query(
        Return(EntityOutput("n")),
        Start(NodeById("n", 1)),
        Slice(None, Some(5))))
  }

  @Test def from5() {
    testQuery(
      "start n=(1) return n from 5",
      Query(
        Return(EntityOutput("n")),
        Start(NodeById("n", 1)),
        Slice(Some(5), None)))
  }

  @Test def from5limit5() {
    testQuery(
      "start n=(1) return n from 5 limit 5",
      Query(
        Return(EntityOutput("n")),
        Start(NodeById("n", 1)),
        Slice(Some(5), Some(5))))
  }
}
