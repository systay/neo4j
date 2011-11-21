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
import org.scalatest.junit.JUnitSuite
import parser.{ConsoleCypherParser, CypherParser}
import org.junit.Test
import org.junit.Ignore
import org.scalatest.Assertions

class CypherParserTest extends JUnitSuite with Assertions {
  @Test def shouldParseEasiestPossibleQuery() {
    val q = Query.
      start(NodeById("s", 1)).
      returns(ValueReturnItem(EntityValue("s")))
    testQuery("start s = NODE(1) return s", q)
  }

  @Test def sourceIsAnIndex() {
    testQuery(
      """start a = node:index(key = "value") return a""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def sourceIsAnNonParsedIndexQuery() {
    testQuery(
      """start a = node:index("key:value") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:value"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Ignore
  @Test def sourceIsParsedAdvancedLuceneQuery() {
    testQuery(
      """start a = node:index(key="value" AND otherKey="otherValue") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" AND otherKey:\"otherValue\""))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Ignore
  @Test def parsedOrIdxQuery() {
    testQuery(
      """start a = node:index(key="value" or otherKey="otherValue") return a""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" OR otherKey:\"otherValue\""))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldParseEasiestPossibleRelationshipQuery() {
    testQuery(
      "start s = relationship(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ValueReturnItem(EntityValue("s"))))
  }

  @Test def shouldParseEasiestPossibleRelationshipQueryShort() {
    testQuery(
      "start s = rel(1) return s",
      Query.
        start(RelationshipById("s", 1)).
        returns(ValueReturnItem(EntityValue("s"))))
  }

  @Test def sourceIsARelationshipIndex() {
    testQuery(
      """start a = rel:index(key = "value") return a""",
      Query.
        start(RelationshipByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }


  @Test def keywordsShouldBeCaseInsensitive() {
    testQuery(
      "START s = NODE(1) RETURN s",
      Query.
        start(NodeById("s", 1)).
        returns(ValueReturnItem(EntityValue("s"))))
  }

  @Test def shouldParseMultipleNodes() {
    testQuery(
      "start s = NODE(1,2,3) return s",
      Query.
        start(NodeById("s", 1, 2, 3)).
        returns(ValueReturnItem(EntityValue("s"))))
  }

  @Test def shouldParseMultipleInputs() {
    testQuery(
      "start a = node(1), b = NODE(2) return a,b",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def shouldFilterOnProp() {
    testQuery(
      "start a = NODE(1) where a.name = \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldFilterOnPropWithDecimals() {
    testQuery(
      "start a = node(1) where a.extractReturnItems = 3.1415 return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(PropertyValue("a", "extractReturnItems"), Literal(3.1415))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleNot() {
    testQuery(
      "start a = node(1) where not(a.name = \"andres\") return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(PropertyValue("a", "name"), Literal("andres")))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleNotEqualTo() {
    testQuery(
      "start a = node(1) where a.name <> \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(PropertyValue("a", "name"), Literal("andres")))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleLessThan() {
    testQuery(
      "start a = node(1) where a.name < \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThan(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleGreaterThan() {
    testQuery(
      "start a = node(1) where a.name > \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThan(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleLessThanOrEqual() {
    testQuery(
      "start a = node(1) where a.name <= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(LessThanOrEqual(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleRegularComparison() {
    testQuery(
      "start a = node(1) where \"Andres\" =~ /And.*/ return a",
      Query.
        start(NodeById("a", 1)).
        where(RegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ValueReturnItem(EntityValue("a")))
    )
  }

  @Test def shouldHandleMultipleRegularComparison() {
    testQuery(
      """start a = node(1) where a.name =~ /And.*/ AND a.name =~ /And.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(And(RegularExpression(PropertyValue("a", "name"), Literal("And.*")), RegularExpression(PropertyValue("a", "name"), Literal("And.*")))).
        returns(ValueReturnItem(EntityValue("a")))
    )
  }

  @Test def shouldHandleEscapedRegexs() {
    testQuery(
      """start a = node(1) where a.name =~ /And\/.*/ return a""",
      Query.
        start(NodeById("a", 1)).
        where(RegularExpression(PropertyValue("a", "name"), Literal("And\\/.*"))).
        returns(ValueReturnItem(EntityValue("a")))
    )
  }

  @Test def shouldHandleGreaterThanOrEqual() {
    testQuery(
      "start a = node(1) where a.name >= \"andres\" return a",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThanOrEqual(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }


  @Test def booleanLiterals() {
    testQuery(
      "start a = node(1) where true = false return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(true), Literal(false))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldFilterOnNumericProp() {
    testQuery(
      "start a = NODE(1) where 35 = a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(35), PropertyValue("a", "age"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }


  @Test def shouldHandleNegativeLiteralsAsExpected() {
    testQuery(
      "start a = NODE(1) where -35 = a.age AND a.age > -1.2 return a",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(Literal(-35), PropertyValue("a", "age")),
        GreaterThan(PropertyValue("a", "age"), Literal(-1.2)))
      ).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldCreateNotEqualsQuery() {
    testQuery(
      "start a = NODE(1) where 35 != a.age return a",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Literal(35), PropertyValue("a", "age")))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def multipleFilters() {
    testQuery(
      "start a = NODE(1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query.
        start(NodeById("a", 1)).
        where(Or(
        Equals(PropertyValue("a", "name"), Literal("andres")),
        Equals(PropertyValue("a", "name"), Literal("mattias")))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def relatedTo() {
    testQuery(
      "start a = NODE(1) match a -[:KNOWS]-> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def relatedToWithoutRelType() {
    testQuery(
      "start a = NODE(1) match a --> (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    testQuery(
      "start a = NODE(1) match a -[r]-> (b) return r",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", None, Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("r"))))
  }

  @Test def relatedToTheOtherWay() {
    testQuery(
      "start a = NODE(1) match a <-[:KNOWS]- (b) return a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.INCOMING, false)).
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def shouldOutputVariables() {
    testQuery(
      "start a = NODE(1) return a.name",
      Query.
        start(NodeById("a", 1)).
        returns(ValueReturnItem(PropertyValue("a", "name"))))
  }

  @Test def shouldHandleAndClauses() {
    testQuery(
      "start a = NODE(1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(PropertyValue("a", "name"), Literal("andres")),
        Equals(PropertyValue("a", "lastname"), Literal("taylor")))).
        returns(ValueReturnItem(PropertyValue("a", "name"))))
  }

  @Test def relatedToWithRelationOutput() {
    testQuery(
      "start a = NODE(1) match a -[rel:KNOWS]-> (b) return rel",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "rel", Some("KNOWS"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("rel"))))
  }

  @Test def relatedToWithoutEndName() {
    testQuery(
      "start a = NODE(1) match a -[:MARRIED]-> () return a",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED1", "  UNNAMED2", Some("MARRIED"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def relatedInTwoSteps() {
    testQuery(
      "start a = NODE(1) match a -[:KNOWS]-> b -[:FRIEND]-> (c) return c",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, false),
        RelatedTo("b", "c", "  UNNAMED2", Some("FRIEND"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("c")))
    )
  }

  @Test def djangoRelationshipType() {
    testQuery(
      "start a = NODE(1) match a -[:`<<KNOWS>>`]-> b return c",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("<<KNOWS>>"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("c"))))
  }

  @Test def countTheNumberOfHits() {
    testQuery(
      "start a = NODE(1) match a --> b return a, b, count(*)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def distinct() {
    testQuery(
      "start a = NODE(1) match a --> b return distinct a, b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation().
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def sumTheAgesOfPeople() {
    testQuery(
      "start a = NODE(1) match a --> b return a, b, sum(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Sum(PropertyValue("a", "age")))).
        columns("a", "b", "sum(a.age)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def avgTheAgesOfPeople() {
    testQuery(
      "start a = NODE(1) match a --> b return a, b, avg(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Avg(PropertyValue("a", "age")))).
        columns("a", "b", "avg(a.age)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def minTheAgesOfPeople() {
    testQuery(
      "start a = NODE(1) match (a) --> b return a, b, min(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Min(PropertyValue("a", "age")))).
        columns("a", "b", "min(a.age)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def maxTheAgesOfPeople() {
    testQuery(
      "start a = NODE(1) match a --> b return a, b, max(a.age)",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Max((PropertyValue("a", "age"))))).
        columns("a", "b", "max(a.age)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def singleColumnSorting() {
    testQuery(
      "start a = NODE(1) return a order by a.name",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueReturnItem(PropertyValue("a", "name")), true)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def sortOnAggregatedColumn() {
    testQuery(
      "start a = NODE(1) return a order by avg(a.name)",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueAggregationItem(Avg(PropertyValue("a", "name"))), true)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleTwoSortColumns() {
    testQuery(
      "start a = NODE(1) return a order by a.name, a.age",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(ValueReturnItem(PropertyValue("a", "name")), true),
        SortItem(ValueReturnItem(PropertyValue("a", "age")), true)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleTwoSortColumnsAscending() {
    testQuery(
      "start a = NODE(1) return a order by a.name ASCENDING, a.age ASC",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(ValueReturnItem(PropertyValue("a", "name")), true),
        SortItem(ValueReturnItem(PropertyValue("a", "age")), true)).
        returns(ValueReturnItem(EntityValue("a"))))

  }

  @Test def orderByDescending() {
    testQuery(
      "start a = NODE(1) return a order by a.name DESCENDING",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueReturnItem(PropertyValue("a", "name")), false)).
        returns(ValueReturnItem(EntityValue("a"))))

  }

  @Test def orderByDesc() {
    testQuery(
      "start a = NODE(1) return a order by a.name desc",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueReturnItem(PropertyValue("a", "name")), false)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def nullableProperty() {
    testQuery(
      "start a = NODE(1) return a.name?",
      Query.
        start(NodeById("a", 1)).
        returns(ValueReturnItem(NullablePropertyValue("a", "name"))))
  }

  @Test def nestedBooleanOperatorsAndParentesis() {
    testQuery(
      """start n = NODE(1,2,3) where (n.animal = "monkey" and n.food = "banana") or (n.animal = "cow" and n
      .food="grass") return n""",
      Query.
        start(NodeById("n", 1, 2, 3)).
        where(Or(
        And(
          Equals(PropertyValue("n", "animal"), Literal("monkey")),
          Equals(PropertyValue("n", "food"), Literal("banana"))),
        And(
          Equals(PropertyValue("n", "animal"), Literal("cow")),
          Equals(PropertyValue("n", "food"), Literal("grass"))))).
        returns(ValueReturnItem(EntityValue("n"))))
  }

  @Test def limit5() {
    testQuery(
      "start n=NODE(1) return n limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        returns(ValueReturnItem(EntityValue("n"))))
  }

  @Test def skip5() {
    testQuery(
      "start n=NODE(1) return n skip 5",
      Query.
        start(NodeById("n", 1)).
        skip(5).
        returns(ValueReturnItem(EntityValue("n"))))
  }

  @Test def skip5limit5() {
    testQuery(
      "start n=NODE(1) return n skip 5 limit 5",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        skip(5).
        returns(ValueReturnItem(EntityValue("n"))))
  }

  @Test def relationshipType() {
    testQuery(
      "start n=NODE(1) match n-[r]->(x) where type(r) = \"something\" return r",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
        where(Equals(RelationshipTypeValue(EntityValue("r")), Literal("something"))).
        returns(ValueReturnItem(EntityValue("r"))))
  }

  @Test def pathLength() {
    testQuery(
      "start n=NODE(1) match p=(n-->x) where LENGTH(p) = 10 return p",
      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        where(Equals(ArrayLengthValue(EntityValue("p")), Literal(10.0))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def relationshipTypeOut() {
    testQuery(
      "start n=NODE(1) match n-[r]->(x) return type(r)",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
        returns(ValueReturnItem(RelationshipTypeValue(EntityValue("r")))))
  }

  @Test def relationshipsFromPathOutput() {
    testQuery(
      "start n=NODE(1) match p=n-->x return relationships(p)",

      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        returns(ValueReturnItem(PathRelationshipsValue(EntityValue("p")))))
  }

  @Test def relationshipsFromPathInWhere() {
    testQuery(
      "start n=NODE(1) match p=n-->x where length(rels(p))=1 return p",

      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        where(Equals(ArrayLengthValue(PathRelationshipsValue(EntityValue("p"))), Literal(1)))
        returns (ValueReturnItem(EntityValue("p"))))
  }

  @Test def countNonNullValues() {
    testQuery(
      "start a = NODE(1) return a, count(a)",
      Query.
        start(NodeById("a", 1)).
        aggregation(ValueAggregationItem(Count(EntityValue("a")))).
        columns("a", "count(a)").
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleIdBothInReturnAndWhere() {
    testQuery(
      "start a = NODE(1) where id(a) = 0 return id(a)",
      Query.
        start(NodeById("a", 1)).
        where(Equals(IdValue(EntityValue("a")), Literal(0)))
        returns (ValueReturnItem(IdValue(EntityValue("a")))))
  }

  @Test def shouldBeAbleToHandleStringLiteralsWithApostrophe() {
    testQuery(
      "start a = node:index(key = 'value') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleQuotationsInsideApostrophes() {
    testQuery(
      "start a = node:index(key = 'val\"ue') return a",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("val\"ue"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def simplePathExample() {
    testQuery(
      "start a = node(0) match p = ( a-->b ) return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def threeStepsPath() {
    testQuery(
      "start a = node(0) match p = ( a-->b-->c ) return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p",
        RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false),
        RelatedTo("b", "c", "  UNNAMED2", None, Direction.OUTGOING, false)
      ))
        returns (ValueReturnItem(EntityValue("a"))))
  }

  @Test def pathsShouldBePossibleWithoutParenthesis() {
    testQuery(
      "start a = node(0) match p = a-->b return a",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)))
        returns (ValueReturnItem(EntityValue("a"))))
  }

  @Test def variableLengthPath() {
    testQuery("start a=node(0) match a -[:knows*1..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
        returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def fixedVarLengthPath() {
    testQuery("start a=node(0) match a -[*3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(3), Some(3), None, Direction.OUTGOING, None,
        false)).
        returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def variableLengthPathWithoutMinDepth() {
    testQuery("start a=node(0) match a -[:knows*..3]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
        returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def variableLengthPathWithRelationshipIdentifier() {
    testQuery("start a=node(0) match a -[r:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, Some("knows"), Direction.OUTGOING,
        Some("r"), false)).
        returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def variableLengthPathWithoutMaxDepth() {
    testQuery("start a=node(0) match a -[:knows*2..]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
        returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def unboundVariableLengthPath() {
    testQuery("start a=node(0) match a -[:knows*]-> x return x",
      Query.
        start(NodeById("a", 0)).
        matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, None, "knows", Direction.OUTGOING)).
        returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def optionalRelationship() {
    testQuery(
      "start a = node(1) match a -[?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, true)).
        returns(ValueReturnItem(EntityValue("b"))))
  }

  @Test def optionalTypedRelationship() {
    testQuery(
      "start a = node(1) match a -[?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, true)).
        returns(ValueReturnItem(EntityValue("b"))))
  }

  @Test def optionalTypedAndNamedRelationship() {
    testQuery(
      "start a = node(1) match a -[r?:KNOWS]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Some("KNOWS"), Direction.OUTGOING, true)).
        returns(ValueReturnItem(EntityValue("b"))))
  }

  @Test def optionalNamedRelationship() {
    testQuery(
      "start a = node(1) match a -[r?]-> (b) return b",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", None, Direction.OUTGOING, true)).
        returns(ValueReturnItem(EntityValue("b"))))
  }

  @Test def testOnAllNodesInAPath() {
    testQuery(
      """start a = node(1) match p = a --> b --> c where ALL(n in NODES(p) : n.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        namedPaths(
        NamedPath("p",
          RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false),
          RelatedTo("b", "c", "  UNNAMED2", None, Direction.OUTGOING, false))).
        where(AllInSeq(PathNodesValue(EntityValue("p")), "n", Equals(PropertyValue("n", "name"), Literal("Andres"))))
        returns (ValueReturnItem(EntityValue("b"))))
  }

  @Test def extractNameFromAllNodes() {
    testQuery(
      """start a = node(1) match p = a --> b --> c return extract(n in nodes(p) : n.name)""",
      Query.
        start(NodeById("a", 1)).
        namedPaths(
        NamedPath("p",
          RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false),
          RelatedTo("b", "c", "  UNNAMED2", None, Direction.OUTGOING, false))).
        returns(ValueReturnItem(Extract(PathNodesValue(EntityValue("p")), "n", PropertyValue("n", "name")))))
  }


  @Test def testAny() {
    testQuery(
      """start a = node(1) where ANY(x in NODES(p): x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(AnyInSeq(PathNodesValue(EntityValue("p")), "x", Equals(PropertyValue("x", "name"), Literal("Andres"))))
        returns (ValueReturnItem(EntityValue("b"))))
  }

  @Test def testNone() {
    testQuery(
      """start a = node(1) where none(x in nodes(p) : x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(NoneInSeq(PathNodesValue(EntityValue("p")), "x", Equals(PropertyValue("x", "name"), Literal("Andres"))))
        returns (ValueReturnItem(EntityValue("b"))))
  }

  @Test def testSingle() {
    testQuery(
      """start a = node(1) where single(x in NODES(p): x.name = "Andres") return b""",
      Query.
        start(NodeById("a", 1)).
        where(SingleInSeq(PathNodesValue(EntityValue("p")), "x", Equals(PropertyValue("x", "name"),
        Literal("Andres"))))
        returns (ValueReturnItem(EntityValue("b"))))
  }

  @Test def testParamAsStartNode() {
    testQuery(
      """start pA = node({a}) return pA""",
      Query.
        start(NodeById("pA", ParameterValue("a"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testNumericParamNameAsStartNode() {
    testQuery(
      """start pA = node({0}) return pA""",
      Query.
        start(NodeById("pA", ParameterValue("0"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForWhereLiteral() {
    testQuery(
      """start pA = node(1) where pA.name = {name} return pA""",
      Query.
        start(NodeById("pA", 1)).
        where(Equals(PropertyValue("pA", "name"), ParameterValue("name")))
        returns (ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForIndexKey() {
    testQuery(
      """start pA = node:idx({key} = "Value") return pA""",
      Query.
        start(NodeByIndex("pA", "idx", ParameterValue("key"), Literal("Value"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForIndexValue() {
    testQuery(
      """start pA = node:idx(key = {Value}) return pA""",
      Query.
        start(NodeByIndex("pA", "idx", Literal("key"), ParameterValue("Value"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForIndexQuery() {
    testQuery(
      """start pA = node:idx({query}) return pA""",
      Query.
        start(NodeByIndexQuery("pA", "idx", ParameterValue("query"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForSkip() {
    testQuery(
      """start pA = node(0) return pA skip {skipper}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        returns (ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForLimit() {
    testQuery(
      """start pA = node(0) return pA limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        limit("stop")
        returns (ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForLimitAndSkip() {
    testQuery(
      """start pA = node(0) return pA skip {skipper} limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        limit ("stop")
        returns (ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForRegex() {
    testQuery(
      """start pA = node(0) where pA.name =~ {regex} return pA""",
      Query.
        start(NodeById("pA", 0)).
        where(RegularExpression(PropertyValue("pA", "name"), ParameterValue("regex")))
        returns (ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testShortestPath() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", None, Direction.OUTGOING, Some(1), false))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testShortestPathWithMaxDepth() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", None, Direction.OUTGOING, Some(6), false))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testShortestPathWithType() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[:KNOWS*..6]->b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", Some("KNOWS"), Direction.OUTGOING, Some(6),
        false))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testShortestPathBiDirectional() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[*..6]-b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", None, Direction.BOTH, Some(6), false))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testShortestPathOptional() {
    testQuery(
      """start a=node(0), b=node(1) match p = shortestPath( a-[?*..6]-b ) return p""",
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", None, Direction.BOTH, Some(6), true))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testForNull() {
    testQuery(
      """start a=node(0) where a is null return a""",
      Query.
        start(NodeById("a", 0)).
        where(IsNull(EntityValue("a")))
        returns (ValueReturnItem(EntityValue("a"))))
  }

  @Test def testForNotNull() {
    testQuery(
      """start a=node(0) where a is not null return a""",
      Query.
        start(NodeById("a", 0)).
        where(Not(IsNull(EntityValue("a"))))
        returns (ValueReturnItem(EntityValue("a"))))
  }

  @Test def testCountDistinct() {
    testQuery(
      """start a=node(0) return count(distinct a)""",
      Query.
        start(NodeById("a", 0)).
        aggregation(ValueAggregationItem(Distinct(Count(EntityValue("a")), EntityValue("a")))).
        columns("count(distinct a)")
        returns())
  }

  @Test def consoleModeParserShouldOutputNullableProperties() {
    val query = "start a = node(1) return a.name"
    val parser = new ConsoleCypherParser()
    val executionTree = parser.parse(query)

    assertEquals(
      Query.
        start(NodeById("a", 1)).
        returns(ValueReturnItem(NullablePropertyValue("a", "name"))),
      executionTree)
  }

  def testQuery(query: String, expectedQuery: Query) {
    val parser = new CypherParser()

    try {
      val ast = parser.parse(query)

      assert(expectedQuery === ast)
    } catch {
      case x => {
        println(x)
        throw new Exception(query + "\n\n" + x.getMessage)
      }
    }
  }


}
