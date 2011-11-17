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

import commands._
import org.junit.Assert._
import java.lang.String
import parser.CypherParser
import scala.collection.JavaConverters._
import org.junit.matchers.JUnitMatchers._
import org.neo4j.graphdb.{Path, Relationship, Direction, Node}
import org.junit.{Ignore, Test}

class ExecutionEngineTest extends ExecutionEngineHelper {

  @Test def shouldGetReferenceNode() {
    val query = Query.
      start(NodeById("node", Literal(0))).
      returns(ValueReturnItem(EntityValue("node")))

    val result = execute(query)
    assertEquals(List(refNode), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetRelationshipById() {
    val n = createNode()
    val r = relate(n, refNode, "rel")

    val query = Query.
      start(RelationshipById("r", Literal(0))).
      returns(ValueReturnItem(EntityValue("r")))

    val result = execute(query)
    assertEquals(List(r), result.columnAs[Relationship]("r").toList)
  }

  @Test def shouldFilterOnGreaterThan() {
    val result = parseAndExecute("start node=node(0) where 0<1 return node")

    assertEquals(List(refNode), result.columnAs[Node]("node").toList)
  }

  @Test def shouldFilterOnRegexp() {
    val n1 = createNode(Map("name" -> "Andres"))
    val n2 = createNode(Map("name" -> "Jim"))

    val query = Query.
      start(NodeById("node", n1.getId, n2.getId)).
      where(RegularExpression(PropertyValue("node", "name"), Literal("And.*"))).
      returns(ValueReturnItem(EntityValue("node")))

    val result = execute(query)
    assertEquals(List(n1), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetOtherNode() {
    val node: Node = createNode()

    val query = Query.
      start(NodeById("node", node.getId)).
      returns(ValueReturnItem(EntityValue("node")))

    val result = execute(query)
    assertEquals(List(node), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetRelationship() {
    val node: Node = createNode()
    val rel: Relationship = relate(refNode, node, "yo")

    val query = Query.
      start(RelationshipById("rel", rel.getId)).
      returns(ValueReturnItem(EntityValue("rel")))

    val result = execute(query)
    assertEquals(List(rel), result.columnAs[Relationship]("rel").toList)
  }

  @Test def shouldGetTwoNodes() {
    val node: Node = createNode()

    val query = Query.
      start(NodeById("node", refNode.getId, node.getId)).
      returns(ValueReturnItem(EntityValue("node")))

    val result = execute(query)
    assertEquals(List(refNode, node), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetNodeProperty() {
    val name = "Andres"
    val node: Node = createNode(Map("name" -> name))

    val query = Query.
      start(NodeById("node", node.getId)).
      returns(ValueReturnItem(PropertyValue("node", "name")))

    val result = execute(query)
    val list = result.columnAs[String]("node.name").toList
    assertEquals(List(name), list)
  }

  @Test def shouldFilterOutBasedOnNodePropName() {
    val name = "Andres"
    val start: Node = createNode()
    val a1: Node = createNode(Map("name" -> "Someone Else"))
    val a2: Node = createNode(Map("name" -> name))
    relate(start, a1, "x")
    relate(start, a2, "x")

    val query = Query.
      start(NodeById("start", start.getId)).
      matches(RelatedTo("start", "a", "rel", "x", Direction.BOTH)).
      where(Equals(PropertyValue("a", "name"), Literal(name))).
      returns(ValueReturnItem(EntityValue("a")))

    val result = execute(query)
    assertEquals(List(a2), result.columnAs[Node]("a").toList)
  }

  @Test def shouldFilterBasedOnRelPropName() {
    val start: Node = createNode()
    val a: Node = createNode()
    val b: Node = createNode()
    relate(start, a, "KNOWS", Map("name" -> "monkey"))
    relate(start, b, "KNOWS", Map("name" -> "woot"))

    val query = Query.
      start(NodeById("start", start.getId)).
      matches(RelatedTo("start", "a", "r", "KNOWS", Direction.BOTH)).
      where(Equals(PropertyValue("r", "name"), Literal("monkey"))).
      returns(ValueReturnItem(EntityValue("a")))

    val result = execute(query)
    assertEquals(List(a), result.columnAs[Node]("a").toList)
  }

  @Test def shouldOutputTheCartesianProductOfTwoNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()

    val query = Query.
      start(NodeById("n1", n1.getId), NodeById("n2", n2.getId)).
      returns(ValueReturnItem(EntityValue("n1")), ValueReturnItem(EntityValue("n2")))

    val result = execute(query)

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetNeighbours() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    relate(n1, n2, "KNOWS")

    val query = Query.
      start(NodeById("n1", n1.getId)).
      matches(RelatedTo("n1", "n2", "rel", "KNOWS", Direction.OUTGOING)).
      returns(ValueReturnItem(EntityValue("n1")), ValueReturnItem(EntityValue("n2")))

    val result = execute(query)

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetTwoRelatedNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val query = Query.
      start(NodeById("start", n1.getId)).
      matches(RelatedTo("start", "x", "rel", "KNOWS", Direction.OUTGOING)).
      returns(ValueReturnItem(EntityValue("x")))

    val result = execute(query)

    assertEquals(List(Map("x" -> n2), Map("x" -> n3)), result.toList)
  }

  @Test def executionResultTextualOutput() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val query = Query.
      start(NodeById("start", n1.getId)).
      matches(RelatedTo("start", "x", "rel", "KNOWS", Direction.OUTGOING)).
      returns(ValueReturnItem(EntityValue("x")), ValueReturnItem(EntityValue("start")))

    val result = execute(query)

    val textOutput = result.dumpToString()

    println(textOutput)
  }

  @Test def doesNotFailOnVisualizingEmptyOutput() {
    val query = Query.
      start(NodeById("start", refNode.getId)).
      where(Equals(Literal(1), Literal(0))).
      returns(ValueReturnItem(EntityValue("start")))

    val result = execute(query)

    println(result.dumpToString())
  }

  @Test def shouldGetRelatedToRelatedTo() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n2, n3, "FRIEND")

    val query = Query.
      start(NodeById("start", n1.getId)).
      matches(
      RelatedTo("start", "a", "rel", "KNOWS", Direction.OUTGOING),
      RelatedTo("a", "b", "rel2", "FRIEND", Direction.OUTGOING)).
      returns(ValueReturnItem(EntityValue("b")))

    val result = execute(query)

    assertEquals(List(Map("b" -> n3)), result.toList)
  }

  @Test def shouldFindNodesByExactIndexLookup() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = Query.
      start(NodeByIndex("n", idxName, Literal(key), Literal(value))).
      returns(ValueReturnItem(EntityValue("n")))

    val result = execute(query)

    assertEquals(List(Map("n" -> n)), result.toList)
  }

  @Test def shouldFindNodesByIndexQuery() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = Query.
      start(NodeByIndexQuery("n", idxName, Literal(key + ":" + value))).
      returns(ValueReturnItem(EntityValue("n")))

    val result = execute(query)

    assertEquals(List(Map("n" -> n)), result.toList)
  }

  @Test def shouldFindNodesByIndexParameters() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    indexNode(n, idxName, key, "Andres")

    val query = Query.
      start(NodeByIndex("n", idxName, Literal(key), ParameterValue("value"))).
      returns(ValueReturnItem(EntityValue("n")))

    val result = execute(query, "value" -> "Andres")

    assertEquals(List(Map("n" -> n)), result.toList)
  }

  @Test def shouldFindNodesByIndexWildcardQuery() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = Query.
      start(NodeByIndexQuery("n", idxName, Literal(key + ":andr*"))).
      returns(ValueReturnItem(EntityValue("n")))

    val result = execute(query)

    assertEquals(List(Map("n" -> n)), result.toList)
  }

  @Test def shouldHandleOrFilters() {
    val n1 = createNode(Map("name" -> "boy"))
    val n2 = createNode(Map("name" -> "girl"))

    val query = Query.
      start(NodeById("n", n1.getId, n2.getId)).
      where(Or(
      Equals(PropertyValue("n", "name"), Literal("boy")),
      Equals(PropertyValue("n", "name"), Literal("girl")))).
      returns(ValueReturnItem(EntityValue("n")))

    val result = execute(query)

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }


  @Test def shouldHandleNestedAndOrFilters() {
    val n1 = createNode(Map("animal" -> "monkey", "food" -> "banana"))
    val n2 = createNode(Map("animal" -> "cow", "food" -> "grass"))
    val n3 = createNode(Map("animal" -> "cow", "food" -> "banana"))

    val query = Query.
      start(NodeById("n", n1.getId, n2.getId, n3.getId)).
      where(Or(
      And(
        Equals(PropertyValue("n", "animal"), Literal("monkey")),
        Equals(PropertyValue("n", "food"), Literal("banana"))),
      And(
        Equals(PropertyValue("n", "animal"), Literal("cow")),
        Equals(PropertyValue("n", "food"), Literal("grass"))))).
      returns(ValueReturnItem(EntityValue("n")))

    val result = execute(query)

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldBeAbleToOutputNullForMissingProperties() {
    val query = Query.
      start(NodeById("node", 0)).
      returns(ValueReturnItem(NullablePropertyValue("node", "name")))

    val result = execute(query)
    assertEquals(List(Map("node.name" -> null)), result.toList)
  }

  @Test def testOnlyIfPropertyExists() {
    createNode(Map("prop" -> "A"))
    createNode()

    val result = parseAndExecute("start a=node(1,2) where a.prop? = 'A' return a")

    assert(2 === result.toSeq.length)
  }

  @Test def shouldHandleComparisonBetweenNodeProperties() {
    //start n = node(1,4) match (n) --> (x) where n.animal = x.animal return n,x
    val n1 = createNode(Map("animal" -> "monkey"))
    val n2 = createNode(Map("animal" -> "cow"))
    val n3 = createNode(Map("animal" -> "monkey"))
    val n4 = createNode(Map("animal" -> "cow"))

    relate(n1, n2, "A")
    relate(n1, n3, "A")
    relate(n4, n2, "A")
    relate(n4, n3, "A")

    val query = Query.
      start(NodeById("n", n1.getId, n4.getId)).
      matches(RelatedTo("n", "x", "rel", None, Direction.OUTGOING, false)).
      where(Equals(PropertyValue("n", "animal"), PropertyValue("x", "animal"))).
      returns(ValueReturnItem(EntityValue("n")), ValueReturnItem(EntityValue("x")))

    val result = execute(query)

    assertEquals(List(
      Map("n" -> n1, "x" -> n3),
      Map("n" -> n4, "x" -> n2)), result.toList)

    assertEquals(List("n", "x"), result.columns)
  }

  @Test def comparingNumbersShouldWorkNicely() {
    val n1 = createNode(Map("x" -> 50))
    val n2 = createNode(Map("x" -> 50l))
    val n3 = createNode(Map("x" -> 50f))
    val n4 = createNode(Map("x" -> 50d))
    val n5 = createNode(Map("x" -> 50.toByte))

    val query = Query.
      start(NodeById("n", n1.getId, n2.getId, n3.getId, n4.getId, n5.getId)).
      where(LessThan(PropertyValue("n", "x"), Literal(100))).
      returns(ValueReturnItem(EntityValue("n")))

    val result = execute(query)

    assertEquals(List(n1, n2, n3, n4, n5), result.columnAs[Node]("n").toList)
  }

  @Test def comparingStringAndCharsShouldWorkNicely() {
    val n1 = createNode(Map("x" -> "Anders"))
    val n2 = createNode(Map("x" -> 'C'))

    val query = Query.
      start(NodeById("n", n1.getId, n2.getId)).
      where(And(
      LessThan(PropertyValue("n", "x"), Literal("Z")),
      LessThan(PropertyValue("n", "x"), Literal('Z')))).
      returns(ValueReturnItem(EntityValue("n")))

    val result = execute(query)

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldBeAbleToCountNodes() {
    val a = createNode() //start a = (0) match (a) --> (b) return a, count(*)
    val b = createNode()
    relate(refNode, a, "A")
    relate(refNode, b, "A")

    val query = Query.
      start(NodeById("a", refNode.getId)).
      matches(RelatedTo("a", "b", "rel", None, Direction.OUTGOING, false)).
      aggregation(CountStar()).
      returns(ValueReturnItem(EntityValue("a")))

    val result = execute(query)

    assertEquals(List(Map("a" -> refNode, "count(*)" -> 2)), result.toList)
  }

  @Test def shouldReturnTwoSubgraphsWithBoundUndirectedRelationship() {
    val a = createNode("a")
    val b = createNode("b")
    relate(a, b, "rel", "r")

    val result = parseAndExecute("start r=rel(0) match a-[r]-b return a,b")

    assertEquals(List(Map("a" -> a, "b" -> b), Map("a" -> b, "b" -> a)), result.toList)
  }

  @Test def shouldAcceptSkipZero() {
    val result = parseAndExecute("start n=node(0) where 1 = 0 return n skip 0")

    assertEquals(List(), result.columnAs[Node]("n").toList)
  }

  @Test def shouldReturnTwoSubgraphsWithBoundUndirectedRelationshipAndOptionalRelationship() {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "rel", "r")
    relate(b, c, "rel", "r2")

    val result = parseAndExecute("start r=rel(0) match a-[r]-b-[?]-c return a,b,c")

    assertEquals(List(Map("a" -> a, "b" -> b, "c" -> c), Map("a" -> b, "b" -> a, "c" -> null)), result.toList)
  }

  @Test def shouldLimitToTwoHits() {
    createNodes("A", "B", "C", "D", "E")

    val query = Query.
      start(NodeById("start", nodeIds: _*)).
      limit(2).
      returns(ValueReturnItem(EntityValue("start")))

    val result = execute(query)

    assertEquals("Result was not trimmed down", 2, result.size)
  }

  @Test def shouldStartTheResultFromSecondRow() {
    val nodes = createNodes("A", "B", "C", "D", "E")

    val query = Query.
      start(NodeById("start", nodeIds: _*)).
      orderBy(SortItem(ValueReturnItem(PropertyValue("start", "name")), true)).
      skip(2).
      returns(ValueReturnItem(EntityValue("start")))

    val result = execute(query)

    assertEquals(nodes.drop(2).toList, result.columnAs[Node]("start").toList)
  }

  @Test def shouldStartTheResultFromSecondRowByParam() {
    val nodes = createNodes("A", "B", "C", "D", "E")

    val query = Query.
      start(NodeById("start", nodeIds: _*)).
      orderBy(SortItem(ValueReturnItem(PropertyValue("start", "name")), true)).
      skip("skippa").
      returns(ValueReturnItem(EntityValue("start")))

    val result = execute(query, "skippa" -> 2)

    assertEquals(nodes.drop(2).toList, result.columnAs[Node]("start").toList)
  }

  @Test def shouldGetStuffInTheMiddle() {
    val nodes = createNodes("A", "B", "C", "D", "E")

    val query = Query.
      start(NodeById("start", nodeIds: _*)).
      orderBy(SortItem(ValueReturnItem(PropertyValue("start", "name")), true)).
      limit(2).
      skip(2).
      returns(ValueReturnItem(EntityValue("start")))

    val result = execute(query)

    assertEquals(nodes.slice(2, 4).toList, result.columnAs[Node]("start").toList)
  }

  @Test def shouldGetStuffInTheMiddleByParam() {
    val nodes = createNodes("A", "B", "C", "D", "E")

    val query = Query.
      start(NodeById("start", nodeIds: _*)).
      orderBy(SortItem(ValueReturnItem(PropertyValue("start", "name")), true)).
      limit("l").
      skip("s").
      returns(ValueReturnItem(EntityValue("start")))

    val result = execute(query, "l" -> 2, "s" -> 2)

    assertEquals(nodes.slice(2, 4).toList, result.columnAs[Node]("start").toList)
  }

  @Test def shouldSortOnAggregatedFunction() {
    val n1 = createNode(Map("name" -> "andres", "divison" -> "Sweden", "age" -> 33))
    val n2 = createNode(Map("name" -> "michael", "divison" -> "Germany", "age" -> 22))
    val n3 = createNode(Map("name" -> "jim", "divison" -> "England", "age" -> 55))
    val n4 = createNode(Map("name" -> "anders", "divison" -> "Sweden", "age" -> 35))

    val query = Query.
      start(NodeById("n", n1.getId, n2.getId, n3.getId, n4.getId)).
      aggregation(ValueAggregationItem(Max(PropertyValue("n", "age")))).
      orderBy(SortItem(ValueAggregationItem(Max(PropertyValue("n", "age"))), true)).
      returns(ValueReturnItem(PropertyValue("n", "divison")))

    val result = execute(query)

    assertEquals(List("Germany", "Sweden", "England"), result.columnAs[String]("n.divison").toList)
  }

  @Test def shouldSortOnAggregatedFunctionAndNormalProperty() {
    val n1 = createNode(Map("name" -> "andres", "division" -> "Sweden"))
    val n2 = createNode(Map("name" -> "michael", "division" -> "Germany"))
    val n3 = createNode(Map("name" -> "jim", "division" -> "England"))
    val n4 = createNode(Map("name" -> "mattias", "division" -> "Sweden"))

    val query = Query.
      start(NodeById("n", n1.getId, n2.getId, n3.getId, n4.getId)).
      aggregation(CountStar()).
      orderBy(SortItem(CountStar(), false), SortItem(ValueReturnItem(PropertyValue("n", "division")), true)).
      returns(ValueReturnItem(PropertyValue("n", "division")))

    val result = execute(query)

    assertEquals(List("Sweden", "England", "Germany"), result.columnAs[String]("n.division").toList)
    assertEquals(List(2, 1, 1), result.columnAs[Int]("count(*)").toList)
  }

  @Test def magicRelTypeWorksAsExpected() {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val query = Query.
      start(NodeById("n", 1)).
      matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
      where(Equals(RelationshipTypeValue(EntityValue("r")), Literal("KNOWS"))).
      returns(ValueReturnItem(EntityValue("x")))

    val result = execute(query)

    assertEquals(List(node("B")), result.columnAs[Node]("x").toList)
  }

  @Test def magicRelTypeOutput() {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val query = Query.
      start(NodeById("n", 1)).
      matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
      returns(ValueReturnItem(RelationshipTypeValue(EntityValue("r"))))

    val result = execute(query)

    assertEquals(List("KNOWS", "HATES"), result.columnAs[String]("TYPE(r)").toList)
  }

  @Test def shouldAggregateOnProperties() {
    val n1 = createNode(Map("x" -> 33))
    val n2 = createNode(Map("x" -> 33))
    val n3 = createNode(Map("x" -> 42))

    val query = Query.
      start(NodeById("node", n1.getId, n2.getId, n3.getId)).
      aggregation(CountStar()).
      returns(ValueReturnItem(PropertyValue("node", "x")))

    val result = execute(query)

    assertThat(result.toList.asJava, hasItems[Map[String, Any]](Map("node.x" -> 33, "count(*)" -> 2), Map("node.x" -> 42, "count(*)" -> 1)))
  }

  @Test def shouldCountNonNullValues() {
    val n1 = createNode(Map("y" -> "a", "x" -> 33))
    val n2 = createNode(Map("y" -> "a"))
    val n3 = createNode(Map("y" -> "b", "x" -> 42))

    val query = Query.
      start(NodeById("node", n1.getId, n2.getId, n3.getId)).
      aggregation(ValueAggregationItem(Count(NullablePropertyValue("node", "x")))).
      returns(ValueReturnItem(PropertyValue("node", "y")))

    val result = execute(query)

    assertThat(result.toList.asJava,
      hasItems[Map[String, Any]](
        Map("node.y" -> "a", "count(node.x)" -> 1),
        Map("node.y" -> "b", "count(node.x)" -> 1)))
  }


  @Test def shouldSumNonNullValues() {
    val n1 = createNode(Map("y" -> "a", "x" -> 33))
    val n2 = createNode(Map("y" -> "a"))
    val n3 = createNode(Map("y" -> "a", "x" -> 42))

    val query = Query.
      start(NodeById("node", n1.getId, n2.getId, n3.getId)).
      aggregation(ValueAggregationItem(Sum(NullablePropertyValue("node", "x")))).
      returns(ValueReturnItem(PropertyValue("node", "y")))

    val result = execute(query)

    assertThat(result.toList.asJava,
      hasItems[Map[String, Any]](Map("node.y" -> "a", "sum(node.x)" -> 75)))
  }

  @Test def shouldWalkAlternativeRelationships() {
    val nodes: List[Node] = createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val query = Query.
      start(NodeById("n", 1)).
      matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
      where(Or(Equals(RelationshipTypeValue(EntityValue("r")), Literal("KNOWS")), Equals(RelationshipTypeValue(EntityValue("r")), Literal("HATES")))).
      returns(ValueReturnItem(EntityValue("x")))

    val result = execute(query)

    assertEquals(nodes.slice(1, 3), result.columnAs[Node]("x").toList)
  }

  @Test def shouldReturnASimplePath() {
    createNodes("A", "B")
    val r = relate("A" -> "KNOWS" -> "B")

    val query = Query.
      start(NodeById("a", 1)).
      namedPaths(NamedPath("p", RelatedTo("a", "b", "rel", None, Direction.OUTGOING, false))).
      returns(ValueReturnItem(EntityValue("p"))) //  new CypherParser().parse("start a=(1) match p=(a-->b) return p")

    val result = execute(query)

    assertEquals(List(PathImpl(node("A"), r, node("B"))), result.columnAs[Path]("p").toList)
  }

  @Test def shouldReturnAThreeNodePath() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")


    val query = Query.
      start(NodeById("a", 1)).
      namedPaths(NamedPath("p",
      RelatedTo("a", "b", "rel1", None, Direction.OUTGOING, false),
      RelatedTo("b", "c", "rel2", None, Direction.OUTGOING, false))).
      returns(ValueReturnItem(EntityValue("p"))) //  new CypherParser().parse("start a=(1) match p=(a-->b) return p")

    val result = execute(query)

    assertEquals(List(PathImpl(node("A"), r1, node("B"), r2, node("C"))), result.columnAs[Path]("p").toList)
  }

  @Test def shouldWalkAlternativeRelationships2() {
    val nodes: List[Node] = createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = parseAndExecute("start n=node(1) match (n)-[r]->(x) where type(r)='KNOWS' or type(r) = 'HATES' return x")

    assertEquals(nodes.slice(1, 3), result.columnAs[Node]("x").toList)
  }

  @Test def shouldNotReturnAnythingBecausePathLengthDoesntMatch() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = parseAndExecute("start n=node(1) match p = n-->x where length(p) = 10 return x")

    assertTrue("Result set should be empty, but it wasn't", result.isEmpty)
  }

  @Ignore
  @Test def statingAPathTwiceShouldNotBeAProblem() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = parseAndExecute("start n=node(1) match x<--n, p = n-->x return p")

    assertEquals(1, result.toSeq.length)
  }

  @Test def shouldPassThePathLengthTest() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = parseAndExecute("start n=node(1) match p = n-->x where length(p)=1 return x")

    assertTrue("Result set should not be empty, but it was", !result.isEmpty)
  }

  @Test def shouldReturnPathLength() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = parseAndExecute("start n=node(1) match p = n-->x return length(p)")

    assertEquals(List(1), result.columnAs[Int]("LENGTH(p)").toList)
  }

  @Test def shouldBeAbleToFilterOnPathNodes() {
    val a = createNode(Map("foo" -> "bar"))
    val b = createNode(Map("foo" -> "bar"))
    val c = createNode(Map("foo" -> "bar"))
    val d = createNode(Map("foo" -> "bar"))

    relate(a, b, "rel")
    relate(b, c, "rel")
    relate(c, d, "rel")

    val query = Query.start(NodeById("pA", a.getId), NodeById("pB", d.getId)).
      namedPaths(NamedPath("p", VarLengthRelatedTo("x", "pA", "pB", Some(1), Some(5), "rel", Direction.OUTGOING))).
      where(AllInSeq(PathNodesValue(EntityValue("p")), "i", Equals(PropertyValue("i", "foo"), Literal("bar")))).
      returns(ValueReturnItem(EntityValue("pB")))

    val result = execute(query)

    assertEquals(List(d), result.columnAs[Node]("pB").toList)
  }

  @Test def shouldReturnRelationships() {
    val a = createNode(Map("foo" -> "bar"))
    val b = createNode(Map("foo" -> "bar"))
    val c = createNode(Map("foo" -> "bar"))

    val r1 = relate(a, b, "rel")
    val r2 = relate(b, c, "rel")

    val query = Query.start(NodeById("pA", a.getId)).
      namedPaths(NamedPath("p", VarLengthRelatedTo("x", "pA", "pB", Some(2), Some(2), "rel", Direction.OUTGOING))).
      returns(ValueReturnItem(PathRelationshipsValue(EntityValue("p"))))

    val result = execute(query)

    assertEquals(List(r1, r2), result.columnAs[Node]("RELATIONSHIPS(p)").toList.head)
  }

  @Test def shouldReturnAVarLengthPath() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = parseAndExecute("start n=node(1) match p=n-[:KNOWS*1..2]->x return p")

    assertEquals(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ), result.columnAs[Path]("p").toList)
  }

  @Test def aVarLengthPathOfLengthZero() {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("B" -> "KNOWS" -> "C")

    val result = parseAndExecute("start a=node(1) match a-[*0..1]->b return a,b")

    assertEquals(
      Set(
        Map("a" -> node("A"), "b" -> node("B")),
        Map("a" -> node("A"), "b" -> node("A"))),
      result.toSet)
  }

  @Test def aNamedVarLengthPathOfLengthZero() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "FRIEND" -> "C")

    val result = parseAndExecute("start a=node(1) match p=a-[:KNOWS*0..1]->b-[:FRIEND*0..1]->c return p,a,b,c")

    assertEquals(
      Set(
        PathImpl(node("A")),
        PathImpl(node("A"), r1, node("B")),
        PathImpl(node("A"), r1, node("B"), r2, node("C"))
      ),
      result.columnAs[Path]("p").toSet)
  }

  @Test def testZeroLengthVarLenPathInTheMiddle() {
    createNodes("A", "B", "C", "D", "E")
    relate("A" -> "CONTAINS" -> "B")
    relate("B" -> "FRIEND" -> "C")


    val result = parseAndExecute("start a=node(1) match a-[:CONTAINS*0..1]->b-[:FRIEND*0..1]->c return a,b,c")

    assertEquals(
      Set(
        Map("a" -> node("A"), "b" -> node("A"), "c" -> node("A")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("B")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))),
      result.toSet)
  }

  @Test def shouldReturnAVarLengthPathWithoutMinimalLength() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = parseAndExecute("start n=node(1) match p=n-[:KNOWS*..2]->x return p")

    assertEquals(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ), result.columnAs[Path]("p").toList)
  }

  @Test def shouldReturnAVarLengthPathWithUnboundMax() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = parseAndExecute("start n=node(1) match p=n-[:KNOWS*..]->x return p")

    assertEquals(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ), result.columnAs[Path]("p").toList)
  }


  @Test def shouldHandleBoundNodesNotPartOfThePattern() {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")

    val result = parseAndExecute("start a=node(1), c = node(3) match a-->b return a,b,c").toList

    assert(List(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))) === result)
  }

  @Test def shouldReturnShortestPath() {
    createNodes("A", "B")
    val r1 = relate("A" -> "KNOWS" -> "B")

    val query = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      namedPaths(NamedPath("p", ShortestPath("  UNNAMED1", "a", "b", None, Direction.BOTH, Some(15), false))).
      returns(ValueReturnItem(EntityValue("p")))

    val result = execute(query).toList.head("p").asInstanceOf[Path]

    val number_of_relationships_in_path = result.length()
    assert(number_of_relationships_in_path === 1)
    assert(result.startNode() === node("A"))
    assert(result.endNode() === node("B"))
    assert(result.lastRelationship() === r1)
  }

  @Test def shouldReturnShortestPathUnboundLength() {
    createNodes("A", "B")
    val r1 = relate("A" -> "KNOWS" -> "B")

    val query = Query.
      start(NodeById("a", 1), NodeById("b", 2)).
      namedPaths(NamedPath("p", ShortestPath("  UNNAMED1", "a", "b", None, Direction.BOTH, None, false))).
      returns(ValueReturnItem(EntityValue("p")))

    //Checking that we don't get an exception
    execute(query).toList
  }

  @Test def shouldBeAbleToTakeParamsInDifferentTypes() {
    createNodes("A", "B", "C", "D", "E")

    val query = Query.
      start(
      NodeById("pA", ParameterValue("a")),
      NodeById("pB", ParameterValue("b")),
      NodeById("pC", ParameterValue("c")),
      NodeById("pD", ParameterValue("0")),
      NodeById("pE", ParameterValue("1"))).
      returns(ValueReturnItem(EntityValue("pA")), ValueReturnItem(EntityValue("pB")), ValueReturnItem(EntityValue("pC")), ValueReturnItem(EntityValue("pD")), ValueReturnItem(EntityValue("pE")))

    val result = execute(query,
      "a" -> Seq[Long](1),
      "b" -> 2,
      "c" -> Seq(3L).asJava,
      "0" -> Seq(4).asJava,
      "1" -> List(5)
    )

    assertEquals(1, result.toList.size)
  }

  @Test(expected = classOf[ParameterWrongTypeException]) def parameterTypeErrorShouldBeNicelyExplained() {
    createNodes("A")

    val query = Query.
      start(NodeById("pA", ParameterValue("a"))).
      returns(ValueReturnItem(EntityValue("pA")))

    execute(query, "a" -> "Andres").toList
  }

  @Test def shouldBeAbleToTakeParamsFromParsedStuff() {
    createNodes("A")

    val query = new CypherParser().parse("start pA = node({a}) return pA")
    val result = execute(query, "a" -> Seq[Long](1))

    assertEquals(List(Map("pA" -> node("A"))), result.toList)
  }

  @Test def shouldBeAbleToTakeParamsForEqualityComparisons() {
    createNode(Map("name" -> "Andres"))

    val query = Query.
      start(NodeById("a", 1)).
      where(Equals(PropertyValue("a", "name"), ParameterValue("name")))
      .returns(ValueReturnItem(EntityValue("a")))

    assert(0 === execute(query, "name" -> "Tobias").toList.size)
    assert(1 === execute(query, "name" -> "Andres").toList.size)
  }

  @Test def shouldHandlePatternMatchingWithParameters() {
    val a = createNode()
    val b = createNode(Map("name" -> "you"))
    relate(a, b, "KNOW")

    val result = parseAndExecute("start x  = node({startId}) match x-[r]-friend where friend.name = {name} return TYPE(r)", "startId" -> 1, "name" -> "you")

    assert(List(Map("TYPE(r)" -> "KNOW")) === result.toList)
  }

  @Test def twoBoundNodesPointingToOne() {
    val a = createNode("A")
    val b = createNode("B")
    val x1 = createNode("x1")
    val x2 = createNode("x2")

    relate(a, x1, "REL", "AX1")
    relate(a, x2, "REL", "AX2")

    relate(b, x1, "REL", "BX1")
    relate(b, x2, "REL", "BX2")

    val result = parseAndExecute("""
start a  = node({A}), b = node({B})
match a-[rA]->x<-[rB]->b
return x""", "A" -> 1, "B" -> 2)

    assert(List(x1, x2) === result.columnAs[Node]("x").toList)
  }


  @Test def threeBoundNodesPointingToOne() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val x1 = createNode("x1")
    val x2 = createNode("x2")

    relate(a, x1, "REL", "AX1")
    relate(a, x2, "REL", "AX2")

    relate(b, x1, "REL", "BX1")
    relate(b, x2, "REL", "BX2")

    relate(c, x1, "REL", "CX1")
    relate(c, x2, "REL", "CX2")

    val result = parseAndExecute("""
start a  = node({A}), b = node({B}), c = node({C})
match a-[rA]->x, b-[rB]->x, c-[rC]->x
return x""", "A" -> 1, "B" -> 2, "C" -> 3)

    assert(List(x1, x2) === result.columnAs[Node]("x").toList)
  }

  @Test def threeBoundNodesPointingToOneWithABunchOfExtraConnections() {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    val e = createNode("e")
    val f = createNode("f")
    val g = createNode("g")
    val h = createNode("h")
    val i = createNode("i")
    val j = createNode("j")
    val k = createNode("k")

    relate(a, d)
    relate(a, e)
    relate(a, f)
    relate(a, g)
    relate(a, i)

    relate(b, d)
    relate(b, e)
    relate(b, f)
    relate(b, h)
    relate(b, k)

    relate(c, d)
    relate(c, e)
    relate(c, h)
    relate(c, g)
    relate(c, j)

    val result = parseAndExecute("""
start a  = node({A}), b = node({B}), c = node({C})
match a-->x, b-->x, c-->x
return x""", "A" -> 1, "B" -> 2, "C" -> 3)

    assert(List(d, e) === result.columnAs[Node]("x").toList)
  }

  @Test def shouldHandleCollaborativeFiltering() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val d = createNode("D")
    val e = createNode("E")
    val f = createNode("F")

    relate(a, b, "knows", "rAB")
    relate(a, c, "knows", "rAC")
    relate(a, f, "knows", "rAF")

    relate(b, c, "knows", "rBC")
    relate(b, d, "knows", "rBD")
    relate(b, e, "knows", "rBE")

    relate(c, e, "knows", "rCE")

    val result = parseAndExecute("""
start a  = node(1)
match a-[r1:knows]->friend-[r2:knows]->foaf, a-[foafR?:knows]->foaf
where foafR is null
return foaf, count(*)
order by count(*)""")

    assert(List(Map("foaf" -> d, "count(*)" -> 1), Map("foaf" -> e, "count(*)" -> 2)) === result.toList)
  }

  @Test def shouldSplitOptionalMandatoryCleverly() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")

    relate(a, b, "knows", "rAB")
    relate(b, c, "knows", "rBC")

    val result = parseAndExecute("""
start a  = node(1)
match a-[r1?:knows]->friend-[r2:knows]->foaf
return foaf""")

    assert(List(Map("foaf" -> c)) === result.toList)
  }

  @Test(expected = classOf[ParameterNotFoundException]) def shouldComplainWhenMissingParams() {
    val query = Query.
      start(NodeById("pA", ParameterValue("a"))).
      returns(ValueReturnItem(EntityValue("pA")))

    execute(query).toList
  }

  @Test def shouldSupportSortAndDistinct() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")

    val result = parseAndExecute("""
start a  = node(1,2,3,1)
return distinct a
order by a.name
""")

    assert(List(a,b,c) === result.columnAs[Node]("a") .toList)
  }

  @Test def shouldHandleAggregationOnFunctions() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    relate(a,b,"X")
    relate(a,c,"X")

    val result = parseAndExecute("""
start a  = node(1)
match p = a -[*]-> b
return b, avg(length(p))
""")

    assert(List(b,c) === result.columnAs[Node]("b") .toList)
  }

  @Test def shouldHandleOptionalPaths() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = parseAndExecute("""
start a  = node(1), x = node(2,3)
match p = a -[?]-> x
return x, p
""")

    assert(List(
      Map("x"->b, "p"->PathImpl(a,r,b)),
      Map("x"->c, "p"->null)
    ) === result.toList)
  }

  @Test def shouldHandleOptionalPathsFromGraphAlgo() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = parseAndExecute("""
start a  = node(1), x = node(2,3)
match p = shortestPath(a -[?*]-> x)
return x, p
""")

    assert(List(
      Map("x"->b, "p"->PathImpl(a,r,b)),
      Map("x"->c, "p"->null)
    ) === result.toList)
  }

  @Test def shouldHandleOptionalPathsFromACombo() {
    val a = createNode("A")
    val b = createNode("B")
    val r = relate(a, b, "X")

    val result = parseAndExecute("""
start a  = node(1)
match p = a-->b-[?*]->c
return p
""")

    assert(List(
      Map("p"->null)
    ) === result.toList)
  }

  @Test def shouldHandleOptionalPathsFromVarLengthPath() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = parseAndExecute("""
start a  = node(1), x = node(2,3)
match p = a -[?*]-> x
return x, p
""")

    assert(List(
      Map("x"->b, "p"->PathImpl(a,r,b)),
      Map("x"->c, "p"->null)
    ) === result.toList)
  }

  @Ignore("This test exposes a bug")
  @Test def shouldSupportMultipleRegexes() {
    val a = createNode(Map("name"->"Andreas"))


    val result = parseAndExecute("""
start a  = node(1)
where a.name =~ /And.*/ AND a.name =~ /And.*/
return a
""")

    assert(List(a) === result.columnAs[Node]("a") .toList)
  }


  @Test(expected = classOf[SyntaxException]) def shouldNotSupportSortingOnThingsAfterDistinctHasRemovedIt() {
    val a = createNode("name"->"A", "age"->13)
    val b = createNode("name"->"B", "age"->12)
    val c = createNode("name"->"C", "age"->11)

    val result = parseAndExecute("""
start a  = node(1,2,3,1)
return distinct a.name
order by a.age
""")

    assert(List(a,b,c) === result.columnAs[Node]("a") .toList)
  }

  @Test def shouldThrowNiceErrorMessageWhenPropertyIsMissing() {
    val query = new CypherParser().parse("start n=node(0) return n.A_PROPERTY_THAT_IS_MISSING")
    try {
      execute(query).toList
    } catch {
      case x: SyntaxException => assertEquals("n.A_PROPERTY_THAT_IS_MISSING does not exist on Node[0]", x.getMessage)
    }
  }
}

