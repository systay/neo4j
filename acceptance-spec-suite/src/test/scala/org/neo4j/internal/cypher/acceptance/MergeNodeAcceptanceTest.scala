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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, MergeConstraintConflictException, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyConstraintViolationKernelException

class MergeNodeAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  // TCK'd
  test("merge node when no nodes exist") {
    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a) return count(*) as n")

    // Then
    val createdNodes = result.columnAs[Int]("n").toList

    createdNodes should equal(List(1))
    assertStats(result, nodesCreated = 1)
  }

  // TCK'd
  test("merge node with label") {
    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) return labels(a)")

    result.toList should equal(List(Map("labels(a)" -> List("Label"))))
    assertStats(result, nodesCreated = 1, labelsAdded = 1)
  }

  // TCK'd
  test("merge node with label add label on create") {
    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on create set a:Foo return labels(a)")

    // Then

    result.toList should equal(List(Map("labels(a)" -> List("Label", "Foo"))))
    assertStats(result, nodesCreated = 1, labelsAdded = 2)
  }

  // TCK'd
  test("merge node with label add property on update") {
    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on create set a.prop = 42 return a.prop")

    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  // TCK'd
  test("merge node with label when it exists") {
    // Given
    val existingNode = createLabeledNode("Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) return id(a)")

    // Then
    val createdNodes = result.columnAs[Long]("id(a)").toList

    createdNodes should equal(List(existingNode.getId))
    assertStats(result, nodesCreated = 0)
  }

  // TCK'd
  test("merge node with property when it exists") {
    // Given
    createNode("prop" -> 42)

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a {prop: 42}) return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, nodesCreated = 0)
  }

  // TCK'd
  test("merge node should create when it doesn't match") {
    // Given
    createNode("prop" -> 42)

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a {prop: 43}) return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 43)))
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
  }

  // TCK'd
  test("merge node with prop and label") {
    // Given
    createLabeledNode(Map("prop" -> 42), "Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label {prop: 42}) return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, nodesCreated = 0)
  }

  // TODO: Reflect something like this in the TCK
  test("multiple merges after each other") {
    1 to 100 foreach { prop =>
      val result = updateWithBothPlannersAndCompatibilityMode(s"merge (a:Label {prop: $prop}) return a.prop")
      assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
    }
  }

  // TCK'd
  test("merge node with label add label on match when it exists") {
    // Given
    createLabeledNode("Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on match set a:Foo return labels(a)")

    // Then
    result.toList should equal(List(Map("labels(a)" -> List("Label", "Foo"))))
    assertStats(result, nodesCreated = 0, labelsAdded = 1)
  }

  // TCK'd
  test("merge node with label add property on update when it exists") {
    // Given
    createLabeledNode("Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on create set a.prop = 42 return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> null)))
    assertStats(result, nodesCreated = 0)
  }

  // TCK'd
  test("merge node and set property on match") {
    // Given
    createLabeledNode("Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label) on match set a.prop = 42 return a.prop")

    // Then
    result.toList should equal(List(Map("a.prop" -> 42)))
    assertStats(result, propertiesWritten = 1)
  }

  // TCK'd
  test("merge node should create a node with given properties when no match is found") {
    // Given - a node that does not match
    val other = createLabeledNode(Map("prop" -> 666), "Label")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("merge (a:Label {prop:42}) return a.prop")

    // Then
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
    val props = result.columnAs[Node]("a.prop").toList
    props should equal(List(42))
  }

  // TCK'd
  test("works fine with index") {
    // given
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(name)")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {name:'Lasse'}) RETURN person.name")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  // TCK'd
  test("works with indexed and unindexed property") {
    // given
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(name)")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {name:'Lasse', id:42}) RETURN person.name")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 2)
  }

  // TCK'd
  test("works with two indexed properties") {
    // given
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(name)")
    updateWithBothPlannersAndCompatibilityMode("create index on :Person(id)")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (person:Person {name:'Lasse', id:42}) RETURN person.name")

    // then does not throw
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 2)
  }

  // TCK'd
  test("should work when finding multiple elements") {
    assertStats(updateWithBothPlannersAndCompatibilityMode( "CREATE (:X) CREATE (:X) MERGE (:X)"), nodesCreated = 2, labelsAdded = 2)
  }

  // TCK'd
  test("merge should handle argument properly") {
    createNode("x" -> 42)
    createNode("x" -> "not42")

    val query = "WITH 42 AS x MERGE (c:N {x: x})"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  // TCK'd
  test("merge should handle arguments properly with only write clauses") {
    val query = "CREATE (a {p: 1}) MERGE (b {v: a.p})"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, propertiesWritten = 2)
  }

  // TCK'd
  test("should be able to merge using property from match") {
    createLabeledNode(Map("name" -> "A", "bornIn" -> "New York"), "Person")
    createLabeledNode(Map("name" -> "B", "bornIn" -> "Ohio"), "Person")
    createLabeledNode(Map("name" -> "C", "bornIn" -> "New Jersey"), "Person")
    createLabeledNode(Map("name" -> "D", "bornIn" -> "New York"), "Person")
    createLabeledNode(Map("name" -> "E", "bornIn" -> "Ohio"), "Person")
    createLabeledNode(Map("name" -> "F", "bornIn" -> "New Jersey"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City {name: person.bornIn}) RETURN person.name"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 3, propertiesWritten = 3, labelsAdded = 3)
  }

  // TCK'd
  test("should be able to merge using property from match with index") {
    graph.createIndex("City", "name")

    createLabeledNode(Map("name" -> "A", "bornIn" -> "New York"), "Person")
    createLabeledNode(Map("name" -> "B", "bornIn" -> "Ohio"), "Person")
    createLabeledNode(Map("name" -> "C", "bornIn" -> "New Jersey"), "Person")
    createLabeledNode(Map("name" -> "D", "bornIn" -> "New York"), "Person")
    createLabeledNode(Map("name" -> "E", "bornIn" -> "Ohio"), "Person")
    createLabeledNode(Map("name" -> "F", "bornIn" -> "New Jersey"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City {name: person.bornIn}) RETURN person.name"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 3, propertiesWritten = 3, labelsAdded = 3)
  }

  // TCK'd
  test("should be able to use properties from match in ON CREATE") {
    createLabeledNode(Map("bornIn" -> "New York"), "Person")
    createLabeledNode(Map("bornIn" -> "Ohio"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City) ON CREATE SET city.name = person.bornIn RETURN person.bornIn"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
  }

  // TCK'd
  test("should be able to use properties from match in ON MATCH") {
    createLabeledNode(Map("bornIn" -> "New York"), "Person")
    createLabeledNode(Map("bornIn" -> "Ohio"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City) ON MATCH SET city.name = person.bornIn RETURN person.bornIn"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
  }

  // TCK'd
  test("should be able to use properties from match in ON MATCH and ON CREATE") {
    createLabeledNode(Map("bornIn" -> "New York"), "Person")
    createLabeledNode(Map("bornIn" -> "Ohio"), "Person")

    val query = "MATCH (person:Person) MERGE (city: City) ON MATCH SET city.name = person.bornIn ON CREATE SET city.name = person.bornIn RETURN person.bornIn"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, nodesCreated = 1, propertiesWritten = 2, labelsAdded = 1)
    executeWithAllPlannersAndCompatibilityMode("MATCH (n:City) WHERE NOT exists(n.name) RETURN n").toList shouldBe empty
  }

  // TCK'd
  test("should be able to set labels on match") {
    createNode()

    val query = "MERGE (a) ON MATCH SET a:L"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, labelsAdded = 1)
  }

  // TCK'd
  test("should be able to set labels on match and on create") {
    createNode()
    createNode()

    val query = "MATCH () MERGE (a:L) ON MATCH SET a:M1 ON CREATE SET a:M2"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, nodesCreated=1, labelsAdded = 3)
    executeScalarWithAllPlannersAndCompatibilityMode[Int]("MATCH (a:L) RETURN count(a)") should equal(1)
    executeScalarWithAllPlannersAndCompatibilityMode[Int]("MATCH (a:M1) RETURN count(a)") should equal(1)
    executeScalarWithAllPlannersAndCompatibilityMode[Int]("MATCH (a:M2) RETURN count(a)") should equal(1)
  }

  // TCK'd
  test("should support updates while merging") {
    (0 until 3) foreach(x =>
      (0 until 3) foreach( y=>
        createNode("x"->x, "y"->y)
        ))

    // when
    val result = updateWithBothPlannersAndCompatibilityMode(
      "MATCH (foo) WITH foo.x AS x, foo.y AS y " +
        "MERGE (c:N {x: x, y: y+1}) " +
        "MERGE (a:N {x: x, y: y}) " +
        "MERGE (b:N {x: x+1, y: y})  " +
        "RETURN x, y;")

    assertStats(result, nodesCreated = 15, labelsAdded = 15, propertiesWritten = 30)
    result.toList should equal(List(Map("x" -> 0, "y" -> 0), Map("x" -> 0, "y" -> 1),
                                    Map("x" -> 0, "y" -> 2), Map("x" -> 1, "y" -> 0),
                                    Map("x" -> 1, "y" -> 1), Map("x" -> 1, "y" -> 2),
                                    Map("x" -> 2, "y" -> 0), Map("x" -> 2, "y" -> 1),
                                    Map("x" -> 2, "y" -> 2)))
  }

  // TCK'd
  test("merge must properly handle multiple labels") {
    createLabeledNode(Map("prop" -> 42), "L", "A")

    val result = updateWithBothPlannersAndCompatibilityMode("merge (test:L:B {prop : 42}) return labels(test) as labels")

    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 2)
    result.toList should equal(List(Map("labels" -> List("L", "B"))))
  }

  // TCK'd
  test("merge with an index must properly handle multiple labels") {
    graph.createIndex("L", "prop")
    createLabeledNode(Map("prop" -> 42), "L", "A")

    val result = updateWithBothPlannersAndCompatibilityMode("merge (test:L:B {prop : 42}) return labels(test) as labels")

    assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 2)
    result.toList should equal(List(Map("labels" -> List("L", "B"))))
  }

  // TCK'd
  test("merge followed by multiple creates") {
    val query =
      """MERGE (t:T {id:42})
        |CREATE (f:R)
        |CREATE (t)-[:REL]->(f)
      """.stripMargin

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, labelsAdded = 2, relationshipsCreated = 1, propertiesWritten = 1)
  }

  // TCK'd
  test("unwind combined with merge") {
    val query = "UNWIND [1,2,3,4] AS int MERGE (n {id: int}) RETURN count(*)"
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 4, propertiesWritten = 4)
    result.toList should equal(List(Map("count(*)" -> 4)))
  }

  // TCK'd
  test("merges should not be able to match on deleted nodes") {
    // GIVEN
    val node1 = createLabeledNode(Map("value" -> 1), "A")
    val node2 = createLabeledNode(Map("value" -> 2), "A")

    val query = """
                  |MATCH (a:A)
                  |DELETE a
                  |MERGE (a2:A)
                  |RETURN a2.value
                """.stripMargin

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    // THEN
    result.toList should equal(List(Map("a2.value" -> null), Map("a2.value" -> null)))
    assertStats(result, nodesCreated = 1, nodesDeleted = 2, labelsAdded = 1)
  }
}
