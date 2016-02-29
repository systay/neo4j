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
package org.neo4j.cypher

import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.api.exceptions.Status

class CypherCompatibilityTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport {

  val QUERY = "MATCH (n:Label) RETURN n"

  test("should match paths correctly with rule planner in 2.3") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH (n)-[r:T]->(m) RETURN count(*)"

    execute(s"CYPHER 2.3 planner=rule $query").columnAs[Long]("count(*)").next() shouldBe 1
    execute(s"CYPHER 2.3 $query").columnAs[Long]("count(*)").next() shouldBe 1
    execute(s"CYPHER 3.0 planner=rule $query").columnAs[Long]("count(*)").next() shouldBe 1
    execute(s"CYPHER 3.0 $query").columnAs[Long]("count(*)").next() shouldBe 1
  }

  test("should be able to switch between versions") {
    runWithConfig() {
      engine =>
        engine.execute(s"CYPHER 2.3 $QUERY", Map.empty[String, Object], graph.session()).toList shouldBe empty
        engine.execute(s"CYPHER 3.0 $QUERY", Map.empty[String, Object], graph.session()).toList shouldBe empty
    }
  }

  test("should be able to switch between versions2") {
    runWithConfig() {
      engine =>
        engine.execute(s"CYPHER 3.0 $QUERY", Map.empty[String, Object], graph.session()).toList shouldBe empty

        engine.execute(s"CYPHER 2.3 $QUERY", Map.empty[String, Object], graph.session()).toList shouldBe empty
    }
  }

  test("should be able to override config") {
    runWithConfig(GraphDatabaseSettings.cypher_parser_version -> "2.3") {
      engine =>
        engine.execute(s"CYPHER 3.0 $QUERY", Map.empty[String, Object], graph.session()).toList shouldBe empty
    }
  }

  test("should be able to override config2") {
    runWithConfig(GraphDatabaseSettings.cypher_parser_version -> "3.0") {
      engine =>
        engine.execute(s"CYPHER 2.3 $QUERY", Map.empty[String, Object], graph.session()).toList shouldBe empty
    }
  }

  test("should use default version by default") {
    runWithConfig() {
      engine =>
        val result = engine.execute(QUERY, Map.empty[String, Object], graph.session())
        result shouldBe empty
        result.executionPlanDescription().arguments.get("version") should equal(Some("CYPHER 3.0"))
    }
  }

  //TODO fix this test
  ignore("should handle profile in compiled runtime") {
    runWithConfig() {
      engine =>
        assertProfiled(engine, "CYPHER 2.3 runtime=compiled PROFILE MATCH (n) RETURN n")
        assertProfiled(engine, "CYPHER 3.0 runtime=compiled PROFILE MATCH (n) RETURN n")
    }
  }

  test("should handle profile in interpreted runtime") {
    runWithConfig() {
      engine =>
        assertProfiled(engine, "CYPHER 2.3 runtime=interpreted PROFILE MATCH (n) RETURN n")
        assertProfiled(engine, "CYPHER 3.0 runtime=interpreted PROFILE MATCH (n) RETURN n")
    }
  }

  test("should allow the use of explain in the supported compilers") {
    runWithConfig() {
      engine =>
        assertExplained(engine, "CYPHER 2.3 EXPLAIN MATCH (n) RETURN n")
        assertExplained(engine, "CYPHER 3.0 EXPLAIN MATCH (n) RETURN n")
    }
  }

  private val queryThatCannotRunWithCostPlanner = "MATCH (a), (b) CREATE UNIQUE (a)-[r:X]->(b)"

  private val querySupportedByCostButNotCompiledRuntime = "MATCH (n:Movie)--(b), (a:A)--(c:C)--(d:D) RETURN count(*)"

  test("should not fail if cypher allowed to choose planner or we specify RULE for update query") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      engine =>
        engine.execute(queryThatCannotRunWithCostPlanner, Map.empty[String, Object], graph.session())
        engine.execute(s"CYPHER planner=RULE $queryThatCannotRunWithCostPlanner", Map.empty[String, Object], graph.session())
        shouldHaveNoWarnings(
          engine.execute(s"EXPLAIN CYPHER planner=RULE $queryThatCannotRunWithCostPlanner", Map.empty[String, Object], graph.session())
        )
    }
  }

  test("should fail if asked to execute query with COST instead of falling back to RULE if hint errors turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      engine =>
        intercept[InvalidArgumentException](
          engine.execute(s"EXPLAIN CYPHER planner=COST $queryThatCannotRunWithCostPlanner", Map.empty[String, Object], graph.session())
        )
    }
  }

  test("should not fail if asked to execute query with COST and instead fallback to RULE and return a warning if hint errors turned off") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "false") {
      engine =>
        shouldHaveWarning(
          engine.execute(s"EXPLAIN CYPHER planner=COST $queryThatCannotRunWithCostPlanner", Map.empty[String, Object], graph.session()),
          Status.Statement.PlannerUnsupportedWarning)
    }
  }

  test("should not fail if asked to execute query with COST and instead fallback to RULE and return a warning by default") {
    runWithConfig() {
      engine =>
        shouldHaveWarning(
          engine.execute(s"EXPLAIN CYPHER planner=COST $queryThatCannotRunWithCostPlanner", Map.empty[String, Object], graph.session()),
          Status.Statement.PlannerUnsupportedWarning)
    }
  }

  test("should not fail if asked to execute query with runtime=compiled on simple query") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      engine =>
        engine.execute("MATCH (n:Movie) RETURN n", Map.empty[String, Object], graph.session())
        engine.execute("CYPHER runtime=compiled MATCH (n:Movie) RETURN n", Map.empty[String, Object], graph.session())
        shouldHaveNoWarnings(
          engine.execute("EXPLAIN CYPHER runtime=compiled MATCH (n:Movie) RETURN n", Map.empty[String, Object], graph.session())
        )
    }
  }

  test("should fail if asked to execute query with runtime=compiled instead of falling back to interpreted if hint errors turned on") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "true") {
      engine =>
        intercept[InvalidArgumentException](
          engine.execute(s"EXPLAIN CYPHER runtime=compiled $querySupportedByCostButNotCompiledRuntime", Map.empty[String, Object], graph.session())
        )
    }
  }

  test("should not fail if asked to execute query with runtime=compiled and instead fallback to interpreted and return a warning if hint errors turned off") {
    runWithConfig(GraphDatabaseSettings.cypher_hints_error -> "false") {
      engine =>
        shouldHaveWarning(
          engine.execute(s"EXPLAIN CYPHER runtime=compiled $querySupportedByCostButNotCompiledRuntime", Map.empty[String, Object], graph.session()),
          Status.Statement.RuntimeUnsupportedWarning)
    }
  }

  test("should not fail if asked to execute query with runtime=compiled and instead fallback to interpreted and return a warning by default") {
    runWithConfig() {
      engine =>
        shouldHaveWarning(
          engine.execute(s"EXPLAIN CYPHER runtime=compiled $querySupportedByCostButNotCompiledRuntime", Map.empty[String, Object], graph.session()),
          Status.Statement.RuntimeUnsupportedWarning)
    }
  }

  test("should not fail nor generate a warning if asked to execute query without specifying runtime, knowing that compiled is default but will fallback silently to interpreted") {
    runWithConfig() {
      engine =>
        shouldHaveNoWarnings(
          engine.execute(s"EXPLAIN $querySupportedByCostButNotCompiledRuntime", Map.empty[String, Object], graph.session())
        )
    }
  }

  test("should not support old 2.0 and 2.1 compilers") {
    runWithConfig() {
      engine =>
        intercept[SyntaxException](
          engine.execute("CYPHER 1.9 MATCH (n) RETURN n", Map.empty[String, Object], graph.session())
        )
        intercept[SyntaxException](
          engine.execute("CYPHER 2.0 MATCH (n) RETURN n", Map.empty[String, Object], graph.session())
        )
        intercept[SyntaxException](
          engine.execute("CYPHER 2.1 MATCH (n) RETURN n", Map.empty[String, Object], graph.session())
        )
        intercept[SyntaxException](
          engine.execute("CYPHER 2.2 MATCH (n) RETURN n", Map.empty[String, Object], graph.session())
        )
    }
  }

  private def assertProfiled(engine: ExecutionEngine, q: String) {
    val result = engine.execute(q, Map.empty[String, Object], graph.session())
    result.toList
    assert(result.executionPlanDescription().asJava.hasProfilerStatistics, s"$q was not profiled as expected")
    assert(result.planDescriptionRequested, s"$q was not flagged for planDescription")
  }

  private def assertExplained(engine: ExecutionEngine, q: String) {
    val result = engine.execute(q, Map.empty[String, Object], graph.session())
    result.toList
    assert(!result.executionPlanDescription().asJava.hasProfilerStatistics, s"$q was not profiled as expected")
    assert(result.planDescriptionRequested, s"$q was not flagged for planDescription")
  }
}
