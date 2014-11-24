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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.Normal
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.ExecutionPlan
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.impl.util.StringLogger.DEV_NULL
import org.neo4j.kernel.impl.util.TestLogger.LogCall
import org.neo4j.kernel.impl.util.{StringLogger, TestLogger}

import scala.collection.Map

class CypherCompilerAstCacheAcceptanceTest extends CypherFunSuite with GraphDatabaseTestSupport {
  def createCompiler(queryCacheSize: Int = 128, queryPlanTTL: Long = 1000, logger: StringLogger = DEV_NULL) =
    CypherCompilerFactory.ronjaCompiler(graph, queryCacheSize, queryPlanTTL, kernelMonitors, logger)

  case class CacheCounts(hits: Int = 0, misses: Int = 0, flushes: Int = 0, evicted: Int = 0) {
    override def toString = s"hits = $hits, misses = $misses, flushes = $flushes, evicted = $evicted"
  }

  class CacheCounter(var counts: CacheCounts = CacheCounts()) extends AstCacheMonitor {
    override def cacheHit(key: PreparedQuery) {
      counts = counts.copy(hits = counts.hits + 1)
    }

    override def cacheMiss(key: PreparedQuery) {
      counts = counts.copy(misses = counts.misses + 1)
    }

    override def cacheFlushDetected(justBeforeKey: CacheAccessor[PreparedQuery, ExecutionPlan]) {
      counts = counts.copy(flushes = counts.flushes + 1)
    }

    override def cacheDiscard(key: PreparedQuery): Unit = {
      counts = counts.copy(evicted = counts.evicted + 1)
    }
  }

  override def databaseConfig(): Map[String,String] = Map(GraphDatabaseSettings.query_plan_ttl.name() -> "0")

  test("should monitor cache misses") {
    val counter = new CacheCounter()
    val compiler = createCompiler()
    compiler.monitors.addMonitorListener(counter)

    graph.inTx { compiler.planQuery("return 42", planContext, Normal) }

    counter.counts should equal(CacheCounts(hits = 0, misses = 1, flushes = 1))
  }

  test("should monitor cache hits") {
    val compiler = createCompiler()
    val counter = new CacheCounter()
    compiler.monitors.addMonitorListener(counter)

    graph.inTx { compiler.planQuery("return 42", planContext, Normal) }
    graph.inTx { compiler.planQuery("return 42", planContext, Normal) }

    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("should monitor cache flushes") {
    val compiler = createCompiler()
    val counter = new CacheCounter()
    compiler.monitors.addMonitorListener(counter)

    graph.inTx { compiler.planQuery("return 42", planContext, Normal) }
    graph.createConstraint("Person", "id")
    graph.inTx { compiler.planQuery("return 42", planContext, Normal) }

    counter.counts should equal(CacheCounts(hits = 0, misses = 2, flushes = 2))
  }

  test("should monitor cache remove") {
    // given
    val counter = new CacheCounter()
    val compiler = createCompiler()
    compiler.monitors.addMonitorListener(counter)
    val query: String = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    graph.inTx { compiler.planQuery(query, planContext, Normal) }

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    graph.inTx { compiler.planQuery(query, planContext, Normal) }

    // then
    counter.counts should equal(CacheCounts(hits = 1, misses = 2, flushes = 1, evicted = 1))
  }

  test("should log on cache remove") {
    // given
    val counter = new CacheCounter()
    val logger: TestLogger = new TestLogger()
    val compiler = createCompiler(queryPlanTTL = 0, logger = logger)
    compiler.monitors.addMonitorListener(counter)
    val query: String = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    graph.inTx { compiler.planQuery(query, planContext, Normal) }

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    graph.inTx { compiler.planQuery(query, planContext, Normal) }

    // then
    logger.assertAtLeastOnce(LogCall.info(s"Discarded stale query from the query cache: $query"))
  }
}
