/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_1.commands._
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.builders.{BuilderTest, Solved, TraversalMatcherBuilder, Unsolved}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery}
import org.neo4j.cypher.internal.compiler.v2_1.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{NullPipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.spi.v2_1.TransactionBoundPlanContext
import org.neo4j.graphdb.Transaction

class TraversalMatcherBuilderTest extends GraphDatabaseFunSuite with BuilderTest {
  var builder: TraversalMatcherBuilder = null
  var ctx: PlanContext = null
  var tx: Transaction = null
  private implicit val monitor = mock[PipeMonitor]

  override def beforeEach() {
    super.beforeEach()
    builder = new TraversalMatcherBuilder
    tx = graph.beginTx()
    ctx = new TransactionBoundPlanContext(statement, kernelAPI, graph)
  }

  override def afterEach() {
    tx.finish()
    super.afterEach()
  }

  test("should_not_accept_queries_without_patterns") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndex("n", "index", Literal("key"), Literal("expression"))))
    )

    builder.canWorkWith(plan(NullPipe(), q), ctx) should be(false)
  }

  test("should_accept_variable_length_paths") {
    val q = query("START me=node:node_auto_index(name = 'Jane') " +
                  "MATCH me-[:jane_knows*]->friend-[:has]->status " +
                  "RETURN me")

    builder.canWorkWith(plan(NullPipe(), q), ctx) should be(true)
  }

  test("should_not_accept_queries_with_varlength_paths") {
    val q = query("START me=node:node_auto_index(name = 'Tarzan'), you=node:node_auto_index(name = 'Jane') " +
                  "MATCH me-[:LOVES*]->banana-[:LIKES*]->you " +
                  "RETURN me")

    builder.canWorkWith(plan(NullPipe(), q), ctx) should be(true)
  }

  test("should_handle_loops") {
    val q = query("START me=node:node_auto_index(name = 'Tarzan'), you=node:node_auto_index(name = 'Jane') " +
                  "MATCH me-[:LIKES]->(u1)<-[:LIKES]->you, me-[:HATES]->(u2)<-[:HATES]->you " +
                  "RETURN me")

    builder.canWorkWith(plan(NullPipe(), q), ctx) should be(true)
  }

  test("should_not_take_on_path_expression_predicates") {
    val q = query("START a=node({self}) MATCH a-->b WHERE b-->() RETURN b")

    builder.canWorkWith(plan(NullPipe(), q), ctx) should be(true)

    val testPlan = plan(NullPipe(), q)
    val newPlan = builder.apply(testPlan, ctx)

    assertQueryHasNotSolvedPathExpressions(newPlan)
  }

  test("should_handle_global_queries") {
    val q = query("START a=node({self}), b = node(*) MATCH a-->b RETURN b")

    val testPlan = plan(NullPipe(), q)
    builder.canWorkWith(testPlan, ctx) should be(true)

    val newPlan = builder.apply(testPlan, ctx)

    newPlan.query.start.exists(_.unsolved) should be(false)
  }

  def assertQueryHasNotSolvedPathExpressions(newPlan: ExecutionPlanInProgress) {
    newPlan.query.where.foreach {
      case Solved(pred) if pred.exists(_.isInstanceOf[PathExpression]) => fail("Didn't expect the predicate to be solved")
      case _                                                           =>
    }
  }

  private val parser = new CypherParser(mock[ParserMonitor[Statement]])

  private def query(text: String): PartiallySolvedQuery = PartiallySolvedQuery(parser.parse(text).asQuery.asInstanceOf[Query])
}
