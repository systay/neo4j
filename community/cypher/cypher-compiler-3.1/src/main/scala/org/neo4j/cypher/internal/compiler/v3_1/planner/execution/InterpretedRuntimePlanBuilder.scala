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
package org.neo4j.cypher.internal.compiler.v3_1.planner.execution

import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{PeriodicCommitInfo, PlanFingerprint, PipeInfo}
import org.neo4j.cypher.internal.compiler.v3_1.planner.PeriodicCommit
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_1.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v3_1.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.PatternConverters._
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.StatementConverters
import org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.compiler.v3_1.commands.EntityProducerFactory
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{AggregationExpression, Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v3_1.commands.predicates.{True, _}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.builders.prepare.KeyTokenResolver
import org.neo4j.cypher.internal.compiler.v3_1.pipes._
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.{Limit => LimitPlan, LoadCSV => LoadCSVPlan, Skip => SkipPlan, _}
import org.neo4j.cypher.internal.compiler.v3_1.planner.{CantHandleQueryException, PeriodicCommit, logical}
import org.neo4j.cypher.internal.compiler.v3_1.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v3_1.{ExecutionContext, Monitors, ast => compilerAst, pipes}
import org.neo4j.cypher.internal.frontend.v3_1._
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.helpers.Eagerly
import org.neo4j.graphdb.Relationship
import org.neo4j.helpers.Clock

class InterpretedRuntimePlanBuilder {
  def build(periodicCommit: Option[PeriodicCommit], plan: LogicalPlan)(implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeInfo = {

    val topLevelPipe = buildPipe(plan)

    val fingerprint = planContext.statistics match {
      case igs: InstrumentedGraphStatistics =>
        Some(PlanFingerprint(clock.currentTimeMillis(), planContext.txIdProvider(), igs.snapshot.freeze))
      case _ =>
        None
    }

    val periodicCommitInfo = periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    PipeInfo(topLevelPipe, plan.solved.exists(_.queryGraph.containsUpdates), periodicCommitInfo, fingerprint, context.plannerName)
  }

}
