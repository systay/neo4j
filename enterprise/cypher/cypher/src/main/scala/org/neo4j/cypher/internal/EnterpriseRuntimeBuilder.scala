/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_3.compiled_runtime.{BuildCompiledExecutionPlan, EnterpriseRuntimeContext}
import org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.{RegisterAllocation, RegisterAllocations, RegisterPipeBuilderFactory, RegisterPipeExecutionBuilderContext}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.PipeInfo
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_3.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LogicalPlanIdentificationBuilder
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_3.InvalidArgumentException
import org.neo4j.cypher.internal.frontend.v3_3.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.frontend.v3_3.phases.{Do, If, Transformer}

object EnterpriseRuntimeBuilder extends RuntimeBuilder[Transformer[EnterpriseRuntimeContext, LogicalPlanState, CompilationState]] {
  def create(runtimeName: Option[RuntimeName], useErrorsOverWarnings: Boolean): Transformer[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] = runtimeName match {
    case None =>
      BuildCompiledExecutionPlan andThen
        buildInterpretedRuntimeIfCompilationFailed()

    case Some(InterpretedRuntimeName) =>
      BuildInterpretedExecutionPlan(buildPipesWithRegisterAllocation)

    case Some(CompiledRuntimeName) if useErrorsOverWarnings =>
      BuildCompiledExecutionPlan andThen
        If((state: CompilationState) => state.maybeExecutionPlan.isEmpty)(
        Do((_,_) => throw new InvalidArgumentException("The given query is not currently supported in the selected runtime"))
      )

    case Some(CompiledRuntimeName) =>
      BuildCompiledExecutionPlan andThen
        If((state: CompilationState) => state.maybeExecutionPlan.isEmpty)(
        Do((_: EnterpriseRuntimeContext).notificationLogger.log(RuntimeUnsupportedNotification)) andThen
          buildInterpretedRuntimeIfCompilationFailed()
      )

    case Some(x) => throw new InvalidArgumentException(s"This version of Neo4j does not support requested runtime: $x")
  }

  private def buildInterpretedRuntimeIfCompilationFailed(): Transformer[CommunityRuntimeContext, CompilationState, CompilationState] =
    If((state: CompilationState) => state.maybeExecutionPlan.isEmpty) {
      BuildInterpretedExecutionPlan(buildPipesWithRegisterAllocation)
    } andThen
      If((state: CompilationState) => state.maybeExecutionPlan.isEmpty) {
        BuildInterpretedExecutionPlan(CommunityRegisters.buildPipes)
      }

  private def buildPipesWithRegisterAllocation(from: LogicalPlanState, context: CommunityRuntimeContext):
  (LogicalPlan, Map[LogicalPlan, Id], PipeInfo) = {
    val logicalPlan = from.logicalPlan
    val planToAllocations: Map[LogicalPlan, RegisterAllocations] = RegisterAllocation.allocateRegisters(logicalPlan)
    val idMap = LogicalPlanIdentificationBuilder(logicalPlan)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(context.clock, context.monitors, pipeBuilderFactory = RegisterPipeBuilderFactory())
    val pipeBuildContext = new RegisterPipeExecutionBuilderContext(context.metrics.cardinality, from.semanticTable(), from.plannerName, planToAllocations)
    val pipeInfo = executionPlanBuilder.build(from.periodicCommit, logicalPlan, idMap)(pipeBuildContext, context.planContext)
    (logicalPlan, idMap, pipeInfo)
  }
}
