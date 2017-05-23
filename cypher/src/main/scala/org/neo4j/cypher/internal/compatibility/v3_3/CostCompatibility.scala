/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.phases.{CompilerContext, LogicalPlanState}
import org.neo4j.cypher.internal.frontend.v3_3.phases.{Monitors, Transformer}
import org.neo4j.cypher.{CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

case class CostCompatibility[PC <: CompilerContext,
                             RC <: RuntimeContext,
                             T <: Transformer[RC, LogicalPlanState, CompilationState]](config: CypherCompilerConfiguration,
                                                                                       clock: Clock,
                                                                                       kernelMonitors: KernelMonitors,
                                                                                       kernelAPI: KernelAPI,
                                                                                       log: Log,
                                                                                       planner: CypherPlanner,
                                                                                       runtime: CypherRuntime,
                                                                                       updateStrategy: CypherUpdateStrategy,
                                                                                       runtimeBuilder: RuntimeBuilder[T],
                                                                                       planContextCreator: ContextCreator[PC],
                                                                                       runtimeContextCreator: (PC, RuntimeSpecificContext) => RC)
  extends Compatibility[PC, RC, T] {

  assert(planContextCreator != null)
  override val maybePlannerName: Option[CostBasedPlannerName] = planner match {
    case CypherPlanner.default => None
    case CypherPlanner.cost | CypherPlanner.idp => Some(IDPPlannerName)
    case CypherPlanner.dp => Some(DPPlannerName)
    case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
  }

  override val maybeUpdateStrategy: Option[UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
    case _ => None
  }

  override val maybeRuntimeName: Option[RuntimeName] = runtime match {
    case CypherRuntime.default => None
    case CypherRuntime.interpreted => Some(InterpretedRuntimeName)
    case CypherRuntime.compiled => Some(CompiledRuntimeName)
  }

  protected override val compiler: CypherCompiler[PC] = {

    val monitors = WrappedMonitors(kernelMonitors)

    new CypherCompilerFactory().costBasedCompiler(config, clock, monitors, rewriterSequencer,
                                                  maybePlannerName, maybeUpdateStrategy, planContextCreator)
  }

  override val logger = new StringInfoLogger(log)

  override val monitors: Monitors = WrappedMonitors(kernelMonitors)

  override val queryCacheSize: Int = config.queryCacheSize


}