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

import org.neo4j.cypher.internal.compatibility.{v2_3, v3_1, _}
import org.neo4j.cypher.internal.compiled_runtime.v3_2.BuildCompiledExecutionPlan
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.{ByteCodeMode, CodeGenConfiguration, CodeGenMode, SourceCodeMode}
import org.neo4j.cypher.internal.compiled_runtime.v3_2.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.phases.{Context, Do, If, Transformer}
import org.neo4j.cypher.internal.frontend.v3_2.InvalidArgumentException
import org.neo4j.cypher.internal.frontend.v3_2.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.internal.spi.v3_2.codegen.GeneratedQueryStructure
import org.neo4j.cypher.{CypherCodeGenMode, CypherPlanner, CypherRuntime}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.LogProvider

class EnterpriseCompatibilityFactory(inner: CompatibilityFactory, graph: GraphDatabaseQueryService,
                                     kernelAPI: KernelAPI, kernelMonitors: KernelMonitors,
                                     logProvider: LogProvider) extends CompatibilityFactory {
  override def create(spec: PlannerSpec_v2_3, config: CypherCompilerConfiguration): v2_3.Compatibility =
    inner.create(spec, config)

  override def create(spec: PlannerSpec_v3_1, config: CypherCompilerConfiguration): v3_1.Compatibility =
    inner.create(spec, config)

  override def create(spec: PlannerSpec_v3_2, config: CypherCompilerConfiguration): v3_2.Compatibility =
    (spec.planner, spec.runtime) match {
      case (CypherPlanner.rule, _) => inner.create(spec, config)

      case (_, CypherRuntime.compiled) | (_, CypherRuntime.default) =>

        val codeGenMode = spec.codeGenMode match {
          case CypherCodeGenMode.default => CodeGenMode.default
          case CypherCodeGenMode.byteCode => ByteCodeMode
          case CypherCodeGenMode.sourceCode => SourceCodeMode
        }

        val codeGenConfiguration = CodeGenConfiguration(mode = codeGenMode)
        def contextUpdater(context: Context): Context = context.
          set[CodeStructure[GeneratedQuery]](GeneratedQueryStructure).
          set(codeGenConfiguration)

        v3_2.CostCompatibility(config, CompilerEngineDelegator.CLOCK, kernelMonitors, kernelAPI, logProvider.getLog
        (getClass), spec.planner, spec.runtime, spec.updateStrategy, EnterpriseRuntimeBuilder, contextUpdater)

      case _ => inner.create(spec, config)
    }
}

object EnterpriseRuntimeBuilder extends RuntimeBuilder {
  def create(runtimeName: Option[RuntimeName], useErrorsOverWarnings: Boolean): Transformer = runtimeName match {
    case None =>
      BuildCompiledExecutionPlan andThen
      If(_.maybeExecutionPlan.isEmpty) {
        BuildInterpretedExecutionPlan
      }

    case Some(InterpretedRuntimeName) =>
      BuildInterpretedExecutionPlan

    case Some(CompiledRuntimeName) if useErrorsOverWarnings =>
      BuildCompiledExecutionPlan andThen
      If(_.maybeExecutionPlan.isEmpty)(
        Do(_ => throw new InvalidArgumentException("The given query is not currently supported in the selected runtime"))
      )

    case Some(CompiledRuntimeName) =>
      BuildCompiledExecutionPlan andThen
      If(_.maybeExecutionPlan.isEmpty)(
        Do(_.notificationLogger.log(RuntimeUnsupportedNotification)) andThen
        BuildInterpretedExecutionPlan
      )

    case Some(x) => throw new InvalidArgumentException(s"This version of Neo4j does not support requested runtime: $x")
  }
}