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
package org.neo4j.cypher.internal

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.v3_2.exceptionHandler
import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.{InvalidArgumentException, SyntaxException, _}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.impl.notification.NotificationCode.RULE_PLANNER_UNAVAILABLE_FALLBACK
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.{Log, LogProvider}

object CypherCompiler {
  val DEFAULT_QUERY_CACHE_SIZE: Int = 128
  val DEFAULT_QUERY_PLAN_TTL: Long = 1000 // 1 second
  val CLOCK: Clock = Clock.systemUTC()
  val DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD = 0.5
  val DEFAULT_NON_INDEXED_LABEL_WARNING_THRESHOLD = 10000
}

case class PreParsedQuery(statement: String, rawStatement: String, version: CypherVersion,
                          executionMode: CypherExecutionMode, planner: CypherPlanner, runtime: CypherRuntime,
                          codeGenMode: CypherCodeGenMode, updateStrategy: CypherUpdateStrategy)
                         (val offset: InputPosition) {
  val statementWithVersionAndPlanner: String = {
    val plannerInfo = planner match {
      case CypherPlanner.default => ""
      case _ => s"planner=${planner.name}"
    }
    val runtimeInfo = runtime match {
      case CypherRuntime.default => ""
      case _ => s"runtime=${runtime.name}"
    }
    val codeGenModeInfo = codeGenMode match {
      case CypherCodeGenMode.default => ""
      case _ => s"codeGenMode=${codeGenMode.name}"
    }
    val updateStrategyInfo = updateStrategy match {
      case CypherUpdateStrategy.default => ""
      case _ => s"updateStrategy=${updateStrategy.name}"
    }

    s"CYPHER ${version.name} $plannerInfo $runtimeInfo $codeGenModeInfo $updateStrategyInfo $statement".replaceAll("\\s+", " ")
  }
}

class CypherCompiler(graph: GraphDatabaseQueryService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     configuredVersion: CypherVersion,
                     configuredPlanner: CypherPlanner,
                     configuredRuntime: CypherRuntime,
                     useErrorsOverWarnings: Boolean,
                     idpMaxTableSize: Int,
                     idpIterationDuration: Long,
                     errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                     logProvider: LogProvider) {
  import org.neo4j.cypher.internal.CypherCompiler._

  private val log: Log = logProvider.getLog(getClass)

  private val config = CypherCompilerConfiguration(
    queryCacheSize = getQueryCacheSize,
    statsDivergenceThreshold = getStatisticsDivergenceThreshold,
    queryPlanTTL = getMinimumTimeBeforeReplanning,
    useErrorsOverWarnings = useErrorsOverWarnings,
    idpMaxTableSize = idpMaxTableSize,
    idpIterationDuration = idpIterationDuration,
    errorIfShortestPathFallbackUsedAtRuntime = errorIfShortestPathFallbackUsedAtRuntime,
    nonIndexedLabelWarningThreshold = getNonIndexedLabelWarningThreshold
  )

  private val factory = new PlannerFactory(graph, kernelAPI, kernelMonitors, log, config)
  private val planners: PlannerCache = new PlannerCache(factory)


  private final val ILLEGAL_PLANNER_RUNTIME_COMBINATIONS: Set[(CypherPlanner, CypherRuntime)] = Set((CypherPlanner.rule, CypherRuntime.compiled))

  @throws(classOf[SyntaxException])
  def preParseQuery(queryText: String): PreParsedQuery = exceptionHandler.runSafely {
    val preParsedStatement = CypherPreParser(queryText)
    val CypherStatementWithOptions(statement, offset, version, planner, runtime, codeGenMode, updateStrategy, mode) =
      CypherStatementWithOptions(preParsedStatement)

    val cypherVersion = version.getOrElse(configuredVersion)
    val pickedExecutionMode = mode.getOrElse(CypherExecutionMode.default)

    val pickedPlanner = pick(planner, CypherPlanner, if (cypherVersion == configuredVersion) Some(configuredPlanner) else None)
    val pickedRuntime = pick(runtime, CypherRuntime, if (cypherVersion == configuredVersion) Some(configuredRuntime) else None)
    val pickedCodeGenMode = pick(codeGenMode, CypherCodeGenMode, None)
    val pickedUpdateStrategy = pick(updateStrategy, CypherUpdateStrategy, None)

    assertValidOptions(CypherStatementWithOptions(preParsedStatement), cypherVersion, pickedExecutionMode, pickedPlanner, pickedRuntime)

    PreParsedQuery(statement, queryText, cypherVersion, pickedExecutionMode,
      pickedPlanner, pickedRuntime, pickedCodeGenMode, pickedUpdateStrategy)(offset)
  }

  private def pick[O <: CypherOption](candidate: Option[O], companion: CypherOptionCompanion[O], configured: Option[O]): O = {
    val specified = candidate.getOrElse(companion.default)
    if (specified == companion.default) configured.getOrElse(specified) else specified
  }

  private def assertValidOptions(statementWithOption: CypherStatementWithOptions,
                                 cypherVersion: CypherVersion, executionMode: CypherExecutionMode,
                                 planner: CypherPlanner, runtime: CypherRuntime) {
    if (ILLEGAL_PLANNER_RUNTIME_COMBINATIONS((planner, runtime)))
      throw new InvalidArgumentException(s"Unsupported PLANNER - RUNTIME combination: ${planner.name} - ${runtime.name}")
  }

  @throws(classOf[SyntaxException])
  def parseQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer): ParsedQuery = {
    import org.neo4j.cypher.internal.compatibility.v2_3.helpers._
    import org.neo4j.cypher.internal.compatibility.v3_1.helpers._

    var version = preParsedQuery.version
    val planner = preParsedQuery.planner
    val runtime = preParsedQuery.runtime
    val codeGenMode = preParsedQuery.codeGenMode
    val updateStrategy = preParsedQuery.updateStrategy

    var preParsingNotifications: Set[org.neo4j.graphdb.Notification] = Set.empty
    if (version == CypherVersion.v3_2 && planner == CypherPlanner.rule) {
      val position = preParsedQuery.offset
      preParsingNotifications = preParsingNotifications + rulePlannerUnavailableFallbackNotification(position)
      version = CypherVersion.v3_1
    }

    version match {
      case CypherVersion.v3_2 => planners(PlannerSpec_v3_2(planner, runtime, updateStrategy, codeGenMode))
        .produceParsedQuery(preParsedQuery, tracer, preParsingNotifications)
      case CypherVersion.v3_1 => planners(PlannerSpec_v3_1(planner, runtime, updateStrategy))
        .produceParsedQuery(preParsedQuery, as3_1(tracer), preParsingNotifications)
      case CypherVersion.v2_3 => planners(PlannerSpec_v2_3(planner, runtime))
        .produceParsedQuery(preParsedQuery, as2_3(tracer), preParsingNotifications)
    }
  }

  private def rulePlannerUnavailableFallbackNotification(offset: InputPosition): org.neo4j.graphdb.Notification = {
    val pos = new org.neo4j.graphdb.InputPosition(offset.offset, offset.line, offset.column)
    RULE_PLANNER_UNAVAILABLE_FALLBACK.notification(pos)
  }

  private def getQueryCacheSize : Int = {
    val setting: (Config) => Int = config => config.get(GraphDatabaseSettings.query_cache_size).intValue()
    getSetting(graph, setting, DEFAULT_QUERY_CACHE_SIZE)
  }


  private def getStatisticsDivergenceThreshold : Double = {
    val setting: (Config) => Double = config => config.get(GraphDatabaseSettings.query_statistics_divergence_threshold).doubleValue()
    getSetting(graph, setting, DEFAULT_STATISTICS_DIVERGENCE_THRESHOLD)
  }

  private def getNonIndexedLabelWarningThreshold: Long = {
    val setting: (Config) => Long = config => config.get(GraphDatabaseSettings.query_non_indexed_label_warning_threshold).longValue()
    getSetting(graph, setting, DEFAULT_NON_INDEXED_LABEL_WARNING_THRESHOLD)
  }

  private def getMinimumTimeBeforeReplanning: Long = {
    val setting: (Config) => Long = config => config.get(GraphDatabaseSettings.cypher_min_replan_interval).longValue()
    getSetting(graph, setting, DEFAULT_QUERY_PLAN_TTL)
  }

  private def getSetting[A](gds: GraphDatabaseQueryService, configLookup: Config => A, default: A): A = gds match {
    // TODO: Cypher should not be pulling out components from casted interfaces, it should ask for Config as a dep
    case (gdbApi:GraphDatabaseQueryService) => configLookup(gdbApi.getDependencyResolver.resolveDependency(classOf[Config]))
    case _ => default
  }
}
