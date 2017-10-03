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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized

import java.io.PrintWriter
import java.{lang, util}

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.{ExecutionPlan, InternalQueryType, READ_ONLY}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.{InternalPlanDescription, LogicalPlanIdentificationBuilder}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.compiler.v3_3.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.{GraphStatistics, PlanContext}
import org.neo4j.cypher.internal.frontend.v3_3.PlannerName
import org.neo4j.cypher.internal.frontend.v3_3.notification.InternalNotification
import org.neo4j.cypher.internal.frontend.v3_3.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_3.phases.{CompilationPhaseTracer, Condition, Phase}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.cypher.internal.{InternalExecutionResult, QueryStatistics}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.graphdb._
import org.neo4j.values.virtual.MapValue

object BuildVectorizedExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] {
  override def phase: CompilationPhaseTracer.CompilationPhase = CompilationPhase.PIPE_BUILDING

  override def description: String = "build pipes"

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    val (physicalPlan, pipelines) = rewritePlan(context, from.logicalPlan)
    val idMap = LogicalPlanIdentificationBuilder(physicalPlan)
    val converters: ExpressionConverters = new ExpressionConverters(SlottedExpressionConverters, CommunityExpressionConverter)

    val operatorBuilder = new PipelineBuilder(pipelines, converters)
    val operators = operatorBuilder.create(physicalPlan)
    val execPlan: ExecutionPlan = VectorizedExecutionPlan(from.plannerName, operators, pipelines, physicalPlan)
    new CompilationState(from, Some(execPlan))
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan) = {
    beforeRewrite.assignIds()
    val pipelines: Map[LogicalPlanId, PipelineInformation] = SlotAllocation.allocateSlots(beforeRewrite)
    val slottedRewriter = new SlottedRewriter(context.planContext)
    val logicalPlan = slottedRewriter(beforeRewrite, pipelines)
    (logicalPlan, pipelines)
  }

  override def postConditions: Set[Condition] = Set.empty


  case class VectorizedExecutionPlan(plannerUsed: PlannerName,
                                     operators: Pipeline,
                                     pipelineInformation: Map[LogicalPlanId, PipelineInformation],
                                     physicalPlan: LogicalPlan) extends executionplan.ExecutionPlan {
    override def run(queryContext: QueryContext, planType: ExecutionMode, params: MapValue): InternalExecutionResult = {
      new VectorizedOperatorExecutionResult(operators, pipelineInformation, physicalPlan, queryContext, params)
    }

    override def isPeriodicCommit: Boolean = false

    override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = false

    override def runtimeUsed: RuntimeName = SlottedRuntimeName

    override def notifications(planContext: PlanContext): Seq[InternalNotification] = Seq.empty
  }

}

class VectorizedOperatorExecutionResult(operators: Pipeline,
                                        pipelineInformation: Map[LogicalPlanId, PipelineInformation],
                                        logicalPlan: LogicalPlan,
                                        queryContext: QueryContext,
                                        params: MapValue) extends InternalExecutionResult {


  override def columnAs[T](column: String): Iterator[T] = ???

  override def javaColumnAs[T](column: String): ResourceIterator[T] = ???

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = ???

  override def dumpToString(writer: PrintWriter): Unit = ???

  override def dumpToString(): String = ???

  override def queryStatistics(): QueryStatistics = ???

  override def planDescriptionRequested: Boolean = ???

  override def executionPlanDescription(): InternalPlanDescription = ???

  override def queryType: InternalQueryType = READ_ONLY

  override def executionMode: ExecutionMode = NormalMode

  override def notifications: Iterable[Notification] = ???

  override def accept[E <: Exception](visitor: Result.ResultVisitor[E]): Unit = {
    Dispatcher.instance.run(operators, visitor, queryContext, pipelineInformation(logicalPlan.assignedId), params)
  }

  override def withNotifications(notification: Notification*): InternalExecutionResult = this

  override def fieldNames(): Array[String] = pipelineInformation(logicalPlan.assignedId).slots.keys.toArray

  override def accept[E <: Exception](visitor: QueryResult.QueryResultVisitor[E]): Unit = ???

  override def close(): Unit = ???

  override def hasNext: Boolean = ???

  override def next(): Map[String, Any] = ???
}

class MorselResultRow(var morsel: Morsel, var currentPos: Int, pipelineInformation: PipelineInformation, queryContext: QueryContext) extends Result.ResultRow {
  override def getNode(key: String): Node = {
    val nodeOffset = pipelineInformation.getLongOffsetFor(key)
    val nodeId = morsel.longs(currentPos * pipelineInformation.numberOfLongs + nodeOffset)
    queryContext.nodeOps.getById(nodeId)
  }

  override def getRelationship(key: String): Relationship = ???

  override def get(key: String): AnyRef = ???

  override def getString(key: String): String = ???

  override def getNumber(key: String): Number = ???

  override def getBoolean(key: String): lang.Boolean = ???

  override def getPath(key: String): Path = ???
}