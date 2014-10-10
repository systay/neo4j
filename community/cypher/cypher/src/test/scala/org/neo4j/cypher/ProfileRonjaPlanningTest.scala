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
package org.neo4j.cypher

import org.json4s.JsonAST._
import org.json4s.native.JsonMethods
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_2.executionplan._
import org.neo4j.cypher.internal.compiler.v2_2.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.{CardinalityModel, CostModel, QueryGraphCardinalityModel}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence.{Pattern2Selectivity, AssumeIndependenceQueryGraphCardinalityModel, Expression2Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{LogicalPlan, PatternRelationship}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, PlanContext, QueriedGraphStatistics}
import org.neo4j.cypher.internal.spi.v2_2.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.internal.{LRUCache, Profiled}
import org.neo4j.cypher.toJson._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.collection.mutable
import scala.text.Document


class ProfileRonjaPlanningTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  val monitorTag = "APA"

  def buildCompiler(metricsFactoryInput: MetricsFactory = SimpleMetricsFactory)(graph: GraphDatabaseService) = {
    val kernelMonitors = new KernelMonitors()
    val monitors = new Monitors(kernelMonitors)
    val parser = new CypherParser(monitors.newMonitor[ParserMonitor[ast.Statement]](monitorTag))
    val checker = new SemanticChecker(monitors.newMonitor[SemanticCheckMonitor](monitorTag))
    val rewriter = new ASTRewriter(monitors.newMonitor[AstRewritingMonitor](monitorTag))
    val planBuilderMonitor = monitors.newMonitor[NewLogicalPlanSuccessRateMonitor](monitorTag)
    val planningMonitor = monitors.newMonitor[PlanningMonitor](monitorTag)
    val events = new LoggingState()
    val metricsFactory = LoggingMetricsFactory(metricsFactoryInput, events)
    val planner = new Planner(monitors, metricsFactory, planningMonitor)
    val pipeBuilder = new LegacyVsNewPipeBuilder(new LegacyPipeBuilder(monitors), planner, planBuilderMonitor)
    val execPlanBuilder = new ExecutionPlanBuilder(graph, pipeBuilder)
    val planCacheFactory = () => new LRUCache[PreparedQuery, ExecutionPlan](100)
    val cacheMonitor = monitors.newMonitor[AstCacheMonitor](monitorTag)
    val cache = new MonitoringCacheAccessor[PreparedQuery, ExecutionPlan](cacheMonitor)

    val compiler = new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheMonitor, monitors)

    (compiler, events)
  }

  test("Should only be turned on for debugging purposes") {
    val db = new GraphDatabaseFactory().newEmbeddedDatabase("/Users/ata/dev/neo/ronja-benchmarks/target/benchmarkdb").asInstanceOf[GraphDatabaseAPI]
    try {
      val (compiler, events) = buildCompiler(customMetrics(QueryGraphCardinalityModel.default))(db)
      val (_, result) = runQueryWith("MATCH (corp:Company)<-[:SIGNED_WITH]-(a1:Artist)-[:PERFORMED_AT]->(c:Concert)-[:IN]->(v:Venue) MATCH (corp)<-[:SIGNED_WITH]-(a2:Artist)-[:PERFORMED_AT]->(c) RETURN a1, a2, v", compiler, db)
      println(result.size)
      println(events.toJson)

    } finally db.shutdown()
  }

  trait RealStatistics extends PlanContext {
    def gdb: GraphDatabaseService

    lazy val _statistics: GraphStatistics = {
      val db = gdb.asInstanceOf[GraphDatabaseAPI]
      val queryCtx = new TransactionBoundQueryContext(db, null, true, db.statement)
      new QueriedGraphStatistics(gdb, queryCtx)
    }

    override def statistics: GraphStatistics = _statistics
  }

  private def runQueryWith(query: String, compiler: CypherCompiler, db: GraphDatabaseAPI): (List[Map[String, Any]], InternalExecutionResult) = {
    val (plan, parameters) = db.withTx {
      tx =>
        val planContext = new TransactionBoundPlanContext(db.statement, db) with RealStatistics
        compiler.planQuery(query, planContext, Profiled)
    }

    db.withTx {
      tx =>
        val queryContext = new TransactionBoundQueryContext(db, tx, true, db.statement)
        val result = plan.execute(queryContext, parameters)
        (result.toList, result)
    }
  }

  private def customMetrics(qgcmCreator: (GraphStatistics, SemanticTable) => QueryGraphCardinalityModel) = new MetricsFactory {
    def newQueryGraphCardinalityModel(statistics: GraphStatistics, semanticTable: SemanticTable) =
      qgcmCreator(statistics, semanticTable)

    def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
      SimpleMetricsFactory.newCardinalityEstimator(queryGraphCardinalityModel)

    def newCostModel(cardinality: CardinalityModel) = SimpleMetricsFactory.newCostModel(cardinality)

    def newCandidateListCreator(): (Seq[LogicalPlan]) => CandidateList = SimpleMetricsFactory.newCandidateListCreator()
  }

  class LoggingState() {

    def toJson: String = {
      val selectionsOutput = selections.map(_.toJson).toList
      val d: Document = JsonMethods.render(JArray(selectionsOutput))
      JsonMethods.pretty(d)
    }

    val selections: mutable.ListBuffer[BestPlanSelection] = new mutable.ListBuffer[BestPlanSelection]()
    var currentBestPlan: Option[BestPlanSelection] = None
    var currentCostCalc: Option[CostCalculation] = None
    var currentCardinalityEstimation: Option[CardinalityEstimation] = None

    def finishCardinalityEstimation(cardinality: Cardinality) {
      if (currentCostCalc.nonEmpty) {
        val cardinalityEstimation = currentCardinalityEstimation.get.copy(result = Some(cardinality))
        val costCalc = currentCostCalc.get
        currentCardinalityEstimation = None
        currentCostCalc = Some(costCalc.addCardinalityEstimation(cardinalityEstimation))
      }
    }

    def startCardinalityEstimation(plan: LogicalPlan) {
      if (currentCostCalc.nonEmpty) {
        assert(currentCardinalityEstimation.isEmpty)
        currentCardinalityEstimation = Some(CardinalityEstimation(plan, None))
      }
    }

    def finishCostCalculation(cost: Cost) {
      val costCalculation = currentCostCalc.get.copy(result = Some(cost))
      val currentBest = currentBestPlan.get
      currentCostCalc = None
      currentBestPlan = Some(currentBest.addCostCalculation(costCalculation))
    }

    def startCostCalculation(plan: LogicalPlan) {
      assert(currentCostCalc.isEmpty)
      currentCostCalc = Some(CostCalculation(plan, None, Seq.empty))
    }

    def finishedSelection(winner: Option[LogicalPlan]) {
      val selection = currentBestPlan.get
      selections += selection.copy(winner = winner)
      currentBestPlan = None
    }

    def startNewSelection(plans: Seq[LogicalPlan]) {
      assert(currentBestPlan.isEmpty)
      currentBestPlan = Some(BestPlanSelection(plans, None, Seq.empty))
    }

    def calculatedSelectivityFor(exp: Expression, sel: Selectivity) {
      if(currentCardinalityEstimation.nonEmpty) {
        val estimation: CardinalityEstimation = currentCardinalityEstimation.get
        currentCardinalityEstimation = Some(estimation.addExpressionCalculation(exp, sel))
      }
    }

    def calculatedSelectivityFor(p: PatternRelationship, sel: Selectivity) {
      if(currentCardinalityEstimation.nonEmpty) {
        val estimation: CardinalityEstimation = currentCardinalityEstimation.get
        currentCardinalityEstimation = Some(estimation.addPatternCalculation(p, sel))
      }
    }
  }

  case class LoggingExpression2Selectivity(inner: Expression2Selectivity, log: LoggingState) extends Expression2Selectivity {
    def apply(exp: Expression)(implicit semanticTable: SemanticTable, selections: Selections) = {
      val result = inner.apply(exp)
      log.calculatedSelectivityFor(exp, result)
      result
    }
  }
  
  case class LoggingPattern2Selectivity(inner: Pattern2Selectivity, log: LoggingState) extends Pattern2Selectivity {
    def apply(p: PatternRelationship)(implicit semanticTable: SemanticTable, selections: Selections) = {
      val result = inner.apply(p)
      log.calculatedSelectivityFor(p, result)
      result
    }
  }

  case class LoggingMetricsFactory(inner: MetricsFactory, log: LoggingState) extends MetricsFactory {
    def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
      new CardinalityModel {
        val innerCardinalityModel = inner.newCardinalityEstimator(queryGraphCardinalityModel)

        def apply(in: LogicalPlan): Cardinality = {
          log.startCardinalityEstimation(in)
          val result = innerCardinalityModel(in)
          log.finishCardinalityEstimation(result)
          result
        }
      }

    def newCostModel(cardinality: CardinalityModel): CostModel = new CostModel {
      val innerCostModel = inner.newCostModel(cardinality)

      def apply(in: LogicalPlan): Cost = {
        log.startCostCalculation(in)
        val result = innerCostModel(in)
        log.finishCostCalculation(result)
        result
      }
    }

    class LoggingCandidateList(plans: Seq[LogicalPlan] = Seq.empty) extends CandidateList(plans) {
      override def ++(other: CandidateList) = new LoggingCandidateList(super.++(other).plans)

      override def +(plan: LogicalPlan) = new LoggingCandidateList(super.+(plan).plans)

      override def bestPlan(costs: CostModel) = if (plans.size > 1) {
        log.startNewSelection(plans)
        val winner: Option[LogicalPlan] = super.bestPlan(costs)
        log.finishedSelection(winner)
        winner
      } else super.bestPlan(costs)

      override def map(f: (LogicalPlan) => LogicalPlan) = new LoggingCandidateList(super.map(f).plans)
    }

    def newCandidateListCreator(): (Seq[LogicalPlan]) => CandidateList = plans => new LoggingCandidateList(plans)

    def newQueryGraphCardinalityModel(statistics: GraphStatistics, semanticTable: SemanticTable) =
      inner.newQueryGraphCardinalityModel(statistics, semanticTable) match {
        case c: AssumeIndependenceQueryGraphCardinalityModel =>
          c.copy()(
            expressionSelectivityEstimator = LoggingExpression2Selectivity(c.expressionSelectivityEstimator, log),
            patternSelectivityEstimator = LoggingPattern2Selectivity(c.patternSelectivityEstimator, log)
          )
        case x =>
          x
      }
  }
}

case class BestPlanSelection(plans: Seq[LogicalPlan], winner: Option[LogicalPlan], costCalculations: Seq[CostCalculation]) {
  def addCostCalculation(in: CostCalculation) = copy(costCalculations = costCalculations :+ in)

  def toJson: JValue = JObject(List[(String, JValue)](
    "plans" -> JArray(plans.map(p => JString(p.toString)).toList),
    "winner" -> JString(winner.map(_.toString).getOrElse("???")),
    "calculations" -> JArray(costCalculations.map(_.toJson).toList)
  ))
}

case class CostCalculation(plan: LogicalPlan, result: Option[Cost], cardinalityEstimations: Seq[CardinalityEstimation]) {
  def addCardinalityEstimation(estimation: CardinalityEstimation) = copy(cardinalityEstimations = cardinalityEstimations :+ estimation)

  def toJson: JValue = JObject(List[(String, JValue)](
    "plan" -> JString(plan.toString),
    "cost" -> JDouble(result.map(_.gummyBears).getOrElse(-1)),
    "cardinalityEstimations" -> JArray(cardinalityEstimations.distinct.map(_.toJson).toList)
  ))
}

case class ExpressionCalculation(e: Expression, s: Selectivity) {
  def toJson: JValue = JObject(
    "expression" -> JString(e.toString),
    s.toJson
  )
}

case class PatternCalculation(p: PatternRelationship, s: Selectivity) {
  def toJson: JValue = JObject(
    "pattern" -> JString(p.toString),
    s.toJson
  )
}

case class AndSelectivity(selectivities: Seq[Selectivity], result: Selectivity) {
  def toJson: JValue = JObject(
    "selectivities" -> JArray(selectivities.map(_.toJson._2).toList)
  )
}

case class CardinalityEstimation(plan: LogicalPlan,
                                 result: Option[Cardinality],
                                 expressionCalcs: Seq[ExpressionCalculation] = Seq.empty,
                                 patternCalcs: Seq[PatternCalculation] = Seq.empty) {
  def addExpressionCalculation(e: Expression, s: Selectivity) = {
    copy(expressionCalcs = expressionCalcs :+ ExpressionCalculation(e, s))
  }

  def addPatternCalculation(p: PatternRelationship, s: Selectivity) = {
    copy(patternCalcs = patternCalcs :+ PatternCalculation(p, s))
  }

  def toJson: JValue = JObject(
    "plan" -> JString(plan.toString),
    "result" -> JDouble(result.map(_.amount).getOrElse(-1)),
    "expressions" -> JArray(expressionCalcs.map(_.toJson).toList),
    "patterns" -> JArray(patternCalcs.map(_.toJson).toList)
  )
}

object toJson {

  implicit class SelectivityToJson(s: Selectivity) {
    def toJson: (String, JValue) = "selectivity" -> JDouble(s.factor)
  }

}

