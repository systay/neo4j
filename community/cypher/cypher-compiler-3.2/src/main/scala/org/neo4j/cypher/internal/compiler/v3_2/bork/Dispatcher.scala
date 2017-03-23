package org.neo4j.cypher.internal.compiler.v3_2.bork

import java.util.function.Supplier

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{ExecutionPlan, InternalExecutionResult, InternalQueryType, StandardInternalExecutionResult}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, InternalResultVisitor, PlanContext, QueryContext}
import org.neo4j.cypher.internal.frontend.v3_2.PlannerName
import org.neo4j.cypher.internal.frontend.v3_2.notification.InternalNotification
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.Statement

class Dispatcher(db: GraphDatabaseQueryService) {
  def execute(executionPlan: Operator, pipeLines: Map[Operator, Pipeline], statementGetter: () => Statement): ExecutionPlan = {

    new ExecutionPlan {
      override def run(queryContext: QueryContext, planType: ExecutionMode, params: Map[String, Any]): InternalExecutionResult =
        new StandardInternalExecutionResult(queryContext) with StandardInternalExecutionResult.IterateByAccepting {
          override def queryStatistics(): InternalQueryStatistics = InternalQueryStatistics()

          override def executionPlanDescription(): InternalPlanDescription = ???

          override def executionType: InternalQueryType = org.neo4j.cypher.internal.compiler.v3_2.executionplan.READ_ONLY

          override def columns: List[String] = List("n")

          override def executionMode: ExecutionMode = NormalMode

          override def accept[EX <: Exception](visitor: InternalResultVisitor[EX]): Unit = {
            ???
//            var start: Operator = startOperator(executionPlan)
//
//            val currentPipeline: PipeLine = pipeLines(start)
//            val registerInfo = new RegisterInfo(currentPipeline.numberOfLongs, currentPipeline.numberOfReferences)
//
//            var moreFromStore = true
//            val morselSize = 100000
//
//            val morsel = new Morsel(registerInfo, morselSize)
//            val queryState = new QueryRun(statementGetter(), visitor)
//
//            var current = Optional.of(start)
//
//            while(current.isPresent || moreFromStore) {
//              morsel.resetReadPos()
//
//              // start over when we have reached the end but the is still data from the stores to be had
//              // TODO: this is probably not correct.
//              val op = current.orElseGet(asJava(() => start))
//
//              val result = op.execute(morsel, queryState)
//              if(op.isLeaf) {
//                moreFromStore = result
//              }
//              current = op.parent
//            }

          }
        }

      override def isPeriodicCommit: Boolean = false

      override def plannerUsed: PlannerName = IDPPlannerName

      override def isStale(lastTxId: () => Long, statistics: GraphStatistics): Boolean = false

      override def runtimeUsed: RuntimeName = InterpretedRuntimeName

      override def notifications(planContext: PlanContext): Seq[InternalNotification] = Seq.empty
    }

  }

  private def asJava[T](in: () => T): Supplier[T] = new Supplier[T] {
    override def get() = in()
  }

  private def startOperator(executionPlan: Operator): Operator = {
    var start: Operator = executionPlan
    while (start.lhs.isPresent) {
      start = start.lhs.get()
    }
    start
  }

}


case class UnitOfWork(morsel: Morsel, operator: Operator) extends WorkPackage {

  var moreWorkForOperator = false

  override def run(queryRun: QueryRun): Unit = {
//    println(s"${operator.getClass.getSimpleName} running on thread ${Thread.currentThread().getId}")
    morsel.resetReadPos()
    moreWorkForOperator = operator.execute(morsel, queryRun)
  }
}