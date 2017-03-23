package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_2.phases.{CompilationContains, CompilationState, CompilerContext}
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.v3_2.phases.Phase
import org.neo4j.kernel.GraphDatabaseQueryService

object BuildInterpreted2ExecutionPlan extends Phase[CompilerContext, CompilationState, CompilationState] {

  var graphDatabase: GraphDatabaseQueryService = null // TODO: This is a major hack to cut through all the layers

  override def phase = PIPE_BUILDING

  override def description = "create interpreted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: CompilationState, context: CompilerContext): CompilationState = {
    ???
//    val logicalPlan = from.logicalPlan
//    val pipeLines: Map[LogicalPlan, PipeLine] = Pipeliner.calculate(logicalPlan)
//    val (operator, operatorPipelineMap) = new LogicalPlan2Operator(pipeLines, from.semanticTable()).create(logicalPlan)
//    val statementGetter = graphDatabase.getDependencyResolver.provideDependency(classOf[ThreadToStatementContextBridge]).get()
//    val execPlan = new Dispatcher(graphDatabase).execute(operator, operatorPipelineMap, () => statementGetter.get())
//
//    from.copy(maybeExecutionPlan = Some(execPlan))
  }
}
