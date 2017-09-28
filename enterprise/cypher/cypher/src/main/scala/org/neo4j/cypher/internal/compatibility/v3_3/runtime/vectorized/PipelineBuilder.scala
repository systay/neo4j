package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.operators.{AllNodeScanOperator, FilterOperator}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.ir.v3_3.IdName

class PipelineBuilder(pipelines: Map[LogicalPlanId, PipelineInformation], converters: ExpressionConverters)
  extends TreeBuilder[Pipeline] {
  override protected def build(plan: LogicalPlan): Pipeline = {
    val pipeline = pipelines(plan.assignedId)

    val thisOp = plan match {
      case plans.AllNodesScan(IdName(column), argumentIds) =>
        new AllNodeScanOperator(
          pipeline.numberOfLongs,
          pipeline.numberOfReferences,
          pipeline.getLongOffsetFor(column))

    }

    Pipeline(Seq(thisOp), pipeline, Set.empty)
  }

  override protected def build(plan: LogicalPlan, source: Pipeline): Pipeline = {
    val pipeline = pipelines(plan.assignedId)

    if(plan.isInstanceOf[ProduceResult])
      source
    else {
      val thisOp: Operator = plan match {
        case plans.Selection(predicates, _) =>
          val predicate = converters.toCommandPredicate(predicates.head)
          new FilterOperator(pipeline, predicate)

        case plans.Expand(_, IdName(from), dir, types, IdName(to), IdName(relName), ExpandAll) =>
          ???
      }

      source.copy(operators = source.operators :+ thisOp)
    }


  }

  override protected def build(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline = ???
}
