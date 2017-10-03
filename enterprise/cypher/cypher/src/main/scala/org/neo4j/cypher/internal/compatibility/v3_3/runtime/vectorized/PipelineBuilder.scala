package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.LazyTypes
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.operators.{AllNodeScanOperator, ExpandAllOperator, FilterOperator}
import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.cypher.internal.ir.v3_3.IdName
import org.neo4j.cypher.internal.v3_3.logical.plans
import org.neo4j.cypher.internal.v3_3.logical.plans._

class PipelineBuilder(pipelines: Map[LogicalPlanId, PipelineInformation], converters: ExpressionConverters)
  extends TreeBuilder[Pipeline] {

  override def create(plan: LogicalPlan): Pipeline = {
    val pipeline = super.create(plan)
    pipeline.construct
  }

  override protected def build(plan: LogicalPlan): Pipeline = {
    val pipeline = pipelines(plan.assignedId)

    val thisOp = plan match {
      case plans.AllNodesScan(IdName(column), argumentIds) =>
        new AllNodeScanOperator(
          pipeline.numberOfLongs,
          pipeline.numberOfReferences,
          pipeline.getLongOffsetFor(column))

    }

    Pipeline(thisOp, Seq.empty, pipeline, Set.empty)()
  }

  override protected def build(plan: LogicalPlan, source: Pipeline): Pipeline = {
    val pipeline = pipelines(plan.assignedId)

    if(plan.isInstanceOf[ProduceResult])
      source
    else {
      val thisOp = plan match {
        case plans.Selection(predicates, _) =>
          val predicate = converters.toCommandPredicate(predicates.head)
          new FilterOperator(pipeline, predicate)

        case plans.Expand(lhs, IdName(from), dir, types, IdName(to), IdName(relName), ExpandAll) =>
          val fromOffset = pipeline.getLongOffsetFor(from)
          val relOffset = pipeline.getLongOffsetFor(relName)
          val toOffset = pipeline.getLongOffsetFor(to)
          val fromPipe = pipelines(lhs.assignedId)
          new ExpandAllOperator(pipeline, fromPipe, fromOffset, relOffset, toOffset, dir, LazyTypes(types)(SemanticTable()))
      }

      thisOp match {
        case op: Operator =>
          source.addOperator(op)
        case breaker: LeafOperator =>
          Pipeline(breaker, Seq.empty, pipeline, Set(source))()
      }
    }
  }

  override protected def build(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline = ???
}
