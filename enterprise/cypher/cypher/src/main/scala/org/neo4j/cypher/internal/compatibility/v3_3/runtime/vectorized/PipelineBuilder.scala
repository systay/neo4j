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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.LazyTypes
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.operators._
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

    Pipeline(thisOp, Seq.empty, pipeline, NoDependencies)()
  }

  override protected def build(plan: LogicalPlan, source: Pipeline): Pipeline = {
    val pipeline = pipelines(plan.assignedId)

      val thisOp = plan match {
        case plans.ProduceResult(_, _) =>
          new ProduceResultOperator(pipeline)

        case plans.Optional(inner, symbols) =>
          val nullableKeys = inner.availableSymbols -- symbols
          val nullableOffsets = nullableKeys.map(k => pipeline.getLongOffsetFor(k.name))
          new OptionalOperator(nullableOffsets)

        case plans.Selection(predicates, _) =>
          val predicate = converters.toCommandPredicate(predicates.head)
          new FilterOperator(pipeline, predicate)

        case plans.Expand(lhs, IdName(from), dir, types, IdName(to), IdName(relName), ExpandAll) =>
          val fromOffset = pipeline.getLongOffsetFor(from)
          val relOffset = pipeline.getLongOffsetFor(relName)
          val toOffset = pipeline.getLongOffsetFor(to)
          val fromPipe = pipelines(lhs.assignedId)
          val lazyTypes = LazyTypes(types.toArray)(SemanticTable())
          new ExpandAllOperator(pipeline, fromPipe, fromOffset, relOffset, toOffset, dir, lazyTypes)
      }

    thisOp match {
      case o: Operator =>
        Pipeline(o, Seq.empty, pipeline, MorselByMorsel(source))()
      case mo: MiddleOperator =>
        source.addOperator(mo)
    }
  }

  override protected def build(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline = {
      val pipeline = pipelines(plan.assignedId)

      val thisOp = plan match {
        case plans.NodeHashJoin(nodes, _, _) =>
//          val updatedRhs = rhs.copy(dependency = rhs.dependency+lhs)
//
//          Pipeline()
          ???
      }
    ???
  }
}

object IsPipelineBreaker {
  def apply(plan: LogicalPlan): Boolean = {
    plan match {
      case _ => true
    }
  }
}