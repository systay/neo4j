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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_1.PropertyKeyId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.CandidateList
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexUniqueSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.LabelId

abstract class IndexLeafPlanner extends LeafPlanner {
  def apply(qg: QueryGraph)(implicit context: LogicalPlanContext) = {
    implicit val semanticTable = context.semanticTable
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap = qg.selections.labelPredicates

    CandidateList(
      predicates.collect {
        // n.prop = value
        case propertyPredicate@Equals(Property(identifier@Identifier(name), propertyKey), ConstantExpression(valueExpr)) =>
          val idName = IdName(name)
          for (propertyKeyId <- propertyKey.id.toSeq;
               labelPredicate <- labelPredicateMap.getOrElse(idName, Set.empty);
               label <- labelPredicate.labels;
               labelId <- label.id if findIndexesForLabel(labelId.id).toSeq.exists(_.getPropertyKeyId == propertyKeyId.id))
          yield {
            val entryConstructor = constructPlan(idName, labelId, propertyKeyId, valueExpr)
            entryConstructor(Seq(propertyPredicate, labelPredicate))
          }
      }.flatten
    )
  }

  protected def constructPlan(idName: IdName,
                              labelId: LabelId,
                              propertyKeyId: PropertyKeyId,
                              valueExpr: Expression)(implicit context: LogicalPlanContext): (Seq[Expression]) => QueryPlan

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanContext): Iterator[IndexDescriptor]
}

object uniqueIndexSeekLeafPlanner extends IndexLeafPlanner {
  protected def constructPlan(idName: IdName, labelId: LabelId, propertyKeyId: PropertyKeyId, valueExpr: Expression)
                             (implicit context: LogicalPlanContext): (Seq[Expression]) => QueryPlan =
    (predicates: Seq[Expression]) =>
      QueryPlan( NodeIndexUniqueSeek(idName, labelId, propertyKeyId, valueExpr)(predicates) )

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanContext): Iterator[IndexDescriptor] =
    context.planContext.uniqueIndexesGetForLabel(labelId)
}

object indexSeekLeafPlanner extends IndexLeafPlanner {
  protected def constructPlan(idName: IdName, labelId: LabelId, propertyKeyId: PropertyKeyId, valueExpr: Expression)
                             (implicit context: LogicalPlanContext): (Seq[Expression]) => QueryPlan =
    (predicates: Seq[Expression]) =>
      QueryPlan( NodeIndexSeek(idName, labelId, propertyKeyId, valueExpr)(predicates) )

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanContext): Iterator[IndexDescriptor] =
    context.planContext.indexesGetForLabel(labelId)
}
