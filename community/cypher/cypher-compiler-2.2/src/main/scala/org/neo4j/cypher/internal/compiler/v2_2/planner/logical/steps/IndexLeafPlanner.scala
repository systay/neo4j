/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.commands.{ManyQueryExpression, QueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.kernel.api.index.IndexDescriptor


case class IndexLeafPlanner(
  constructPlan: (IdName, LabelToken, PropertyKeyToken, QueryExpression[Expression], Option[UsingIndexHint], Set[IdName], LogicalPlanningContext, Seq[Expression]) => LogicalPlan,
  findIndexesFor: (String, String, LogicalPlanningContext) => Option[IndexDescriptor],
  findPredicates: QueryGraph => PartialFunction[Expression, (String, PropertyKeyName, Expression, QueryExpression[Expression])]
                             ) extends LeafPlanner {
  def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext) = {
    implicit val semanticTable = context.semanticTable
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates

    def producePlanFor(name: String, propertyKeyName: PropertyKeyName, propertyPredicate: Expression, queryExpression: QueryExpression[Expression]) = {
      val idName = IdName(name)
      for (labelPredicate <- labelPredicateMap.getOrElse(idName, Set.empty);
        labelName <- labelPredicate.labels;
        indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName.name, context);
        labelId <- labelName.id)
      yield {
        val propertyName = propertyKeyName.name
        val hint = qg.hints.collectFirst {
          case hint@UsingIndexHint(Identifier(`name`), `labelName`, Identifier(`propertyName`)) => hint
        }
        constructPlan(idName, LabelToken(labelName, labelId), PropertyKeyToken(propertyKeyName, propertyKeyName.id.head),
          queryExpression, hint, qg.argumentIds, context, Seq(propertyPredicate, labelPredicate))
      }
    }

    val predicates: Seq[Expression] = qg.selections.flatPredicates
    predicates.collect(findPredicates(qg)).map {
      case (name, propertyKeyName, propertyPredicate, queryExpression) =>
        producePlanFor(name, propertyKeyName, propertyPredicate, queryExpression)
    }.flatten
  }
}

object IndexLeafPlanner {

  def constructUniquePlan(idName: IdName,
                          label: LabelToken,
                          propertyKey: PropertyKeyToken,
                          valueExpr: QueryExpression[Expression],
                          hint: Option[UsingIndexHint],
                          argumentIds: Set[IdName],
                          context: LogicalPlanningContext,
                          predicates: Seq[Expression]): LogicalPlan =
    planNodeIndexUniqueSeek(idName, label, propertyKey, valueExpr, predicates, hint, argumentIds)

  def findUniqueIndexesFor(label: String, property: String, context: LogicalPlanningContext): Option[IndexDescriptor] =
    context.planContext.getUniqueIndexRule(label, property)

  def constructPlan(idName: IdName,
                    label: LabelToken,
                    propertyKey: PropertyKeyToken,
                    valueExpr: QueryExpression[Expression],
                    hint: Option[UsingIndexHint],
                    argumentIds: Set[IdName],
                    context: LogicalPlanningContext,
                    predicates: Seq[Expression]): LogicalPlan =
    planNodeIndexSeek(idName, label, propertyKey, valueExpr, predicates, hint, argumentIds)

  def findIndexesFor(label: String, property: String, context: LogicalPlanningContext): Option[IndexDescriptor] =
    context.planContext.getIndexRule(label, property)

  def equalityComparisons(qg: QueryGraph): PartialFunction[Expression, (String, PropertyKeyName, Expression, QueryExpression[Expression])] = {
    case inPredicate@In(Property(Identifier(name), propertyKeyName), ConstantExpression(valueExpr))
      if !qg.argumentIds.contains(IdName(name)) => (name, propertyKeyName, inPredicate, ManyQueryExpression(valueExpr))
  }

  def likeQuery(qg: QueryGraph): PartialFunction[Expression, (String, PropertyKeyName, Expression, QueryExpression[Expression])] = {
    case like@Like(Property(Identifier(name), propertyKeyName), ConstantExpression(valueExpr), false)
      if !qg.argumentIds.contains(IdName(name)) => (name, propertyKeyName, like, SingleQueryExpression(valueExpr))
  }
}

object uniqueIndexSeekLeafPlanner extends IndexLeafPlanner(
  constructPlan = IndexLeafPlanner.constructUniquePlan,
  findIndexesFor = IndexLeafPlanner.findUniqueIndexesFor,
  findPredicates = IndexLeafPlanner.equalityComparisons
)

object indexSeekLeafPlanner extends IndexLeafPlanner(
  constructPlan = IndexLeafPlanner.constructPlan,
  findIndexesFor = IndexLeafPlanner.findIndexesFor,
  findPredicates = IndexLeafPlanner.equalityComparisons
)

object likeByIndexLeafPlanner extends IndexLeafPlanner(
  constructPlan = IndexLeafPlanner.constructPlan,
  findIndexesFor = IndexLeafPlanner.findIndexesFor,
  findPredicates = IndexLeafPlanner.likeQuery
)


object legacyHintLeafPlanner extends LeafPlanner {
  def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext) = {
    qg.hints.toSeq.collect {
      case hint: LegacyIndexHint => planLegacyHintSeek(IdName(hint.identifier.name), hint, qg.argumentIds)
    }
  }
}
