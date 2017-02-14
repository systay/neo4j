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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_2.commands.{QueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.notification.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.ir.v3_2.{IdName, QueryGraph}
import org.neo4j.cypher.internal.compiler.v3_2.IndexDescriptor

abstract class AbstractIndexSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph)(implicit context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    producePlanFor(e: Expression, qg.selections.labelPredicates, qg)
  }

  private def producePlanFor(e: Expression, labelPredicateMap: Map[IdName, Set[HasLabels]], qg: QueryGraph)
                            (implicit context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates
    val arguments: Set[Variable] = qg.argumentIds.map(n => Variable(n.name)(null))
    indexPlannableExpression(qg.argumentIds, arguments, labelPredicateMap, qg.hints)
      .lift(e).flatMap(e => maybeLeafPlans(e.name, e.producePlans()))
  }

  private def indexPlannableExpression(argumentIds: Set[IdName], arguments: Set[Variable],
                                       labelPredicateMap: Map[IdName, Set[HasLabels]], hints: Set[Hint]):
  PartialFunction[Expression, IndexPlannableExpression] = {
    // n.prop IN [ ... ]
    case predicate@AsPropertySeekable(seekable: PropertySeekable)
      if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
      IndexPlannableExpression(seekable.name, seekable.propertyKey, predicate,
        seekable.args.asQueryExpression, labelPredicateMap, hints, argumentIds)

    // ... = n.prop
    // In some rare cases, we can't rewrite these predicates cleanly,
    // and so planning needs to search for these cases explicitly
    case predicate@Equals(a, Property(seekable@Variable(name), propKeyName))
      if a.dependencies.forall(arguments) && !arguments(seekable) =>
      val expr = SingleQueryExpression(a)
      IndexPlannableExpression(seekable.name, propKeyName, predicate,
        expr, labelPredicateMap, hints, argumentIds)

    // n.prop STARTS WITH "prefix%..."
    case predicate@AsStringRangeSeekable(seekable) =>
      IndexPlannableExpression(seekable.name, seekable.propertyKey, PartialPredicate(seekable.expr, predicate),
        seekable.asQueryExpression, labelPredicateMap, hints, argumentIds)

    // n.prop <|<=|>|>= value
    case predicate@AsValueRangeSeekable(seekable) =>
      IndexPlannableExpression(seekable.name, seekable.propertyKeyName, predicate,
        seekable.asQueryExpression, labelPredicateMap, hints, argumentIds)
  }

  def producePlanFor(expressions: Seq[Expression], labelPredicateMap: Map[IdName, Set[HasLabels]], qg: QueryGraph)
                    (implicit context: LogicalPlanningContext): Seq[LeafPlansForVariable] = {
    val arguments: Set[Variable] = qg.argumentIds.map(n => Variable(n.name)(null))
    val plannables: Seq[IndexPlannableExpression] = expressions.collect(
      indexPlannableExpression(qg.argumentIds, arguments, labelPredicateMap, qg.hints))
    val singlePropertyIndexes = plannables.flatMap(p => maybeLeafPlans(p.name, p.producePlans()))
//    val doublePropertyIndexes = plannables.combinations(2).map { pair =>
//      //TODO: find composite plans
//      maybeLeafPlans(pair.head.name, pair.head.producePlans())
//    }.flatten
    singlePropertyIndexes //++ doublePropertyIndexes
  }

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates
    if(labelPredicateMap.isEmpty)
      Seq.empty
    else {
      val predicates: Seq[Expression] = qg.selections.flatPredicates
      val resultPlans: Seq[LogicalPlan] = predicates.flatMap {
        e => producePlanFor(e, labelPredicateMap, qg).toSeq.flatMap(_.plans)
      }
      val compositePlans: Seq[LogicalPlan] = producePlanFor(predicates, labelPredicateMap, qg).flatMap(p => p.plans)

      if (resultPlans.isEmpty && compositePlans.isEmpty) {
        DynamicPropertyNotifier.process(findNonSeekableIdentifiers(qg.selections.flatPredicates), IndexLookupUnfulfillableNotification, qg)
      }

      (resultPlans ++ compositePlans).distinct
    }
  }

  protected def findNonSeekableIdentifiers(predicates: Seq[Expression])(implicit context: LogicalPlanningContext) =
    predicates.flatMap {
      // n['some' + n.prop] IN [ ... ]
      case predicate@AsDynamicPropertyNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      // n['some' + n.prop] STARTS WITH "prefix%..."
      case predicate@AsStringRangeNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      // n['some' + n.prop] <|<=|>|>= value
      case predicate@AsValueRangeNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      case _ => None
    }.toSet

  protected def collectPlans(predicates: Seq[Expression], argumentIds: Set[IdName],
                             labelPredicateMap: Map[IdName, Set[HasLabels]],
                             hints: Set[Hint])
                            (implicit context: LogicalPlanningContext): Seq[(String, Set[LogicalPlan])]  = {
    val arguments: Set[Variable] = argumentIds.map(n => Variable(n.name)(null))
    val results = predicates.collect(indexPlannableExpression(argumentIds, arguments, labelPredicateMap, hints))
    results.map(e => (e.name, e.producePlans()))
  }

  private def producePlansFor(name: String, propertyKeyName: PropertyKeyName,
                              propertyPredicate: Expression, queryExpression: QueryExpression[Expression],
                              labelPredicateMap: Map[IdName, Set[HasLabels]],
                              hints: Set[Hint], argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): Set[LogicalPlan] = {
    IndexPlannableExpression(name, propertyKeyName, propertyPredicate, queryExpression, labelPredicateMap, hints, argumentIds).producePlans()
  }



  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan

  protected def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext): Option[IndexDescriptor]

  case class IndexPlannableExpression(name: String, propertyKeyName: PropertyKeyName,
                                      propertyPredicate: Expression, queryExpression: QueryExpression[Expression],
                                      labelPredicateMap: Map[IdName, Set[HasLabels]],
                                      hints: Set[Hint], argumentIds: Set[IdName]) {
    def producePlans()(implicit context: LogicalPlanningContext): Set[LogicalPlan] = {
      implicit val semanticTable = context.semanticTable
      val idName = IdName(name)
      for (labelPredicate <- labelPredicateMap.getOrElse(idName, Set.empty);
           labelName <- labelPredicate.labels;
           indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName.name);
           labelId <- labelName.id)
        yield {
          val propertyName = propertyKeyName.name
          val hint = hints.collectFirst {
            case hint @ UsingIndexHint(Variable(`name`), `labelName`, PropertyKeyName(`propertyName`)) => hint
          }
          val entryConstructor: (Seq[Expression]) => LogicalPlan =
            constructPlan(idName, LabelToken(labelName, labelId), PropertyKeyToken(propertyKeyName, propertyKeyName.id.head),
              queryExpression, hint, argumentIds)
          entryConstructor(Seq(propertyPredicate, labelPredicate))
        }
    }
  }

}

object uniqueIndexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan =
    (predicates: Seq[Expression]) =>
      context.logicalPlanProducer.planNodeUniqueIndexSeek(idName, label, propertyKey, valueExpr, predicates, hint, argumentIds)

  protected def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext): Option[IndexDescriptor] =
    context.planContext.getUniqueIndexRule(label, property)
}

object indexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan =
    (predicates: Seq[Expression]) =>
      context.logicalPlanProducer.planNodeIndexSeek(idName, label, propertyKey, valueExpr, predicates, hint, argumentIds)

  protected def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext): Option[IndexDescriptor] = {
    if (uniqueIndex(label, property).isDefined) None else anyIndex(label, property)
  }

  private def anyIndex(label: String, property: String)(implicit context: LogicalPlanningContext) = context.planContext.getIndexRule(label, property)
  private def uniqueIndex(label: String, property: String)(implicit context: LogicalPlanningContext) = context.planContext.getUniqueIndexRule(label, property)
}

