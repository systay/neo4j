/*
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
package org.neo4j.cypher.internal.compiler.v3_0.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.plannerQuery.PatternConverters._
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_0.{SemanticDirection, InternalException}
import org.neo4j.cypher.internal.frontend.v3_0.ast._

object ClauseConverters {

  implicit class OptionalWhereConverter(val optWhere: Option[Where]) extends AnyVal {
    def asSelections = Selections(optWhere.
      map(_.expression.asPredicates).
      getOrElse(Set.empty))
  }

  implicit class SelectionsSubQueryExtraction(val selections: Selections) extends AnyVal {
    def getContainedPatternExpressions: Set[PatternExpression] =
      selections.flatPredicates.flatMap(_.extractPatternExpressions).toSet
  }

  implicit class SortItems(val optOrderBy: Option[OrderBy]) extends AnyVal {
    def asQueryShuffle = {
      val sortItems: Seq[SortItem] = optOrderBy.fold(Seq.empty[SortItem])(_.sortItems)
      QueryShuffle(sortItems, None, None)
    }
  }

  implicit class ReturnItemConverter(val items: Seq[ReturnItem]) extends AnyVal {
    def asQueryProjection(distinct: Boolean): QueryProjection = {
      val (aggregatingItems: Seq[ReturnItem], groupingKeys: Seq[ReturnItem]) =
        items.partition(item => IsAggregate(item.expression))

      def turnIntoMap(x: Seq[ReturnItem]) = x.map(e => e.name -> e.expression).toMap

      val projectionMap = turnIntoMap(groupingKeys)
      val aggregationsMap = turnIntoMap(aggregatingItems)

      if (projectionMap.values.exists(containsAggregate))
        throw new InternalException("Grouping keys contains aggregation. AST has not been rewritten?")

      if (aggregationsMap.nonEmpty || distinct)
        AggregatingQueryProjection(groupingKeys = projectionMap, aggregationExpressions = aggregationsMap)
      else
        RegularQueryProjection(projections = projectionMap)
    }
  }

  implicit class ClauseConverter(val clause: Clause) extends AnyVal {
    def addToLogicalPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = clause match {
      case c: Return => c.addReturnToLogicalPlanInput(acc)
      case c: Match => c.addMatchToLogicalPlanInput(acc)
      case c: With => c.addWithToLogicalPlanInput(acc)
      case c: Unwind => c.addUnwindToLogicalPlanInput(acc)
      case c: Start => c.addStartToLogicalPlanInput(acc)
      case c: Create => c.addCreateToLogicalPlanInput(acc)
      case x         => throw new CantHandleQueryException(s"$x is not supported by the new runtime yet")
    }
  }

  implicit class ReturnConverter(val clause: Return) extends AnyVal {
    def addReturnToLogicalPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = clause match {
      case Return(distinct, ri, optOrderBy, skip, limit) if !ri.includeExisting =>

        val shuffle = optOrderBy.asQueryShuffle.
          withSkip(skip).
          withLimit(limit)

        val projection = ri.items.
          asQueryProjection(distinct).
          withShuffle(shuffle)
        val returns = ri.items.collect {
          case AliasedReturnItem(_, identifier) => IdName.fromIdentifier(identifier)
        }
        acc.
          withHorizon(projection).
          withReturns(returns)
      case _ =>
        throw new InternalException("AST needs to be rewritten before it can be used for planning. Got: " + clause)
    }
  }

  implicit class CreateConverter(val clause: Create) extends AnyVal {
    def addCreateToLogicalPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = {
      clause.pattern.patternParts.foldLeft(acc) {
        //CREATE (n :L1:L2 {prop: 42})
        case (builder, EveryPath(NodePattern(Some(id), labels, props))) =>
          builder
            .amendUpdateGraph(ug => ug.addNodePatterns(CreateNodePattern(IdName.fromIdentifier(id), labels, props)))

        //CREATE (n)-[r: R]->(m)
        case (builder, EveryPath(pattern@RelationshipChain(
          NodePattern(Some(leftId), labelsLeft, propsLeft),
          RelationshipPattern(Some(relId), _, types, _, properties, direction),
          NodePattern(Some(rightId), labelsRight, propsRight)))) =>

          val (start, end) = if (direction == SemanticDirection.OUTGOING) (leftId, rightId) else (rightId, leftId)

          //create nodes that are not already matched or created
          val nodesToCreate = Seq(
            CreateNodePattern(IdName.fromIdentifier(leftId), labelsLeft, propsLeft),
            CreateNodePattern(IdName.fromIdentifier(rightId), labelsRight, propsRight)
          ).filterNot(pattern => builder.allSeenPatternNodes(pattern.nodeName))

          builder
            .amendUpdateGraph(ug => ug
              .addNodePatterns(nodesToCreate:_*)
              .addRelPatterns(
                CreateRelationshipPattern(IdName.fromIdentifier(relId), IdName.fromIdentifier(start),
                  types.head, IdName.fromIdentifier(end), properties)))

        case _ => throw new CantHandleQueryException(s"$clause is not yet supported")
      }
    }

  }

  implicit class ReturnItemsConverter(val clause: ReturnItems) extends AnyVal {
    def asReturnItems(current: QueryGraph): Seq[ReturnItem] =
      if (clause.includeExisting)
        QueryProjection.forIds(current.allCoveredIds) ++ clause.items
      else
        clause.items

    def getContainedPatternExpressions: Set[PatternExpression] =
      clause.items.flatMap(_.expression.extractPatternExpressions).toSet
  }

  implicit class MatchConverter(val clause: Match) extends AnyVal {
    def addMatchToLogicalPlanInput(acc: PlannerQueryBuilder): PlannerQueryBuilder = {
      val patternContent = clause.pattern.destructed

      val selections = clause.where.asSelections

      if (clause.optional) {
        acc.
          amendQueryGraph { qg => qg.withAddedOptionalMatch(
          // When adding QueryGraphs for optional matches, we always start with a new one.
          // It's either all or nothing per match clause.
          QueryGraph(
            selections = selections,
            patternNodes = patternContent.nodeIds.toSet,
            patternRelationships = patternContent.rels.toSet,
            hints = clause.hints.toSet,
            shortestPathPatterns = patternContent.shortestPaths.toSet
          ))
        }
      } else {
        acc.amendQueryGraph {
          qg => qg.
            addSelections(selections).
            addPatternNodes(patternContent.nodeIds: _*).
            addPatternRelationships(patternContent.rels).
            addHints(clause.hints).
            addShortestPaths(patternContent.shortestPaths: _*)
        }
      }
    }
  }

  implicit class WithConverter(val clause: With) extends AnyVal {

    private implicit def returnItemsToIdName(s: Seq[ReturnItem]):Set[IdName] =
      s.map(item => IdName(item.name)).toSet

    def addWithToLogicalPlanInput(builder: PlannerQueryBuilder): PlannerQueryBuilder = clause match {

      /*
      When encountering a WITH that is not an event horizon, and we have no optional matches in the current QueryGraph,
      we simply continue building on the current PlannerQuery. Our ASTRewriters rewrite queries in such a way that
      a lot of queries have these WITH clauses.

      Handles: ... WITH * [WHERE <predicate>] ...
       */
      case With(false, ri, None, None, None, where)
        if !builder.currentQueryGraph.hasOptionalPatterns
          && ri.items.forall(item => !containsAggregate(item.expression))
          && ri.items.forall {
          case item: AliasedReturnItem => item.expression == item.identifier
          case x => throw new InternalException("This should have been rewritten to an AliasedReturnItem.")
        } && builder.readOnly =>
        val selections = where.asSelections
        builder.
          amendQueryGraph(_.addSelections(selections))

      /*
      When encountering a WITH that is an event horizon, we introduce the horizon and start a new empty QueryGraph.

      Handles all other WITH clauses
       */
      case With(distinct, projection, orderBy, skip, limit, where) =>
        val selections = where.asSelections
        val returnItems = projection.asReturnItems(builder.currentQueryGraph)

        val shuffle =
          orderBy.
            asQueryShuffle.
            withLimit(limit).
            withSkip(skip)

        val queryProjection =
          returnItems.
            asQueryProjection(distinct).
            withShuffle(shuffle)

        builder.
          withHorizon(queryProjection).
          withTail(PlannerQuery(QueryGraph(selections = selections)))


      case _ =>
        throw new InternalException("AST needs to be rewritten before it can be used for planning. Got: " + clause)    }
  }

  implicit class UnwindConverter(val clause: Unwind) extends AnyVal {

    def addUnwindToLogicalPlanInput(builder: PlannerQueryBuilder): PlannerQueryBuilder =
      builder.
        withHorizon(
          UnwindProjection(
            identifier = IdName(clause.identifier.name),
            exp = clause.expression)
        ).
        withTail(PlannerQuery.empty)
  }

  implicit class StartConverter(val clause: Start) extends AnyVal {
    def addStartToLogicalPlanInput(builder: PlannerQueryBuilder): PlannerQueryBuilder = {
        builder.amendQueryGraph { qg =>
          val items = clause.items.map {
            case hints: LegacyIndexHint => Right(hints)
            case item              => Left(item)
          }

          val hints = items.collect { case Right(hint) => hint }
          val nonHints = items.collect { case Left(item) => item }

          if (nonHints.nonEmpty) {
            // all other start queries is delegated to legacy planner
            throw new CantHandleQueryException()
          }

          val nodeIds = hints.collect { case n: NodeHint => IdName(n.identifier.name)}

          val selections = clause.where.asSelections

          qg.addPatternNodes(nodeIds: _*)
            .addSelections(selections)
            .addHints(hints)
        }
    }
  }
}
