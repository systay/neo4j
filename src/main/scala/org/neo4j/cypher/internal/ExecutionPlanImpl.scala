/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal

import commands._
import scala.collection.JavaConverters._
import org.neo4j.graphdb._
import collection.Seq
import java.lang.Iterable
import org.neo4j.cypher.internal.pipes._
import org.neo4j.cypher._
import org.neo4j.tooling.GlobalGraphOperations

class ExecutionPlanImpl(query: Query, graph: GraphDatabaseService) extends ExecutionPlan {
  val (executionPlan, executionPlanText) = prepareExecutionPlan()

  def execute(params: Map[String, Any]): ExecutionResult = {
    val plan = executionPlan(params)
    plan
  }


  private def prepareExecutionPlan(): ((Map[String, Any]) => PipeExecutionResult, String) = {
    query match {
      case Query(returns, start, matching, where, aggregation, sort, slice, namedPaths, queryText) => {
        var sorted = false
        var aggregated = false
        val predicates = where match {
          case None => Seq()
          case Some(w) => w.atoms
        }

        val paramPipe = new ParameterPipe()
        val pipe = createSourcePumps(paramPipe, start.startItems.toList)

        var context = new CurrentContext(pipe, predicates)
        context = addFilters(context)

        context = createMatchPipe(matching, namedPaths, context)

        context.pipe = createShortestPathPipe(context.pipe, matching, namedPaths)
        context = addFilters(context)

        namedPaths match {
          case None =>
          case Some(x) => x.paths.foreach(p => context.pipe = new NamedPathPipe(context.pipe, p))
        }

        if (context.predicates.nonEmpty) {
          context.pipe = new FilterPipe(context.pipe, context.predicates.reduceLeft(_ ++ _))
        }


        (aggregation, sort) match {
          case (Some(agg), Some(sorting)) => {
            val sortExpressions = sorting.sortItems.map(_.expression)
            val keyExpressions = returns.returnItems.map(_.expression).filterNot(_.containsAggregate)

            if (canUseOrderedAggregation(sortExpressions, keyExpressions)) {

              val keyColumnsNotAlreadySorted = returns.
                returnItems.
                filterNot(ri => sortExpressions.contains(ri.columnName))
                .map(x => SortItem(x.expression, true))

              val newSort = Some(Sort(sorting.sortItems ++ keyColumnsNotAlreadySorted: _*))

              context.pipe = new ExtractPipe(context.pipe, keyExpressions)
              createSortPipe(newSort, keyExpressions, context)
              context.pipe = new OrderedAggregationPipe(context.pipe, keyExpressions, agg.aggregationItems)
              sorted = true
              aggregated = true
            }
          }
          case _ =>
        }

        if (!aggregated) {
          if (aggregation.nonEmpty) {
            val aggr = aggregation.get
            val keyExpressions = returns.returnItems.map(_.expression).filterNot(_.containsAggregate)

            context.pipe = new ExtractPipe(context.pipe, keyExpressions)
            context.pipe = new EagerAggregationPipe(context.pipe, keyExpressions, aggr.aggregationItems)


            val notKeyAndNotAggregate = returns.returnItems.map(_.expression).filterNot(keyExpressions.contains)

            if (notKeyAndNotAggregate.nonEmpty) {
              val rewritten = notKeyAndNotAggregate.map(e => {
                e.rewrite {
                  case x: AggregationExpression => Entity(x.identifier.name)
                  case x => x
                }
              })

              context.pipe = new ExtractPipe(context.pipe, rewritten)
            }

          }
          else {
            context.pipe = new ExtractPipe(context.pipe, returns.returnItems.map(_.expression))
          }
        }

        if (!sorted) {
          createSortPipe(sort, returns.returnItems.map(_.expression), context)
        }

        slice match {
          case None =>
          case Some(x) => context.pipe = new SlicePipe(context.pipe, x.from, x.limit)
        }

        val result = new ColumnFilterPipe(context.pipe, returns.returnItems)

        val func = (params: Map[String, Any]) => {
          val start = System.currentTimeMillis()
          val results = result.createResults(params)
          val timeTaken = System.currentTimeMillis() - start

          new PipeExecutionResult(results, result.symbols, returns.columns, timeTaken)
        }
        val executionPlan = result.executionPlan()

        (func, executionPlan)
      }
    }
  }
  private def createSortPipe(sort: Option[Sort], allReturnItems: Seq[Expression], context: CurrentContext) {
    sort match {
      case None =>
      case Some(s) => {

        val sortItems = s.sortItems.map(_.expression).filterNot(allReturnItems contains)
        if (sortItems.nonEmpty) {
          context.pipe = new ExtractPipe(context.pipe, sortItems)
        }
        context.pipe = new SortPipe(context.pipe, s.sortItems.toList)
      }
    }
  }

  private def addFilters(context: CurrentContext): CurrentContext = {
    if (context.predicates.isEmpty) {
      context
    }
    else {
      val matchingPredicates = context.predicates.filter(x => {

        val unsatisfiedDependencies = x.dependencies.filterNot(context.pipe.symbols contains)
        unsatisfiedDependencies.isEmpty
      })

      if (matchingPredicates.isEmpty) {
        context
      }
      else {
        val filterPredicate = matchingPredicates.reduceLeft(_ ++ _)
        val p = new FilterPipe(context.pipe, filterPredicate)

        new CurrentContext(p, context.predicates.filterNot(matchingPredicates contains))
      }
    }
  }

  private def createMatchPipe(unnamedPaths: Option[Match], namedPaths: Option[NamedPaths], context: CurrentContext): CurrentContext = {
    val namedPattern = namedPaths match {
      case Some(m) => m.paths.flatten
      case None => Seq()
    }

    val unnamedPattern = unnamedPaths match {
      case Some(m) => m.patterns
      case None => Seq()
    }

    (unnamedPattern ++ namedPattern) match {
      case Seq() =>
      case x => context.pipe = new MatchPipe(context.pipe, x, context.predicates)
    }

    context
  }

  private def createShortestPathPipe(source: Pipe, matching: Option[Match], namedPaths: Option[NamedPaths]): Pipe = {
    val unnamedShortestPaths = matching match {
      case Some(m) => m.patterns.filter(_.isInstanceOf[ShortestPath]).map(_.asInstanceOf[ShortestPath])
      case None => Seq()
    }

    val namedShortestPaths = namedPaths match {
      case Some(m) => m.paths.flatMap(_.pathPattern).filter(_.isInstanceOf[ShortestPath]).map(_.asInstanceOf[ShortestPath])
      case None => Seq()
    }

    val shortestPaths = unnamedShortestPaths ++ namedShortestPaths

    var result = source
    shortestPaths.foreach(p => {
      if (p.single)
        result = new SingleShortestPathPipe(result, p)
      else
        result = new AllShortestPathsPipe(result, p)
    })
    result

  }

  private def createSourcePumps(pipe: Pipe, items: List[StartItem]): Pipe = {
    items match {
      case head :: tail => createSourcePumps(createStartPipe(pipe, head), tail)
      case Seq() => pipe
    }
  }

  private def createStartPipe(lastPipe: Pipe, item: StartItem): Pipe = item match {
    case NodeByIndex(varName, idxName, key, value) =>
      new NodeStartPipe(lastPipe, varName, m => {
        val keyVal = key(m).toString
        val valueVal = value(m)
        val indexHits: Iterable[Node] = graph.index.forNodes(idxName).get(keyVal, valueVal)
        indexHits.asScala
      })

    case RelationshipByIndex(varName, idxName, key, value) =>
      new RelationshipStartPipe(lastPipe, varName, m => {
        val keyVal = key(m).toString
        val valueVal = value(m)
        val indexHits: Iterable[Relationship] = graph.index.forRelationships(idxName).get(keyVal, valueVal)
        indexHits.asScala
      })

    case NodeByIndexQuery(varName, idxName, query) =>
      new NodeStartPipe(lastPipe, varName, m => {
        val queryText = query(m)
        val indexHits: Iterable[Node] = graph.index.forNodes(idxName).query(queryText)
        indexHits.asScala
      })

    case RelationshipByIndexQuery(varName, idxName, query) =>
      new RelationshipStartPipe(lastPipe, varName, m => {
        val queryText = query(m)
        val indexHits: Iterable[Relationship] = graph.index.forRelationships(idxName).query(queryText)
        indexHits.asScala
      })

    case NodeById(varName, valueGenerator) => new NodeStartPipe(lastPipe, varName, m => makeNodes[Node](valueGenerator(m), varName, graph.getNodeById))
    case AllNodes(identifierName) => new NodeStartPipe(lastPipe, identifierName, m => GlobalGraphOperations.at(graph).getAllNodes.asScala)
    case AllRelationships(identifierName) => new RelationshipStartPipe(lastPipe, identifierName, m => GlobalGraphOperations.at(graph).getAllRelationships.asScala)
    case RelationshipById(varName, id) => new RelationshipStartPipe(lastPipe, varName, m => makeNodes[Relationship](id(m), varName, graph.getRelationshipById))
  }

  private def canUseOrderedAggregation(sortColumns: Seq[Expression], keyColumns: Seq[Expression]): Boolean = keyColumns.take(sortColumns.size) == sortColumns

  private def makeNodes[T](data: Any, name: String, getElement: Long => T): Seq[T] = {
    def castElement(x: Any): T = x match {
      case i: Int => getElement(i)
      case i: Long => getElement(i)
      case i: String => getElement(i.toLong)
      case element: T => element
    }

    data match {
      case result: Int => Seq(getElement(result))
      case result: Long => Seq(getElement(result))
      case result: java.lang.Iterable[_] => result.asScala.map(castElement).toSeq
      case result: Seq[_] => result.map(castElement).toSeq
      case element: PropertyContainer => Seq(element.asInstanceOf[T])
      case x => throw new ParameterWrongTypeException("Expected a propertycontainer or number here, but got: " + x.toString)
    }
  }

  override def toString = executionPlanText
}

private class CurrentContext(var pipe: Pipe, var predicates: Seq[Predicate])