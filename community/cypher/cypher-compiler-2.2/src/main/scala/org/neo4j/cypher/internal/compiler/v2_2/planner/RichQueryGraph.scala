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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2.ast.Hint
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship}

import scala.collection.mutable

object RichQueryGraph {

  // Maybe this class should be merged with QueryGraph. Unsure
  implicit class RichQueryGraph(inner: QueryGraph) {
    def size = inner.patternRelationships.size

    /**
     * Returns the connected patterns of this query graph where each connected pattern is represented by a QG.
     * Does not include optional matches, shortest paths or predicates that have dependencies across multiple of the
     * connected query graphs.
     */
    def connectedComponents: Seq[QueryGraph] = {
      val visited = mutable.Set.empty[IdName]
      inner.patternNodes.toSeq.collect {
        case patternNode if !visited(patternNode) =>
          val qg = connectedComponentFor(patternNode, visited)
          val coveredIds = qg.coveredIds
          val predicates = inner.selections.predicates.filter(_.dependencies.subsetOf(coveredIds))
          val arguments = inner.argumentIds.filter(coveredIds)
          val hints = inner.hints.filter(h => coveredIds.contains(IdName(h.identifier.name)))
          val shortestPaths = inner.shortestPathPatterns.filter {
            p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
          }
          qg.
            withSelections(Selections(predicates)).
            withArgumentIds(arguments).
            addHints(hints).
            addShortestPaths(shortestPaths.toSeq: _*)
      }.distinct
    }

    def --(other: QueryGraph): QueryGraph = {
      val remainingRels: Set[PatternRelationship] = inner.patternRelationships -- other.patternRelationships
      val hints = inner.hints -- other.hints
      createSubQueryWithRels(remainingRels, hints)
    }

    def combinations(size: Int): Seq[QueryGraph] = if (size < 0 || size > inner.patternRelationships.size)
      throw new IndexOutOfBoundsException(s"Expected $size to be in [0,${inner.patternRelationships.size}[")
    else
      if (size == 0) {
        patternNodesAndArguments
      } else {
        inner.
        patternRelationships.toSeq.combinations(size).
        map(r => createSubQueryWithRels(r.toSet, inner.hints)).toSeq
      }

    private def patternNodesAndArguments = {
      val nonArgs = inner.
                    patternNodes.filterNot(inner.argumentIds).
                    map(createSubQueryWithNode(_, inner.argumentIds, inner.hints)).toSeq

      val args = if (argumentsOverlapsWithNodes)
        Some(queryGraphWithArguments)
      else
        None

      nonArgs ++ args
    }

    def queryGraphWithArguments: QueryGraph = {
      val filteredHints = inner.hints.filter(h => inner.argumentIds.contains(IdName(h.identifier.name)))

      val graph = QueryGraph(
        patternNodes = inner.patternNodes.filter(inner.argumentIds),
        argumentIds = inner.argumentIds,
        selections = Selections.from(inner.selections.predicatesGiven(inner.argumentIds): _*),
        hints = filteredHints
      )
      graph
    }

    private def argumentsOverlapsWithNodes = (inner.argumentIds intersect inner.patternNodes).nonEmpty

    private def connectedComponentFor(startNode: IdName, visited: mutable.Set[IdName]): QueryGraph = {
      val queue = mutable.Queue(startNode)
      val argumentNodes = inner.patternNodes intersect inner.argumentIds
      var qg = QueryGraph(argumentIds = inner.argumentIds, patternNodes = argumentNodes)
      while (queue.nonEmpty) {
        val node = queue.dequeue()
        qg = if (visited(node)) {
          qg
        } else {
          visited += node

          val patternRelationships = inner.patternRelationships.filter { rel =>
            rel.coveredIds.contains(node) && !qg.patternRelationships.contains(rel)
          }

          queue.enqueue(patternRelationships.toSeq.map(_.otherSide(node)): _*)

          qg
          .addPatternNodes(node)
          .addPatternRelationships(patternRelationships.toSeq)
        }
      }
      qg
    }

    private def createSubQueryWithRels(rels: Set[PatternRelationship], hints: Set[Hint]) = {
      val nodes = rels.map(r => Seq(r.nodes._1, r.nodes._2)).flatten.toSet
      val filteredHints = hints.filter(h => nodes(IdName(h.identifier.name)))

      val qg = QueryGraph(
        patternNodes = nodes,
        argumentIds = inner.argumentIds,
        patternRelationships = rels,
        hints = filteredHints
      )

      val shortestPaths = inner.shortestPathPatterns.filter(p => p.isFindableFrom(qg.coveredIds))

      qg.
        withSelections(selections = Selections.from(inner.selections.predicatesGiven(qg.coveredIds): _*)).
        withShortestPaths(shortestPaths)
    }

    private def createSubQueryWithNode(id: IdName, argumentIds: Set[IdName], hints: Set[Hint]) = {
      val filteredHints = hints.filter(id.name == _.identifier.name)

      QueryGraph(
        patternNodes = Set(id),
        argumentIds = argumentIds,
        selections = Selections.from(inner.selections.predicatesGiven(Set(id)): _*),
        hints = filteredHints
      )
    }
  }
}
