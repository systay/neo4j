/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes.matching

import org.neo4j.cypher.{SyntaxException, SymbolTable}
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.cypher.commands._
import collection.immutable.Map

class MatchingContext(patterns: Seq[Pattern], boundIdentifiers: SymbolTable) {
  type PatternGraph = Map[String, PatternElement]

  val patternGraph: PatternGraph = buildPatternGraph()

  def createNullValuesForOptionalElements(matchedGraph: Map[String, Any]): Map[String, Null] = {
    (patternGraph.keySet -- matchedGraph.keySet).map(_ -> null).toMap
  }

  def getMatches(bindings: Map[String, Any]): Traversable[Map[String, Any]] = {
    val (pinnedName, pinnedNode) = bindings.head

    val pinnedPatternNode = patternGraph(pinnedName).asInstanceOf[PatternNode]

    val boundPairs = bindings.map(kv => {
      val patternElement = patternGraph(kv._1)
      val pair = kv._2 match {
        case node: Node => MatchingPair(patternElement, node)
        case rel: Relationship => MatchingPair(patternElement, rel)
      }

      kv._1 -> pair
    })

    pinnedPatternNode.pin(pinnedNode.asInstanceOf[Node])

    new PatternMatcher(pinnedPatternNode, boundPairs).map(matchedGraph => {
      matchedGraph ++ createNullValuesForOptionalElements(matchedGraph)
    })
  }

  /*
  This method is mutable, but it is only called from the constructor of this class. The created pattern graph
   is immutable and thread safe.
   */
  private def buildPatternGraph(): PatternGraph = {
    val patternNodeMap: scala.collection.mutable.Map[String, PatternNode] = scala.collection.mutable.Map()
    val patternRelMap: scala.collection.mutable.Map[String, PatternRelationship] = scala.collection.mutable.Map()

    boundIdentifiers.identifiers.foreach(_ match {
      case NodeIdentifier(nodeName) => patternNodeMap(nodeName) = new PatternNode(nodeName)
      case _ => None
    })

    patterns.foreach(_ match {
      case RelatedTo(left, right, rel, relType, dir, optional) => {
        val leftNode: PatternNode = patternNodeMap.getOrElseUpdate(left, new PatternNode(left))
        val rightNode: PatternNode = patternNodeMap.getOrElseUpdate(right, new PatternNode(right))

        patternRelMap(rel) = leftNode.relateTo(rel, rightNode, relType, dir, optional)
      }
      case VarLengthRelatedTo(pathName, start, end, minHops, maxHops, relType, dir, optional) => {
        val startNode: PatternNode = patternNodeMap.getOrElseUpdate(start, new PatternNode(start))
        val endNode: PatternNode = patternNodeMap.getOrElseUpdate(end, new PatternNode(end))
        patternRelMap(pathName) = startNode.relateViaVariableLengthPathTo(pathName, endNode, minHops, maxHops, relType, dir, optional)
      }
      case _ =>
    })

    validatePattern(patternNodeMap.toMap, patternRelMap.toMap, boundIdentifiers)

    val pattern = (patternNodeMap.values ++ patternRelMap.values).toSeq
    pattern.map(x => x.key -> x).toMap
  }

  def validatePattern( patternNodes: Map[String, PatternNode],
                       patternRels: Map[String, PatternRelationship],
                       bindings: SymbolTable):Map[String,PatternElement] = {
    val overlaps = patternNodes.keys.filter(patternRels.keys.toSeq contains)
    if(overlaps.nonEmpty) {
      throw new SyntaxException("Some identifiers are used as both relationships and nodes: " + overlaps.mkString(", "))
    }

    val elementsMap: Map[String, PatternElement] = (patternNodes.values ++ patternRels.values).map(x => (x.key -> x)).toMap
    var visited = scala.collection.mutable.Seq[PatternElement]()

    def visit(x: PatternElement) {
      if (!visited.contains(x)) {
        visited = visited ++ Seq(x)
        x match {
          case nod: PatternNode => nod.relationships.foreach(visit)
          case rel: PatternRelationship => {
            visit(rel.leftNode)
            visit(rel.rightNode)
          }
        }
      }
    }

    bindings.identifiers.foreach(id => {
      val el = elementsMap.get(id.name)
      el match {
        case None =>
        case Some(x) => visit(x)
      }
    })

    val notVisited = elementsMap.values.filterNot(visited contains)

    if (notVisited.nonEmpty) {
      throw new SyntaxException("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " + notVisited.map(_.key).mkString("", ", ", ""))
    }

    elementsMap
  }

}