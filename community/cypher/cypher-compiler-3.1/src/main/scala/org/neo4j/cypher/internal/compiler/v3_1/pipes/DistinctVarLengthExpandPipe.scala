/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.collection.primitive.{Primitive, PrimitiveLongSet}
import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, ReadsAllNodes, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.{InternalException, SemanticDirection}
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.mutable

case class DistinctVarLengthExpandPipe(source: Pipe, fromName: String, toName: String, types: LazyTypes,
                                       dir: SemanticDirection, min: Int, max: Int)
                                      (val estimatedCardinality: Option[Double] = None)
                                      (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {
  /*
    Algorithm:

    (Start) --> (Load Next) -[seen (emptied if grouped by startnode)]->


   *  * Depth first until we reach the minimal length - this way we can look at the full path and make sure we are not
   *    traversing the same rel twice.
   *
   *    Once min length has been achieved, we iterate over the relationships and filter out any
   *
   *  * A
   */


  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    state.decorator.registerParentPipe(this)



    trait State {
      def next(): (State, Option[Node])
    }

    case object Empty extends State {
      override def next() = (this, None)
    }

    case class DFS(stateWhenEmptied: State, path: Array[Long], rels: Iterator[Relationship], seen: PrimitiveLongSet, depth: Int) extends State {
      override def next(): (State, Option[Node]) = {
        ???
      }
    }

    case class BFS(nextSet: PrimitiveLongSet, seen: PrimitiveLongSet) extends State {
      override def next(): (State, Option[Node]) = {
        (this, None)
      }

    }

    case object LoadNext extends State {
      override def next(): (State, Option[Node]) = {
        if (input.hasNext) {
          val row = input.next()
          val from = row.getOrElse(fromName, throw new InternalException(s"Expected a node on `$fromName`")).asInstanceOf[Node]
          val relationships = state.query.getRelationshipsForIds(from.getId, dir, types.types(state.query))
          DFS(this, new Array[Long](max), relationships, Primitive.longSet(), 1).next
        } else {
          (Empty, None)
        }
      }
    }




    new Iterator[ExecutionContext] {

      prepareForNextStartNode()
      val todo = new mutable.Stack[Node]()
      var seen = Primitive.longSet()

      def prepareForNextStartNode(): Unit = {
        if (input.hasNext) {
          val row = input.next()
          val from = row.getOrElse(fromName, throw new InternalException(s"Expected a node on `$fromName`"))
          from match {
            case n: Node =>

            case null => prepareForNextStartNode()
          }
          ???
        } else {

        }
      }

      override def hasNext = ???

      override def next() = ???
    }
  }

  override def planDescriptionWithoutCardinality = {
    val expandSpec = ExpandExpression(fromName, "", types.names, toName, dir, varLength = true)
    source.planDescription.andThen(this.id, s"VarLengthExpand(Distinct)", variables, expandSpec)
  }

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]) = {
    val (head :: Nil) = sources
    copy(head)(estimatedCardinality)
  }

  override def symbols = source.symbols.add(toName, CTNode)

  override def localEffects = Effects(ReadsAllNodes, ReadsAllRelationships)
}
