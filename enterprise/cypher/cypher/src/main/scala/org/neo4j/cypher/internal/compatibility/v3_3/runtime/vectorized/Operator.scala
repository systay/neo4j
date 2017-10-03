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
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

trait Initiable {
  def init(state: QueryState, context: QueryContext): Unit
}

trait Operator extends Initiable {
  def operate(data: Morsel, context: QueryContext, state: QueryState): ReturnType
}

trait LeafOperator extends Initiable {
  def operate(source: Continuation,
              output: Morsel,
              context: QueryContext,
              state: QueryState): (ReturnType, Continuation)
}

/*
A continuation is an abstract representation of program state. It allows us to pause the iteration of something and
schedule the continuation of this iteration for a later point in time.
 */
sealed trait Continuation
case object Init extends Continuation
case class InitWithData(data: Morsel) extends Continuation
case class ContinueWithData(data: Morsel, index: Int) extends Continuation
case class ContinueWithDataAndSource[T](data: Morsel, index: Int, source: T) extends Continuation
case class ContinueWithSource[T](source: T) extends Continuation
case object Done extends Continuation

/*
The return type allows an operator to signal if the a morsel it has operated on contains interesting information or not
 */
sealed trait ReturnType
object MorselType extends ReturnType
object UnitType extends ReturnType

class QueryState(val operatorState: mutable.Map[Initiable, AnyRef] = mutable.Map[Initiable, AnyRef](),
                 val params: MapValue)

case class Pipeline(startOperator: LeafOperator,
                    operators: Seq[Operator],
                    slotInformation: PipelineInformation,
                    dependencies: Set[Pipeline])
                   (var parent: Option[Pipeline] = None) {

  def endPipeline: Boolean = parent.isEmpty

  def addOperator(operator: Operator): Pipeline = copy(operators = operators :+ operator)(parent)

  def init(queryState: QueryState, queryContext: QueryContext): Unit = {
    startOperator.init(queryState, queryContext)
    operators.foreach(_.init(queryState, queryContext))
  }

  def operate(continue: Continuation, data: Morsel, context: QueryContext, state: QueryState): (ReturnType, Continuation) = {

    val (ret, next) = startOperator.operate(continue, data, context, state)

    val returnType = operators.foldLeft(ret) {
      case (r, op) =>
        op.operate(data, context, state)
    }

    (returnType, next)
  }

  /*
  Walks the tree, setting parent information everywhere so we can push up the tree
   */
  def construct: Pipeline = {
    dependencies.foreach(_.noIamYourFather(this))
    this
  }

  protected def noIamYourFather(daddy: Pipeline): Unit = {
    dependencies.foreach(_.noIamYourFather(this))
    parent = Some(daddy)
  }

  override def toString = {
    val x = (startOperator +: operators).map(x => x.getClass.getSimpleName)
    s"Pipeline(${x.mkString(",")})"
  }
}
