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
package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.{LogicalPlan, NodeHashJoin}
import org.neo4j.cypher.internal.frontend.v3_2.InternalException
import org.neo4j.cypher.internal.ir.v3_2.IdName

import scala.collection.mutable

object RegisterAllocator {

  type KEY = (LogicalPlan, PipeLine.Direction)
  type VALUE = PipeLine

  def calculate(root: LogicalPlan): Map[KEY, VALUE] = {

    val planStack = new mutable.Stack[LogicalPlan]()
    var comingFrom: LogicalPlan = null
    var currentPipeLine: PipeLine = new PipeLine()
    val resultBuilder = new mutable.MapBuilder[KEY, VALUE, Map[KEY, VALUE]](Map[KEY, VALUE]())

    def populate(plan: LogicalPlan) = {
      var current = plan
      while (!current.isLeaf) {
        planStack.push(current)
        current = current.lhs.getOrElse(
          throw new InternalException("This must not be! Found plan that clams to be leaf but has no LHS"))
      }
      comingFrom = current
      planStack.push(current)
    }

    populate(root)

    while (planStack.nonEmpty) {
      val current = planStack.pop()
      resultBuilder += ((current, PipeLine.In) -> currentPipeLine)

      (current, current.lhs, current.rhs) match {
        case (_, _, None) =>
          currentPipeLine = handle(current, currentPipeLine)
          resultBuilder += ((current, PipeLine.Out) -> currentPipeLine)

        case (_:NodeHashJoin, _, Some(rhs)) =>
          currentPipeLine = handle(current, currentPipeLine)
          resultBuilder += ((current, PipeLine.Out) -> currentPipeLine)


        //        case (Some(left), Some(_)) if comingFrom eq left =>
        //          val arg1 = outputStack.pop()
        //          val arg2 = outputStack.pop()
        //          val output = build(current, arg1, arg2, currentContext)
        //
        //          outputStack.push(output)
        //???
        //        case (Some(left), Some(right)) if comingFrom eq right =>
        //          planStack.push(current)
        //          populate(left)
        //          ???
      }

      comingFrom = current
    }

    resultBuilder.result()
  }


  private def handle(plan: LogicalPlan, in: PipeLine): PipeLine = plan match {
    case plans.AllNodesScan(id, args) => in.addLong(id)
    case plans.Expand(_, from, _, _, to, relName, _) => in.addLong(relName).addLong(to)
  }

//  private def build(plan: LogicalPlan, source: AllocationContext, context: AllocationContext): AllocationResult = ???
//
//  private def build(plan: LogicalPlan, lhs: AllocationContext, rhs: AllocationContext, context: AllocationContext): AllocationResult = ???
//
//  case class AllocationResult(contezt: AllocationContext, addedSlots: Seq[(IdName, Slot)])

}


/**
  * Contains information about how to map variables to slots, and what registers need to be created for the pipeline
  */
class PipeLine(var slots: Map[IdName, Slot] = Map.empty, var numberOfLongs: Int = 0, var numberOfObjs: Int = 0) {
  def addLong(id: IdName): PipeLine = {
    val newSlot = Long(numberOfLongs)
    slots = slots + (id -> newSlot)
    numberOfLongs = numberOfLongs + 1
    this
  }

  def addObj(id: IdName): PipeLine = {
    val newSlot = Obj(numberOfObjs)
    slots = slots + (id -> newSlot)
    numberOfObjs = numberOfObjs + 1
    this
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PipeLine]

  override def equals(other: Any): Boolean = other match {
    case that: PipeLine =>
      (that canEqual this) &&
        slots == that.slots &&
        numberOfLongs == that.numberOfLongs &&
        numberOfObjs == that.numberOfObjs
    case _ => false
  }

  override def hashCode(): Int = {
    (slots.hashCode() * 31 + numberOfLongs) * 31 + numberOfObjs
  }

  override def toString: String = {
    val slotsString = slots.map { case (k, v) => s"${k.name} -> $v" }.mkString(",")
    s"PipeLine(longs: $numberOfLongs, refs: $numberOfObjs, mappings: $slotsString)"
  }
}

object PipeLine {

  trait Direction

  case object In extends Direction

  case object Out extends Direction

}

trait Slot
case class Long(offset: Int) extends Slot
case class Obj(offset: Int) extends Slot
