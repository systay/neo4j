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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.dispatcher

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class Conductor(minions: Array[Minion], MORSEL_SIZE: Int, queryQueue: ConcurrentLinkedQueue[Query]) extends Runnable {

  // Here we keep track of which query each minion is currently assigned to.
  private val _alive = new AtomicBoolean(true)
  private val ongoingIterationsByQuery = mutable.HashMap[Query, ArrayBuffer[Iteration]]()

  type TaskQueue = mutable.Queue[(Minion, Task)]

  def shutdown(): Unit = {
    minions.foreach {
      minion =>
        minion.input.clear()
        minion.input.add(Task(null, null, ShutdownWorkers, null))
    }
    _alive.set(false)
  }

  override def run(): Unit = {
    while (_alive.get()) {
      try {
        val workQueue = new TaskQueue()
        // For each round, do this:
        // 1. Check if any queries are waiting to be executed.
        //    If there are, we'll queue up work for each query waiting.
        enqueueWaitingQueries(workQueue)

        // 2. Go through each minion and check their output buffers, consuming finished tasks from them,
        //    and queueing up follow up tasks.
        emptyOutputBuffersAndProduceFollowUpWork(minions, workQueue)

        // 3. Figure out which minions should work on which queries, and schedule work to them.
        scheduleWorkOnMinions(minions, workQueue)
      } catch {

        // Someone probably left us a message
        case _: InterruptedException =>

        // Uh-oh... An uncaught exception is not good. Let's kill everything.
        case e: Exception =>
          e.printStackTrace()
          shutdown()
          throw e

      }
    }
  }

  private def emptyOutputBuffersAndProduceFollowUpWork(minions: Array[Minion], workQueue: TaskQueue): Unit = {
    minions foreach {
      minion =>
        while (!minion.output.isEmpty) {
          val resultObject: ResultObject = minion.output.poll()
          createContinuationTask(minion, resultObject, workQueue)
          scheduleMoreWorkOnMorsel(resultObject, workQueue)
        }
    }
  }

  val r = new Random()

  private def scheduleWorkOnMinions(minions: Array[Minion], workQueue: TaskQueue): Unit = {
    workQueue.foreach {
      case (null, t) =>
        minions(r.nextInt(minions.length)).input.add(t)
      case (minion, t) =>
        minion.input.add(t)

    }
  }

  private def addIteration(query: Query, iteration: Iteration): Unit = {
    val iterations = ongoingIterationsByQuery.getOrElseUpdate(query, new ArrayBuffer[Iteration]())
    iterations += iteration
  }

  private def removeIteration(query: Query, iteration: Iteration): Unit = {
    val iterations = ongoingIterationsByQuery(query)
    val idx = iterations.indexOf(iteration)
    iterations.remove(idx)
    if(iterations.isEmpty) {
      ongoingIterationsByQuery.remove(query)
      query.finished()
    }
  }

  private def enqueueWaitingQueries(workQueue: TaskQueue): Unit = {
    while (!queryQueue.isEmpty) {
      val query = queryQueue.poll()
      var current = query.pipeline

      while (current.dependency != NoDependencies) {
        current = current.dependency.pipeline
      }

      val morsel = Morsel.create(current.slotInformation, MORSEL_SIZE)
      val iteration = new Iteration(None)
      workQueue.enqueue((null, Task(current, query, InitIteration(iteration, initQuery = true), morsel)))
      addIteration(query, iteration)
    }
  }

  private def createContinuationTask(minion: Minion, result: ResultObject, workQueue: TaskQueue): Unit = {
    result.next match {
      case _: Continue =>
        val morsel = Morsel.create(result.pipeline.slotInformation, MORSEL_SIZE)
        val task = result.createFollowContinuationTask(morsel)
        val runOn = task.message match {
          case ContinueWith(x: Continue) if x.needsSameThread => minion
          case _ => null
        }

        workQueue.enqueue((runOn, task))
      case _: EndOfIteration =>
        removeIteration(result.query, result.next.iterationState)
    }
  }

  private def scheduleMoreWorkOnMorsel(result: ResultObject, workQueue: TaskQueue): Unit = {
    result.pipeline.parent match {
      case Some(daddy) =>
        val data = Morsel.create(daddy.slotInformation, MORSEL_SIZE)
        val iteration = new Iteration(None)
        val message = InitIterationWithData(result.morsel, iteration)
        addIteration(result.query, iteration)
        workQueue.enqueue((null, Task(daddy, result.query, message, data)))

      case None =>
      // no more work here
    }
  }

}
