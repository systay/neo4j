package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, InternalException}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.Result
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable


class Dispatcher {

  private val MORSEL_SIZE = 10
  private var shutting_down = false

  def run[E <: Exception](pipeline: Pipeline,
                          visitor: Result.ResultVisitor[E],
                          context: QueryContext,
                          resultPipe: PipelineInformation,
                          params: MapValue): Unit = {
    println(s"T${Thread.currentThread().getId}")
    val state = new QueryState(params = params)
    val query = Query(pipeline, visitor, context, state, resultPipe, Thread.currentThread().getId.toString)

    val stack = new scala.collection.mutable.Stack[Pipeline]
    stack.push(pipeline)

    while (stack.nonEmpty) {
      val current = stack.pop()
      if (current.dependencies.nonEmpty)
        current.dependencies.foreach(stack.push)
      else
        workQueue.put(new Task(current, query, Init))
    }

    while (query.alive) {
      try {
        Thread.sleep(10) // Wait for stuff to be available to us
      } catch {
        case _: InterruptedException =>
      }
    }
  }

  val workQueue: BlockingQueue[Task] = new LinkedBlockingQueue[Task]()
  val resultQueue: Map[Query, Seq[Morsel]] = Map[Query, Seq[Morsel]]()

  def shutdown(): Unit = {
    println("got kill signal. shutting down.")
    shutting_down = true
    workers.foreach(_.die())
    workQueue.clear()
  }

  private val workers: Seq[Worker] = {
    (0 to 3) map { i =>
      val worker = new Worker(workQueue)
      val t = new Thread(worker, s"Query Worker $i")
      t.setDaemon(true)
      t.start()
      worker
    }
  }

  class Worker(workQueue: BlockingQueue[Task]) extends Runnable {

    // Used for work that cannot move to a different thread
    private val threadLocalWorkQueue = new mutable.Queue[Task]()

    private val alive = new AtomicBoolean(true)

    def die(): Unit = {
      alive.set(false)
    }

    override def run(): Unit = {
      while (alive.get()) {
        try {
          val task = {
            // If we already have worked that can only be executed on this thread, let's run that first
            if (threadLocalWorkQueue.nonEmpty)
              threadLocalWorkQueue.dequeue()
            else
            // otherwise, we'll block waiting for work to do
              workQueue.take()
          }

          if (task.query.alive) try {
            println(s"${Thread.currentThread().getId} starting work on Q${task.query.name} ${task.pipeline} with ${task.continue}")
            val context = task.query.context
            val state = task.query.state
            val morsel = Morsel.create(task.pipeline.slotInformation, MORSEL_SIZE)
            val (returnType, continue) = task.pipeline.operate(task.continue, morsel, context, state)

            println(s"${Thread.currentThread().getId} finished. Produced ${morsel.validRows} rows, next step is: $continue")

            (task.pipeline.parent, returnType) match {
              case (None, MorselType) =>
                val resultRow = new MorselResultRow(morsel, 0, task.query.resultPipe, context)
                (0 until morsel.validRows) foreach { position =>
                  resultRow.currentPos = position
                  task.query.visitor.visit(resultRow)
                }
              case (None, UnitType) =>
                // Empty on purpose

              case (Some(parent), MorselType) =>
                println(s"${Thread.currentThread().getId} is scheduling follow up work for $parent")
                val nextTask = new Task(parent, task.query, InitWithData(morsel))
                workQueue.put(nextTask)

              case (Some(_), UnitType) =>
                throw new InternalException("something went wrong dispatching work for this query")
            }

            val maybeTask = task.continueWith(continue)
            maybeTask.foreach(threadLocalWorkQueue.enqueue(_))
          } catch {
            case e: CypherException =>
              // If a task for a query dies, we need to kill the whole query
              e.printStackTrace(System.err)
              task.query.die()
          }
          else
            println("got a task for a dead query. ignoring it")
        } catch {
          // someone seems to want us to shut down and go home.
          case _: InterruptedException =>

          // Uh-oh... An uncaught exception is not good. Let's kill everything.
          case e: Exception =>
            shutdown()
            throw e
        }
      }
    }
  }
}

object Dispatcher {
  var instance: Dispatcher = new Dispatcher
}

case class Query(pipeline: Pipeline,
                 visitor: Result.ResultVisitor[_],
                 context: QueryContext,
                 state: QueryState,
                 resultPipe: PipelineInformation,
                 name: String) {
  private val _alive = new AtomicBoolean(true)

  def alive: Boolean = _alive.get()

  def die(): Unit = {
    _alive.set(false)
  }
}


class Task(val pipeline: Pipeline,
           val query: Query,
           val continue: Continuation) {
  def continueWith(newContinuation: Continuation): Option[Task] =
    newContinuation match {
      case Done =>
        None
      case _ =>
        Some(new Task(pipeline, query, newContinuation))
    }

}
