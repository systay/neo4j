package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.Result
import org.neo4j.values.virtual.MapValue


class Dispatcher {

  private val MORSEL_SIZE = 10

  def run[E <: Exception](pipeline: Pipeline,
                          visitor: Result.ResultVisitor[E],
                          context: QueryContext,
                          resultPipe: PipelineInformation,
                          params: MapValue): Unit = {
    val state = new QueryState(params = params)
    val query = Query(pipeline, visitor, context, state, resultPipe)
    workQueue.put(new Task(pipeline, 0, query, true))


    while (query.alive) {
      try {
        Thread.sleep(1000) // Wait for stuff to be available to us
      } catch {
        case _: InterruptedException =>
      }
    }
  }

  case class Query[E <: Exception](pipeline: Pipeline,
                                   visitor: Result.ResultVisitor[E],
                                   context: QueryContext,
                                   state: QueryState,
                                   resultPipe: PipelineInformation) {
    private val _alive = new AtomicBoolean(true)
    def alive: Boolean = _alive.get()
    def die(): Unit = {
      _alive.set(false)
    }
  }

  val workQueue: BlockingQueue[Task[_]] = new LinkedBlockingQueue[Task[_]]()
  val resultQueue: Map[Query[_], Seq[Morsel]] = Map[Query[_], Seq[Morsel]]()

  private val workers: Seq[Worker] = {
    (0 to 10) map { i =>
      val worker = new Worker(workQueue)
      val t = new Thread(worker, s"Query Worker $i")
      t.setDaemon(true)
      t.start()
      t.interrupt()
      worker
    }
  }

  class Task[E <: Exception](val pipeline: Pipeline,
                             val startAtIndex: Int,
                             val query: Query[E],
                             val init: Boolean) {
    def cloneWithoutInit(): Task[E] =
      new Task(pipeline, startAtIndex, query, init = false)
  }

  class Worker(workQueue: BlockingQueue[Task[_]]) extends Runnable {

    private val alive = new AtomicBoolean(true)

    def die(): Unit = {
      alive.set(false)
    }

    override def run(): Unit = {
      while (alive.get()) {
        try {
          val task = workQueue.take()
          val context = task.query.context
          val state = task.query.state
          if(task.init)
            task.pipeline.init(state, context)
          val morsel = Morsel.create(task.pipeline.slotInformation, MORSEL_SIZE)
          val output = task.pipeline.operate(morsel, context, state)
          val resultRow = new MorselResultRow(output, 0, task.query.resultPipe, context)

          (0 until output.rows) foreach { position =>
            resultRow.currentPos = position
            task.query.visitor.visit(resultRow)
          }

          if(output.moreDataToCome) {
            workQueue.put(task.cloneWithoutInit())
          } else {
            task.query.die() // Nothing more to do. Die peacefully
          }
          Thread.sleep(123)
        } catch {
          case _: InterruptedException =>
          // someone seems to want us to shut down and go home.
        }
      }
    }
  }

}

object Dispatcher {
  var instance: Dispatcher = new Dispatcher
}