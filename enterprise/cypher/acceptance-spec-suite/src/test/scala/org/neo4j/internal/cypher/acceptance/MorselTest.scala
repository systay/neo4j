package org.neo4j.internal.cypher.acceptance

import java.io.File

import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.factory.GraphDatabaseFactory

class MorselTest extends CypherFunSuite {
  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    val result = block
    val r = System.nanoTime() - t0
    println(r)
    r
  }

  test("comparing numbers should work nicely") {
    val file = new File("target/apa")

    val db = new GraphDatabaseFactory().newEmbeddedDatabase(file)
    try {

      def createDb = {
        println("creating db...")
        var tx = db.beginTx()
        (1 to 2) foreach { i =>
          (1 to 100) foreach { j =>
            val node = db.createNode(Label.label("X"))
            node.setProperty("i", i)
            node.setProperty("j", j)
          }
          tx.success()
          tx.close()
          tx = db.beginTx()
          print(".")
        }
        println("...done!")
      }


      def go = {
        val r = db.execute("cypher runtime=interpreted match (n:X), (m:Y) WHERE n.p = m.p return *")
        r.accept(new ResultVisitor[Exception] {
          override def visit(row: ResultRow): Boolean = {
//            println(row.get("n"))
            true
          }
        })
//        val x = println(r.resultAsString())
//        while(r.hasNext) r.next()
      }

//      createDb

//      time(go) // plan and make sure everything is "hot"

      val numberOfExecutions = 100

      val tenPercent = (numberOfExecutions * 0.1).toInt

      val totalTime =
      ((1 to numberOfExecutions) map { _ =>
        time(go)
      }).
        sorted.
        slice(tenPercent, numberOfExecutions - tenPercent). // drop the slowest 10% and fastest 10%
        sum

      val avg = totalTime / (numberOfExecutions - tenPercent - tenPercent)
      println(s"avg: $avg")
    }

    db.shutdown()
  }

}
