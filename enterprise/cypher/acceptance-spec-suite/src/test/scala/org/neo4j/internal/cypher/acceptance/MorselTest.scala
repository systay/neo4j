package org.neo4j.internal.cypher.acceptance

import java.io.File

import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.factory.GraphDatabaseFactory

class MorselTest extends CypherFunSuite {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  test("comparing numbers should work nicely") {
    val file = new File("target/apa")
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(file)
    try {

      def createDb = {
        var tx = db.beginTx()
        (1 to 1000) foreach { i =>
          (1 to 1000) foreach { j =>
            val node = db.createNode(Label.label("X"))
            node.setProperty("i", i)
            node.setProperty("i", j)
          }
          tx.success()
          tx.close()
          tx = db.beginTx()
          print(".")
        }
      }


      def go = {
        val r = db.execute("match (n:X) where n.i > n.j return n")
        while (r.hasNext) r.next()
      }

      //      createDb

      go // plan and make sure everything is "hot"

      val result = time(go)
    }

    db.shutdown()
  }

}
