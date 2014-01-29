package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Runtime
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.impl.util.FileUtils
import java.io.File
import org.neo4j.graphdb.GraphDatabaseService
import org.junit.Test
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.compiler.v2_1.{ast, parser}
import org.parboiled.scala._
import org.neo4j.cypher.internal.compiler.v2_1.planner.Id
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import scala.Some

class CodeGenTest extends Assertions with MockitoSugar with parser.Query {
  val DB_PATH = "tmpdb"
  val gdb = mock[GraphDatabaseService]//new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH )
  val plan = mock[AbstractPlan]

/*  def initDb() = {

    try
    {
      var tx = gdb.beginTx()
      var firstNode = gdb.createNode();
      firstNode.setProperty( "A", "A1" );
      var secondNode = gdb.createNode();
      secondNode.setProperty( "A", "A2" );
      tx.success()
    } catch {
      case _ :Throwable => fail("failed to init db")
    }
  }

  def dropDb() = {
    try
    {
      FileUtils.deleteRecursively( new File( DB_PATH ) );
    }catch {
      case _ :Throwable => fail("failed to clean up db")
    }
  }
*/
  private def parse(input: String): ast.Query =
    ReportingParseRunner(Query ~ EOI).run(input).result match {
      case Some(ast) => ast
      case None => fail("query parser failed")
  }

  @Test def idRegisterAllocation() {
    val runtime = new Runtime(gdb)
    val generator = new CodeGenerator(runtime)
    val qg = QueryGraphBuilder.build(parse("match (a)-->(b)-->(c)"))

    generator.translate(plan, qg)

    assert(runtime.getIdRegistersCount()=== 3)
  }
}
