/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.docgen

import org.neo4j.graphdb.index.Index
import org.junit.{Before, After}
import scala.collection.JavaConverters._
import java.io.{PrintWriter, File, FileWriter}
import org.neo4j.graphdb._
import org.scalatest.junit.JUnitSuite
import java.io.ByteArrayOutputStream
import org.neo4j.visualization.graphviz.{AsciiDocStyle, GraphvizWriter, GraphStyle}
import org.neo4j.walk.Walker
import org.neo4j.visualization.asciidoc.AsciidocHelper
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds
import org.neo4j.cypher.javacompat.GraphImpl
import org.neo4j.cypher.{CypherParser, ExecutionResult, ExecutionEngine}
import org.neo4j.test.{ImpermanentGraphDatabase, TestGraphDatabaseFactory, GraphDescription, GeoffService}
import org.neo4j.test.GeoffService

abstract class DocumentingTestBase extends JUnitSuite {

  var db: GraphDatabaseService = null
  val parser: CypherParser = new CypherParser
  var engine: ExecutionEngine = null
  var nodes: Map[String, Long] = null
  var nodeIndex: Index[Node] = null
  var relIndex: Index[Relationship] = null
  val properties: Map[String, Map[String, Any]] = Map()
  var generateConsole: Boolean = true

  def section: String

  def graphDescription: List[String]

  def indexProps: List[String] = List()

  def nicefy(in: String): String = in.toLowerCase.replace(" ", "-")

  def dumpToFile(writer: PrintWriter, title: String, query: String, returns: String, text: String, result: ExecutionResult) {
    writer.println("[[" + nicefy(section + " " + title) + "]]")
    writer.println("== " + title + " ==")
    writer.println(text)
    writer.println()
    writer.println("_Query_")
    writer.println()
    writer.println(AsciidocHelper.createCypherSnippet(query))
    writer.println()
    writer.println(returns)
    writer.println()

    val resultText = result.dumpToString()
    writer.println(".Result")
    writer.println(AsciidocHelper.createQueryResultSnippet(resultText))
    writer.println()
    writer.println()
    if(generateConsole) {
      writer.println(".Try this query live")
      writer.println("[console]")
      writer.println("----\n"+new GeoffService(db).toGeoff()+"\n"+query+"\n----")
      writer.println()
    }
    writer.flush()
    writer.close()
  }

  def path: String = {
    "target/docs/dev/ql/"
  }

  protected def getGraphvizStyle: GraphStyle = {
    AsciiDocStyle.withAutomaticRelationshipTypeColors()
  }

  private def emitGraphviz(fileName: String): String = {
    val out = new ByteArrayOutputStream();
    val writer = new GraphvizWriter(getGraphvizStyle);
    writer.emit(out, Walker.fullGraph(db));

    """
_Graph_

["dot", """" + fileName + """.svg", "neoviz"]
----
%s
----

""".format(out)
  }

  def dumpGraphViz(graphViz: PrintWriter, fileName: String) {
    val foo = emitGraphviz(fileName)
    graphViz.write(foo)
    graphViz.flush()
    graphViz.close()
  }

  def testQuery(title: String, text: String, queryText: String, returns: String, assertions: (ExecutionResult => Unit)*) {
    val r = testWithoutDocs(queryText, assertions:_*)
    val result: ExecutionResult = r._1
    var query: String = r._2

    val dir = new File(path + nicefy(section))
    if (!dir.exists()) {
      dir.mkdirs()
    }

    val writer = new PrintWriter(new File(dir, nicefy(title) + ".txt"), "UTF-8")
    dumpToFile(writer, title, query, returns, text, result)

    val graphFileName = "cypher-" + this.getClass.getSimpleName.replaceAll("Test", "").toLowerCase + "-graph"
    val graphViz = new PrintWriter(new File(dir, graphFileName + ".txt"), "UTF-8")
    dumpGraphViz(graphViz, graphFileName)
  }


  def testWithoutDocs(queryText: String, assertions: (ExecutionResult => Unit)*): (ExecutionResult, String) = {
    var query = queryText
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    val result = engine.execute(query)
    assertions.foreach(_.apply(result))
    (result, query)
  }

  def indexProperties[T <: PropertyContainer](n: T, index: Index[T]) {
    indexProps.foreach((property) => {
      if (n.hasProperty(property)) {
        val value = n.getProperty(property)
        index.add(n, property, value)
      }
    })
  }

  def node(name: String): Node = db.getNodeById(nodes.getOrElse(name, throw new NotFoundException(name)))

  def rel(id: Long): Relationship = db.getRelationshipById(id)

  @After
  def teardown() {
    if (db != null) db.shutdown()
  }

  @Before
  def init() {
    db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()
    engine = new ExecutionEngine(db)

    db.asInstanceOf[ImpermanentGraphDatabase].cleanContent(false)

    db.inTx(() => {
      nodeIndex = db.index().forNodes("nodes")
      relIndex = db.index().forRelationships("rels")
      val g = new GraphImpl(graphDescription.toArray[String])
      val description = GraphDescription.create(g)

      nodes = description.create(db).asScala.map {
        case (name, node) => name -> node.getId
      }.toMap

      db.getAllNodes.asScala.foreach((n) => {
        indexProperties(n, nodeIndex)
        n.getRelationships(Direction.OUTGOING).asScala.foreach(indexProperties(_, relIndex))
      })

      properties.foreach((n) => {
        val nod = node(n._1)
        n._2.foreach((kv) => nod.setProperty(kv._1, kv._2))
      })
    })
  }
}




