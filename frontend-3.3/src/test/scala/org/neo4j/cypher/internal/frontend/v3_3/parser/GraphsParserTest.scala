package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.ast.GraphUrl
import org.neo4j.cypher.internal.frontend.v3_3.{DummyPosition, InputPosition, ast}

import scala.language.implicitConversions

class GraphsParserTest
  extends ParserAstTest[ast.GraphDef]
  with Graphs
  with Expressions {

  implicit val parser = GraphDef

  test("foo AS bar") {
    yields(ast.AliasGraph(ast.NamedGraph("foo")(pos), Some("bar")))
  }

  test("foo") {
    yields(ast.AliasGraph(ast.NamedGraph("foo")(pos), None))
  }

  test("SOURCE GRAPH") {
    yields(ast.AliasGraph(ast.SourceGraph()(pos), None))
  }

  test("SOURCE GRAPH AS foo") {
    yields(ast.AliasGraph(ast.SourceGraph()(pos), Some("foo")))
  }

  test("TARGET GRAPH") {
    yields(ast.AliasGraph(ast.TargetGraph()(pos), None))
  }

  test("TARGET GRAPH AS foo") {
    yields(ast.AliasGraph(ast.TargetGraph()(pos), Some("foo")))
  }

  test("DEFAULT GRAPH") {
    yields(ast.AliasGraph(ast.DefaultGraph()(pos), None))
  }

  test("DEFAULT GRAPH AS foo") {
    yields(ast.AliasGraph(ast.DefaultGraph()(pos), Some("foo")))
  }

  test("NEW GRAPH AT 'url' AS foo") {
    yields(ast.NewGraph(Some(url("url")), Some("foo")))
  }

  test("NEW GRAPH AT 'url'") {
    yields(ast.NewGraph(Some(url("url")), None))
  }

  test("NEW GRAPH AS foo") {
    yields(ast.NewGraph(None, Some("foo")))
  }

  test("NEW GRAPH") {
    yields(ast.NewGraph(None, None))
  }

  test("GRAPH AT 'url' AS foo") {
    yields(ast.LoadGraph(url("url"), Some("foo")))
  }

  test("GRAPH AT 'url'") {
    yields(ast.LoadGraph(url("url"), None))
  }

  test("COPY foo TO 'url' AS bar") {
    yields(ast.CopyGraph(ast.NamedGraph("foo")(pos), Some(url("url")), Some("bar")))
  }

  test("COPY foo TO 'url'") {
    yields(ast.CopyGraph(ast.NamedGraph("foo")(pos), Some(url("url")), None))
  }

  test("COPY SOURCE GRAPH TO 'url' AS bar") {
    yields(ast.CopyGraph(ast.SourceGraph()(pos), Some(url("url")), Some("bar")))
  }

  test("COPY SOURCE GRAPH TO 'url'") {
    yields(ast.CopyGraph(ast.SourceGraph()(pos), Some(url("url")), None))
  }

  test("COPY foo AS bar") {
    yields(ast.CopyGraph(ast.NamedGraph("foo")(pos), None, Some("bar")))
  }

  test("COPY foo") {
    yields(ast.CopyGraph(ast.NamedGraph("foo")(pos), None, None))
  }

  test("COPY SOURCE GRAPH AS bar") {
    yields(ast.CopyGraph(ast.SourceGraph()(pos), None, Some("bar")))
  }

  test("COPY SOURCE GRAPH") {
    yields(ast.CopyGraph(ast.SourceGraph()(pos), None, None))
  }

  private def url(addr: String): GraphUrl = ast.GraphUrl(ast.StringLiteral(addr)(pos))(pos)
  private implicit val pos: InputPosition = DummyPosition(-1)
  private implicit def variable(name: String): ast.Variable = ast.Variable(name)(pos)
}
