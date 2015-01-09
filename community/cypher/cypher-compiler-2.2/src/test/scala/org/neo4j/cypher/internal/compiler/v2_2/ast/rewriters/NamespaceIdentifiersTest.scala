package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.{AstRewritingMonitor, ASTRewriter}
import org.neo4j.cypher.internal.compiler.v2_2.ScopeHelper._
import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserFixture.parser

class NamespaceIdentifiersTest extends CypherFunSuite {

  val tests = Seq(
    "match n, x with n match x return *" ->
    "match n, `  x@9` with n match `  x@24` return *",

    "match n return n" ->
    "match n return n",

    "match n, x with n match x return n as n, x as x" ->
    "match n, `  x@9` with n match `  x@24` return n as n, `  x@24` as x",

    """MATCH (person:Person       )<-                                        -(message)<-[like]-(liker:Person)
      |WITH                 like.creationDate AS likeTime, person
      |ORDER BY likeTime         , message.id
      |WITH        head(collect({              likeTime: likeTime})) AS latestLike, person
      |RETURN latestLike.likeTime AS likeTime
      |ORDER BY likeTime""".stripMargin ->
    """MATCH (person:Person       )<-                                        -(message)<-[like]-(liker:Person)
      |WITH                 like.creationDate AS likeTime, person
      |ORDER BY likeTime         , message.id
      |WITH        head(collect({              likeTime: likeTime})) AS latestLike, person
      |RETURN latestLike.likeTime AS likeTime
      |ORDER BY likeTime""".stripMargin
  )

  tests.foreach {
    case (q, rewritten) =>
      test(q) {
        assertRewritten(q, rewritten)
      }
  }

  val astRewriter = new ASTRewriter(mock[AstRewritingMonitor], false)

  private def assertRewritten(from: String, to: String) = {
    val ast = parser.parse(from)
    val preRewrittenAst = astRewriter.rewrite(from, ast, ast.semanticState)
    val expectedResultAst = parser.parse(to)
    val scope = ast.scope

    val rewriter = namespaceIdentifiers(scope)
    val rewrittenAst = preRewrittenAst.endoRewrite(rewriter)

    rewrittenAst should equal(expectedResultAst)
  }
}
