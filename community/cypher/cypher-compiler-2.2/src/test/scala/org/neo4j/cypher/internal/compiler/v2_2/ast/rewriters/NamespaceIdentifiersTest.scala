package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_2.{inSequence, AstRewritingMonitor, ASTRewriter}
import org.neo4j.cypher.internal.compiler.v2_2.ScopeHelper._
import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserFixture.parser

class NamespaceIdentifiersTest extends CypherFunSuite {

  val tests = Seq(
//    "match n return n as n" ->
//    "match n return n as n"
//    ,
//    "match n, x with n as n match x return n as n, x as x" ->
//    "match n, `  x@9` with n as n match `  x@29` return n as n, `  x@29` as x"
//    ,
    "match n, x where (x in n.prop where x = 2) return x as x" ->
    "match n, `  x@9` where (`  x@13` in n.prop where `  x@13` = 2) return `  x@9` as x"
//    ,
//  """MATCH (person:Person       )<-                                -(message)<-[like]-(liker:Person)
//    |WITH                 like.creationDate AS likeTime, person AS person
//    |ORDER BY likeTime         , message.id
//    |WITH        head(collect({              likeTime: likeTime})) AS latestLike, person AS person
//    |RETURN latestLike.likeTime AS likeTime
//    |ORDER BY likeTime
//    | """.stripMargin ->
//  """MATCH (person:Person       )<-                -(message)<-[like]-(liker:Person)
//    |WITH                 like.creationDate AS `  likeTime@138`, person AS person
//    |ORDER BY `  likeTime@138`         , message.id
//    |WITH        head(collect({              likeTime: `  likeTime@138`})) AS latestLike, person AS person
//    |WITH latestLike.likeTime AS `  likeTime@328`
//    |ORDER BY `  likeTime@328`
//    |RETURN `  likeTime@328` AS likeTime""".stripMargin

//  """MATCH (person:Person       )<-                               -(message)<-[like]-(liker:Person)
//    |WITH                 like.creationDate AS likeTime, person AS person
//    |ORDER BY likeTime         , message.id
//    |WITH        head(collect({              likeTime: likeTime})) AS latestLike, person AS person
//    |RETURN latestLike.likeTime AS likeTime
//    |ORDER BY likeTime
//    | """.stripMargin ->
//  """MATCH (person:Person       )<-               -(message)<-[like]-(liker:Person)
//    |WITH                 like.creationDate AS `  likeTime@137`, person AS person
//    |ORDER BY `  likeTime@137`         , message.id
//    |WITH        head(collect({              likeTime: `  likeTime@137`})) AS latestLike, person AS person
//    |WITH latestLike.likeTime AS `  likeTime@327`
//    |ORDER BY `  likeTime@327`
//    |RETURN `  likeTime@327` AS likeTime""".stripMargin
  )

  tests.foreach {
    case (q, rewritten) =>
      test(q) {
        assertRewritten(q, rewritten)
      }
  }

  val astRewriter = new ASTRewriter(mock[AstRewritingMonitor], false)

  private def assertRewritten(from: String, to: String) = {
    val fromAst = parseAndRewrite(from)
    val scope = fromAst.scope
    val namespacer = namespaceIdentifiers(scope)
    val namespacedAst = fromAst.endoRewrite(namespacer)

    val expectedAst = parseAndRewrite(to)

    namespacedAst should equal(expectedAst)
  }

  private def parseAndRewrite(queryText: String): Statement = {
    val parsedAst = parser.parse(queryText)
    val cleanedAst = parsedAst.endoRewrite(inSequence(normalizeReturnClauses, normalizeWithClauses))
    val (rewrittenAst, _) = astRewriter.rewrite(queryText, cleanedAst, cleanedAst.semanticState)
    rewrittenAst
  }
}
