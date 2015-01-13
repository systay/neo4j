/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ScopeHelper._
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.{ASTAnnotationMap, Identifier, Statement}
import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserFixture.parser
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable

class NamespacerTest extends CypherFunSuite {

  val tests = Seq(
    "match n return n as n" ->
    "match n return n as n"
//    ,
//    "match n, x with n as n match x return n as n, x as x" ->
//    "match n, `  x@9` with n as n match `  x@29` return n as n, `  x@29` as x"
//    ,
//    "match n, x where [x in n.prop where x = 2] return x as x" ->
//    "match n, `  x@9` where [`  x@18` in n.prop where `  x@18` = 2] return `  x@9` as x"
//    ,
//    "MATCH (a) WITH a.bar as bars WHERE 1 = 2 RETURN *" ->
//    "MATCH (a) WITH a.bar as bars WHERE 1 = 2 RETURN *"
//    ,
//    "match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll" ->
//    "match (n) where id(n) = 0 WITH collect(n) as coll where length(coll)={id} RETURN coll"
//    ,
//    "match me-[r1]->you with 1 AS x match me-[r1]->food<-[r2]-you return r1.times as `r1.times`" ->
//    "match `  me@6`-[`  r1@10`]->`  you@15` with 1 AS x match `  me@37`-[`  r1@41`]->food<-[r2]-`  you@57` return `  r1@41`.times as `r1.times`"
//    ,
//    "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *" ->
//    "MATCH (a:A)-[r1:T1]->(b:B)-[r2:T1]->(c:C) RETURN *"
//    ,
//    "match (a:Party) return a as a union match (a:Animal) return a as a" ->
//    "match (`  a@7`:Party) return `  a@7` as a union match (`  a@43`:Animal) return `  a@43` as a"
//    ,
//    "match p=(a:Start)-->b return *" ->
//    "match p=(a:Start)-->b return *"
//    ,
//  """match me-[r1:ATE]->food<-[r2:ATE]-you where id(me) = 1
//    |with me,count(distinct r1) as H1,count(distinct r2) as H2,you match me-[r1:ATE]->food<-[r2:ATE]-you
//    |return me,you,sum((1-ABS(r1.times/H1-r2.times/H2))*(r1.times+r2.times)/(H1+H2))""".stripMargin ->
//
//  """match me-[r1:ATE]->food<-[r2:ATE]-you where id(me) = 1
//    |with me,count(distinct r1) as H1,count(distinct r2) as H2,you match me-[r1:ATE]->food<-[r2:ATE]-you
//    |return me,you,sum((1-ABS(r1.times/H1-r2.times/H2))*(r1.times+r2.times)/(H1+H2))""".stripMargin
//    ,
//    """MATCH (person:Person       )<-                                -(message)<-[like]-(liker:Person)
//    |WITH                 like.creationDate AS likeTime, person AS person
//    |ORDER BY likeTime         , message.id
//    |WITH        head(collect({              likeTime: likeTime})) AS latestLike, person AS person
//    |RETURN latestLike.likeTime AS likeTime
//    |ORDER BY likeTime""".stripMargin ->
//  """MATCH (person:Person       )<-                -(message)<-[like]-(liker:Person)
//    |WITH                 like.creationDate AS `  likeTime@138`, person AS person
//    |ORDER BY `  likeTime@138`         , message.id
//    |WITH        head(collect({              likeTime: `  likeTime@138`})) AS latestLike, person AS person
//    |WITH latestLike.likeTime AS `  likeTime@328`
//    |ORDER BY `  likeTime@328`
//    |RETURN `  likeTime@328` AS likeTime""".stripMargin
//  ,
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

  test("Renames identifiers in semantic table") {
    val idA1 = Identifier("a")(InputPosition(1, 0, 1))
    val idA2 = Identifier("a")(InputPosition(2, 0, 2))
    val idA3 = Identifier("a")(InputPosition(3, 0, 3))
    val idB5 = Identifier("b")(InputPosition(5, 0, 5))

    val infoA1 = mock[ExpressionTypeInfo]
    val infoA2 = mock[ExpressionTypeInfo]
    val infoA3 = mock[ExpressionTypeInfo]
    val infoA4 = mock[ExpressionTypeInfo]
    val infoB5 = mock[ExpressionTypeInfo]

    val table = SemanticTable(ASTAnnotationMap(
      idA1 -> infoA1,
      idA2 -> infoA2,
      idA3 -> infoA3,
      idB5 -> infoB5
    ))

    val renamings = Map(
      Ref(idA1) -> Identifier("a@1")(InputPosition(1, 0, 1)),
      Ref(idA2) -> Identifier("a@2")(InputPosition(2, 0, 2))

    )

    val namespacer = Namespacer(renamings)

    val newTable = namespacer.tableRewriter(table)

    newTable.types should equal(ASTAnnotationMap(
      Identifier("a@1")(InputPosition(1, 0, 1)) -> infoA1,
      Identifier("a@2")(InputPosition(2, 0, 2)) -> infoA2,
      idA3 -> infoA3,
      idB5 -> infoB5
    ))
  }

  val astRewriter = new ASTRewriter(mock[AstRewritingMonitor], false)

  private def assertRewritten(from: String, to: String) = {
    val fromAst = parseAndRewrite(from)
    val scope = fromAst.scope
    val state = fromAst.semanticState
    val table = SemanticTable(state.typeTable, state.recordedScopes)
    val namespacer = Namespacer(fromAst, table, state.scopeTree)
    val namespacedAst = fromAst.endoRewrite(namespacer.astRewriter)

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
