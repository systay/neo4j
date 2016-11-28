/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_2.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class TypingTest extends CypherFunSuite {
  val pos = DummyPosition(0)

  test("1+2 should know that it will be a long") {
    val lit1 = SignedDecimalIntegerLiteral("1")(pos)
    val lit2 = SignedDecimalIntegerLiteral("2")(pos)
    val addition = Add(lit1, lit2)(pos)

    Typing.enrich(addition)

    lit1.myType.value should equal(CTInteger.covariant)
    lit2.myType.value should equal(CTInteger.covariant)
    addition.myType.value should equal(CTInteger.covariant)
  }

  test("add is typed correctly") {
    implicit val expressionUnderTest: (Expression, Expression) => Expression = (lhs, rhs) => Add(lhs, rhs)(pos)

    testValidTypes(CTInteger, CTInteger)(CTInteger)
    testValidTypes(CTString, CTString)(CTString)
    testValidTypes(CTString, CTInteger)(CTString)
    testValidTypes(CTString, CTFloat)(CTString)
    testValidTypes(CTInteger, CTString)(CTString)
    testValidTypes(CTInteger, CTFloat)(CTFloat)
    testValidTypes(CTFloat, CTString)(CTString)
    testValidTypes(CTFloat, CTInteger)(CTFloat)
    testValidTypes(CTFloat, CTFloat)(CTFloat)

    testValidTypes(CTList(CTNode), CTList(CTNode))(CTList(CTNode))
    testValidTypes(CTList(CTFloat), CTList(CTFloat))(CTList(CTFloat))

    testValidTypes(CTList(CTNode), CTNode)(CTList(CTNode))
    testValidTypes(CTList(CTFloat), CTFloat)(CTList(CTFloat))

    testValidTypes(CTNode, CTList(CTNode))(CTList(CTNode))
    testValidTypes(CTFloat, CTList(CTFloat))(CTList(CTFloat))
  }

//  test("MATCH (a) FOREACH(x in [1,2] | CREATE ())") {
//    val matchPattern = Pattern(Seq(EveryPath(NodePattern(Some(Variable("a")(pos)), Seq.empty, None)(pos))))(pos)
//    val matchClause = Match(optional = false, matchPattern, Seq.empty, None)(pos)
//
//    val createPattern = Pattern(Seq(EveryPath(NodePattern(None, Seq.empty, None)(pos))))(pos)
//    val create = Create(createPattern)(pos)
//
//    val foreach = Foreach(Variable("x")(pos), ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos), Seq(create))(pos)
//
//    val singleQuery = SingleQuery(Seq(matchClause, foreach))(pos)
//    val query = Query(None, singleQuery)(pos)
//
//    SemanticAnalysis.visit(query)
//
//    query.myScope.value._1 should equal(Scope.empty)
//    singleQuery.myScope.value._1 should equal(Scope.empty)
//    val matchScope = Scope.empty.enterScope().add(Variable("a")(pos))
//    foreach.myScope.value._1 should equal(matchScope)
//    val createScope = matchScope.enterScope().add(Variable("x")(pos))
//    create.myScope.value._1 should equal(createScope)
//  }



  protected def testValidTypes(lhsTypes: TypeSpec, rhsTypes: TypeSpec)(expected: TypeSpec)
                              (implicit expCreator: (Expression, Expression) => Expression): Unit = {
    val expression = expCreator(DummyExpression(lhsTypes), DummyExpression(rhsTypes))
    println(expression)
    Typing.enrich(expression)
    expression.myType.value should equal(expected)
  }
}
