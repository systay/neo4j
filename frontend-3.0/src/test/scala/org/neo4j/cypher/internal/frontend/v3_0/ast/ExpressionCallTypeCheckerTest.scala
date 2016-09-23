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
package org.neo4j.cypher.internal.frontend.v3_0.ast

import org.neo4j.cypher.internal.frontend.v3_0.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticState}
import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_0.symbols._

class ExpressionCallTypeCheckerTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should accept a specified type") {
    typeCheckSuccess(Seq(ExpressionSignature(Vector(CTInteger), CTInteger)), Seq(CTInteger), CTInteger)
    typeCheckSuccess(Seq(ExpressionSignature(Vector(CTInteger), CTString)), Seq(CTInteger), CTString)
    typeCheckSuccess(Seq(ExpressionSignature(Vector(CTInteger), CTString),
                         ExpressionSignature(Vector(CTFloat), CTBoolean)), Seq(CTNumber.covariant), CTBoolean | CTString)
  }

  test("any type") {
    typeCheckSuccess(Seq(ExpressionSignature(Vector(CTInteger), CTBoolean),
                         ExpressionSignature(Vector(CTFloat), CTFloat)), Seq(CTAny.covariant), CTBoolean | CTFloat)
  }

  test("two ExpressionSignatures") {
    val sig = Seq(ExpressionSignature(Vector(CTString), CTInteger),
                  ExpressionSignature(Vector(CTNumber), CTInteger))
    typeCheckSuccess(sig, Seq(CTAny.covariant), CTInteger)
    typeCheckSuccess(sig, Seq(CTNumber.covariant), CTInteger)
    typeCheckSuccess(sig, Seq(CTFloat), CTInteger)
    typeCheckSuccess(sig, Seq(CTInteger), CTInteger)
    typeCheckSuccess(sig, Seq(CTString), CTInteger)
  }

  test("fail on mismatch with ExpressionSignature") {
    typeCheckFail(Seq(ExpressionSignature(Vector(CTBoolean), CTRelationship)), Seq(CTNode)) { errs =>
      errs should contain("Type mismatch: expected Boolean but was Node")
    }
    typeCheckFail(Seq(ExpressionSignature(Vector(CTBoolean), CTRelationship),
                      ExpressionSignature(Vector(CTString), CTRelationship),
                      ExpressionSignature(Vector(CTMap), CTRelationship)), Seq(CTNumber)) { errs =>
      errs should contain("Type mismatch: expected Boolean, Map, Node, Relationship or String but was Number")
    }

    typeCheckFail(Seq(ExpressionSignature(Vector(CTBoolean, CTNode, CTInteger), CTRelationship)), Seq(CTBoolean, CTNode, CTFloat)) { errs =>
      errs should contain("Type mismatch: expected Integer but was Float")
    }
  }

  test("should pick the most specific ExpressionSignature of many applicable maps") {
    val identityExpressionSignature = Seq(ExpressionSignature(Vector(CTMap), CTMap),
                                          ExpressionSignature(Vector(CTRelationship), CTRelationship),
                                          ExpressionSignature(Vector(CTNode), CTNode))
    typeCheckSuccess(identityExpressionSignature, Seq(CTAny.covariant), CTMap | CTNode | CTRelationship)
    typeCheckSuccess(identityExpressionSignature, Seq(CTMap.invariant), CTMap)
    typeCheckSuccess(identityExpressionSignature, Seq(CTMap.covariant), CTMap | CTNode | CTRelationship)
    typeCheckSuccess(identityExpressionSignature, Seq(CTNode), CTMap | CTNode)
    typeCheckSuccess(identityExpressionSignature, Seq(CTRelationship), CTMap | CTRelationship)
  }

  test("should pick the most specific ExpressionSignature of many applicable numbers") {
    val identityExpressionSignature = Seq(ExpressionSignature(Vector(CTInteger), CTInteger),
                                          ExpressionSignature(Vector(CTFloat), CTFloat))
    typeCheckSuccess(identityExpressionSignature, Seq(CTAny.covariant), CTInteger | CTFloat)
    typeCheckSuccess(identityExpressionSignature, Seq(CTNumber.covariant), CTInteger | CTFloat)
    typeCheckSuccess(identityExpressionSignature, Seq(CTInteger), CTInteger)
    typeCheckSuccess(identityExpressionSignature, Seq(CTFloat), CTFloat)
  }

  test("should handle combined typespecs") {
    val ExpressionSignatures = Seq(ExpressionSignature(Vector(CTInteger, CTInteger), CTInteger),
                                   ExpressionSignature(Vector(CTNumber, CTNumber), CTFloat))
    typeCheckSuccess(ExpressionSignatures, Seq(CTInteger, CTInteger), CTFloat | CTInteger)
  }

  test("pretty print") {
    TypeSpec.formatArguments(Seq(CTNumber, CTBoolean, CTString)) should equal("(Number, Boolean, String)")
  }

  private def typeCheck(ExpressionSignatures: Seq[ExpressionSignature], arguments: Seq[TypeSpec]): (TypeExpr, SemanticCheckResult) = {
    val argExpressions = arguments.map(DummyExpression(_))
    val semanticState = argExpressions.foldLeft(SemanticState.clean) {
      case (state, inner) => state.specifyType(inner, inner.possibleTypes).right.get
    }
    val expr = TypeExpr(argExpressions)
    val check = ExpressionCallTypeChecker(ExpressionSignatures).checkTypes(expr)(semanticState)
    (expr, check)
  }

  private def typeCheckSuccess(ExpressionSignatures: Seq[ExpressionSignature], arguments: Seq[TypeSpec], spec: TypeSpec) = {
    val (expr, check) = typeCheck(ExpressionSignatures, arguments)
    check.errors shouldBe empty
    check.state.typeTable.get(expr).map(_.specified) should equal(Some(spec))
  }

  private def typeCheckFail(ExpressionSignatures: Seq[ExpressionSignature], arguments: Seq[TypeSpec])(checkError: Seq[String] => Unit) = {
    val (_, check) = typeCheck(ExpressionSignatures, arguments)
    checkError(check.errors.map(_.msg.replaceAll("\\s+", " ")))
  }

  case class TypeExpr(override val arguments: Seq[Expression]) extends Expression {
    override def semanticCheck(ctx: SemanticContext): SemanticCheck = ???
    override def position: InputPosition = pos
  }
}
