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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

abstract class InfixExpressionTestBase(ctr: (Expression, Expression) => Expression) extends CypherFunSuite {

  protected val context: SemanticContext = SemanticContext.Simple

  protected def testValidTypes(lhsTypes: TypeSpec, rhsTypes: TypeSpec)(expected: TypeSpec) {
    val (result, expression) = evaluateWithTypes(lhsTypes, rhsTypes)
    result.errors shouldBe empty
    expression.types(result.state) should equal(expected)
  }

  protected def testInvalidApplication(lhsTypes: TypeSpec, rhsTypes: TypeSpec)(message: String) {
    val (result, _) = evaluateWithTypes(lhsTypes, rhsTypes)
    result.errors should not be empty
    result.errors.head.msg should equal(message)
  }

  protected def evaluateWithTypes(lhsTypes: TypeSpec, rhsTypes: TypeSpec): (SemanticCheckResult, ast.Expression) = {
    val lhs = DummyExpression(lhsTypes)
    val rhs = DummyExpression(rhsTypes)

    val expression = ctr(lhs, rhs)

    val state = Seq(lhs, rhs).semanticCheck(context)(SemanticState.clean).state
    (expression.semanticCheck(context)(state), expression)
  }
}
