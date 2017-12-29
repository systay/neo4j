/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.phases.semantics

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Types._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.Add
import org.neo4j.cypher.internal.util.v3_4.symbols

class AddTypingTest extends CypherFunSuite with AstConstructionTestSupport {
  test("very simple test") {
    val expression = Add(literalInt(1), literalInt(2))(pos)
    val types: Typing = constructAndTypeAst(expression)
    types.getTypesFor(expression) should equal(Set(IntegerType))
  }

  test("param types") {
    val expression = Add(parameter("prop", symbols.CTAny), literalInt(2))(pos)
    val types: Typing = constructAndTypeAst(expression)
    types.getTypesFor(expression) should equal(Set(IntegerType, FloatType, StringType, ListType.ListOfUnknown))
  }

  testValidTypes(StringType, StringType)(StringType)
  testValidTypes(StringType, IntegerType)(StringType)
  testValidTypes(StringType, FloatType)(StringType)
  testValidTypes(IntegerType, StringType)(StringType)
  testValidTypes(IntegerType, IntegerType)(IntegerType)
  testValidTypes(IntegerType, FloatType)(FloatType)
  testValidTypes(FloatType, StringType)(StringType)
  testValidTypes(FloatType, IntegerType)(FloatType)
  testValidTypes(FloatType, FloatType)(FloatType)

  testValidTypes(ListType(NodeType), ListType(NodeType))(ListType(NodeType))
  testValidTypes(ListType(FloatType), ListType(FloatType))(ListType(FloatType))

  testValidTypes(ListType(NodeType), NodeType)(ListType(NodeType))
  testValidTypes(ListType(FloatType), FloatType)(ListType(FloatType))

  testValidTypes(NodeType, ListType(NodeType))(ListType(NodeType))
  testValidTypes(FloatType, ListType(FloatType))(ListType(FloatType))

  testInvalidApplication(IntegerType, BoolType)
  testInvalidApplication(IntegerType, NodeType)

  private def constructAndTypeAst(expression: Add) = {
    val returnItem = AliasedReturnItem(expression, varFor("x"))(pos)
    val returnItems = ReturnItems(includeExisting = false, Seq(returnItem))(pos)
    val ast = Query(None, SingleQuery(Seq(Return(distinct = false, returnItems, None, None, None, None, Set.empty)(pos)))(pos))(pos)

    val (readScope, writeScope) = Scoping.doIt(ast)
    val bindings = VariableBinding.doIt(ast, readScope, writeScope)
    val types = Typing.doIt(ast, bindings)
    types
  }

  protected def testValidTypes(lhsType: NewCypherType, rhsType: NewCypherType)(expected: NewCypherType) {
    val expression = Add(parameter("l", translate(lhsType)), parameter("r", translate(rhsType)))(pos)
    test(s"$lhsType + $rhsType should equal $expected") {
      val types: Typing = constructAndTypeAst(expression)
      types.getTypesFor(expression) should equal(Set(expected))
      types.get(expression.secretId) shouldBe a [ValidTypeConstraint]
    }
  }
  protected def testInvalidApplication(lhsType: NewCypherType, rhsType: NewCypherType) {
    val expression = Add(parameter("l", translate(lhsType)), parameter("r", translate(rhsType)))(pos)
    test(s"$lhsType + $rhsType should fail") {
      val types: Typing = constructAndTypeAst(expression)
      val constraint = types.get(expression.secretId)
      constraint shouldBe an [InvalidTypeConstraint]
      constraint.possibleErrors should equal(Set(s"Don't know how to + between $lhsType and $rhsType"))
    }
  }

  private def translate(in: NewCypherType): symbols.CypherType = in match {
    case BoolType => symbols.CTBoolean
    case StringType => symbols.CTString
    case FloatType => symbols.CTFloat
    case IntegerType => symbols.CTInteger
    case MapType(_) => symbols.CTMap
    case NodeType => symbols.CTNode
    case RelationshipType => symbols.CTRelationship
    case PointType => symbols.CTPoint
    case GeometryType => symbols.CTGeometry
    case PathType => symbols.CTPath
    case ListType(inner) => symbols.ListType(translate(inner.head))
    case _ => symbols.CTAny
  }

}