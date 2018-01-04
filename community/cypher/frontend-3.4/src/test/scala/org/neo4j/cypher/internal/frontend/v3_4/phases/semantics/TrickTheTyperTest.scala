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

import org.neo4j.cypher.internal.frontend.v3_4.ast.{Return, Statement}
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Types._
import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.scalatest.matchers.{MatchResult, Matcher}

class TrickTheTyperTest extends CypherFunSuite with TypingTestBase {
  test("unwind mix of ints and strings") {
    query("UNWIND [1,2,3, 'a','b','c'] AS x RETURN 42 as y, x") should
      typedAs(
        "x" -> (IntegerType, StringType),
        "y" -> t(IntegerType))
  }

  test("modulo on a property") {
    query("MATCH (a) RETURN a.prop + 1 AS x") should
      typedAs(
        "x" -> (IntegerType, FloatType, NullType))
  }
}

trait TypingTestBase {
  self: CypherFunSuite =>

  // We don't get syntax sugar to create single element tuples
  def t(typ: NewCypherType): Tuple1[NewCypherType] = Tuple1(typ)

  def query(q: String): Statement = {
    val parser = new CypherParser
    parser.parse(q)
  }

  class TypeMatcher(typeAssertion: Seq[(String, Product)]) extends Matcher[Statement] {
    override def apply(ast: Statement): MatchResult = try {
      val (readScope, writeScope) = Scoping.doIt(ast)
      val binding = VariableBinding.doIt(ast, readScope, writeScope)
      val types = Typing.doIt(ast, binding)
      val returnItems = ast.findByClass[Return].returnItems.items.toArray

      val errorMessages = new StringBuilder

      val matches = typeAssertion forall {
        case (columnName, expectedTypesRaw) =>
          val expectedTypes: Set[NewCypherType] =
            expectedTypesRaw.
              productIterator.
              map(_.asInstanceOf[NewCypherType]).
              toSet

          val returnItem = returnItems.find(_.name == columnName).getOrElse(
            fail(s"did not find any such column name $columnName"))

          val actualTypes = types.get(returnItem.expression)
          val success = actualTypes == expectedTypes
          if (!success)
            errorMessages.append(s"For $columnName, expected $expectedTypes but got $actualTypes\n")
          success
      }

      val message = errorMessages.toString()
      MatchResult(matches = matches, rawFailureMessage = message, rawNegatedFailureMessage = message)

    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed while figuring out the types of:\n$ast", e)
    }
  }

  def typedAs(typeExpectations: (String, Product)*) = new TypeMatcher(typeExpectations)
}