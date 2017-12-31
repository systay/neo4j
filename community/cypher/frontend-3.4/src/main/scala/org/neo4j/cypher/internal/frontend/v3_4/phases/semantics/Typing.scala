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

import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Types._
import org.neo4j.cypher.internal.v3_4.expressions._

object Typing {
  /*
  Typically, a type system takes as input an AST, and returns it with type annotations if
  type checking passed, or it will fail with helpful error messages to aid the developer in
  fixing the problems in their code.

  Because Cypher uses a gradual type system, this type system will also annotate the expressions
  with which expectations on them. This information is used in later stages, to introduce explicit type checking
  and casting in the expressions. This way, when we are sure about types, no dynamic checking is needed -
  only when there could different types coming through do we need to do type checking.

  This is done in two stages - first stage is while walking down the tree, we annotate expressions with expectations
  on them, and then when coming back up the tree, we calculate types for expressions.
  */
  type TypeJudgement = PartialFunction[(Expression, TypingContext, Bindings, TypeTable), TypeConstraint]

  def doIt(statement: ast.Statement, variableBindings: VariableBinding): TypeTable = {
    val types = new TypeTable
    val bindings = new Bindings(statement, variableBindings)

    val bottomUpVisitor: PartialFunction[(Any, TypingContext), Unit] = {
      case (ast.Unwind(e, v), _) =>
        val possibleInnerTypes =
          types.get(e) flatMap {
            case ListType(inner) => inner
            case _ => Set.empty[NewCypherType]
          }

        val variableType = if (possibleInnerTypes.isEmpty) {
          // Does not look like it would be possible to produce a list through this expression. Let's make the inner type
          // ANY and fail
          InvalidTypeConstraint(Types.ANY, Set.empty)
        } else {
          ValidTypeConstraint(possibleInnerTypes)
        }
        types.set(v, variableType)

      case (ast.Return(_, ReturnItems(_, returnItems), _, _, _, _, _), _) =>
        for {
          returnItem: ReturnItem <- returnItems
          introducedVariable <- returnItem.alias
        } {
          val typesOfProjection = types.get(returnItem.expression)
          types.set(introducedVariable, ValidTypeConstraint(typesOfProjection))
        }

      case (e: Expression, c) =>
        val typeConstraint = TypeJudgements.expressionTyper(e, c, bindings, types)
        types.set(e, typeConstraint)
    }

    statement.treeVisitBottomUp[TypingContext](MatchContext, contextCreator, bottomUpVisitor)

    types
  }

  // When visiting patterns, it's essential to know which context the patterns appear in. This is difficult
  // to figure out coming up the tree, so
  private def contextCreator: PartialFunction[Any, TypingContext] = {
      case _: Match => MatchContext
      case _: Merge => MergeContext
      case _: Create => CreateContext
      case _: CreateUnique => CreateUniqueContext

      case _: ShortestPathExpression |
           _: PatternExpression => ExpressionContext
    }


  sealed trait TypingContext

  case class VariableFreeTypeJudgement(pf: PartialFunction[(Expression, TypeTable), TypeConstraint]) extends TypeJudgement {
    override def isDefinedAt(x: (Expression, TypingContext, Bindings, TypeTable)): Boolean = pf.isDefinedAt((x._1, x._4))

    override def apply(v1: (Expression, TypingContext, Bindings, TypeTable)): TypeConstraint = pf.apply((v1._1, v1._4))
  }

  case object MatchContext extends TypingContext

  case object MergeContext extends TypingContext

  case object CreateContext extends TypingContext

  case object CreateUniqueContext extends TypingContext

  case object ExpressionContext extends TypingContext

}
