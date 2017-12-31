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

import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Types._
import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Typing._
import org.neo4j.cypher.internal.util.v3_4.{InternalException, symbols}
import org.neo4j.cypher.internal.v3_4.expressions._

object TypeJudgements {

  private val allJudgements =
    params orElse
      literals orElse
      add orElse
      variable

  def expressionTyper(e: Expression, ctx: TypingContext, bindings: Bindings, typeTable: TypeTable): TypeConstraint = {
    val tuple = (e, ctx, bindings, typeTable)
    if(!allJudgements.isDefinedAt(tuple))
      throw new InternalException(s"Failed to type $e")
    allJudgements.apply(tuple)
  }

  def params: TypeJudgement = VariableFreeTypeJudgement({
    case (x: Parameter, _) =>
      // If the user is using the param explicitly, we don't have any type information. If this is the product of
      // autoparameterization, we do have the type
      if (x.parameterType == symbols.CTAny)
        ValidTypeConstraint(Types.ANY)
      else
        valid(translateOldType(x.parameterType))

  })

  private def valid(t: NewCypherType*) = ValidTypeConstraint(t.toSet)

  def literals: TypeJudgement = VariableFreeTypeJudgement({
    case (_: IntegerLiteral, _) => valid(IntegerType)
    case (_: DoubleLiteral, _) => valid(FloatType)
    case (_: StringLiteral, _) => valid(StringType)
    case (_: Null, _) => valid(NullType)
    case (_: BooleanLiteral, _) => valid(BoolType)
    case (_: MapExpression, _) => valid(MapType(?))
    case (_@ListLiteral(expressions), types) =>
      val innerTypes = expressions.flatMap(e => types.get(e)).toSet
      valid(ListType(innerTypes))
  })

  def variable: TypeJudgement = {
    case (e: Variable, c: TypingContext, bindings: Bindings, types: TypeTable) =>
      bindings.variableBindings.get(e.secretId) match {
          // Sometimes variables are typed from the outside when walking down the tree. If this is the case,
          // just return this type
        case _:Declaration if types.contains(e.secretId) => types.get(e.secretId)

          // If we don't know anything, let's just say ANY for now. The containing clause will type us at a later stage
        case _:Declaration  => ValidTypeConstraint(Types.ANY)

          // If this is referencing a variable, we can simply look up the type of that variable
        case Resolution(id) => types.get(id)
      }
  }

  def add: TypeJudgement = VariableFreeTypeJudgement({
    case (a: Add, types: TypeTable) =>
      val lhsTypes = types.get(a.lhs)
      val rhsTypes = types.get(a.rhs)

      val allPossible: Set[Either[NewCypherType, String]] = for {
        lhsTyp <- lhsTypes
        rhsTyp <- rhsTypes
      } yield {
        (lhsTyp, rhsTyp) match {
          // 1 + 1 => 2
          // 1 + 1.1 => 2.1
          // 1.1 + 1 => 2.1
          // 1.1 + 1.1 => 2.2
          case (IntegerType, IntegerType) => Left(IntegerType)
          case (IntegerType, FloatType) => Left(FloatType)
          case (FloatType, IntegerType) => Left(FloatType)
          case (FloatType, FloatType) => Left(FloatType)

          // "a" + "b" => "ab"
          // "a" + 1 => "a1"
          // "a" + 1.1 => "a1.1"
          // 1 + "b" => "1b"
          // 1.1 + "b" => "1.1b"
          case (StringType, StringType) => Left(StringType)
          case (StringType, IntegerType) => Left(StringType)
          case (StringType, FloatType) => Left(StringType)
          case (IntegerType, StringType) => Left(StringType)
          case (FloatType, StringType) => Left(StringType)

          // [1] + ["a"] => [1,"a"]
          // [1] + "a" => [1,"a"]
          // 1 + ["a"] => [1,"a"]
          case (l: ListType, r: ListType) => Left(createGenericTypeFrom(l, r))
          case (l: ListType, r) => Left(createGenericTypeFrom(l, r))
          case (l, r: ListType) => Left(createGenericTypeFrom(r, l))

          case (l, r) => Right(s"Don't know how to + between $l and $r")
        }
      }

      val validTypes = allPossible.flatMap(_.left.toOption)
      val errors = allPossible.flatMap(_.right.toOption)

      if (validTypes.isEmpty) {
        // There is no way this can succeed. To make it possible to continue typing the rest of the tree,
        // we simply make this expression be typed to any possible output for this expression
        val fallbackTypes: Set[NewCypherType] = Set(IntegerType, FloatType, StringType, ListType.ListOfUnknown)
        InvalidTypeConstraint(fallbackTypes, errors)
      } else {
        ValidTypeConstraint(validTypes)
      }
  })

  // List<L> + List<R> => List<L + R> where L and R are sets
  // unless L or R is the Set(?), in which case the resulting type is List<?>
  private def createGenericTypeFrom(a: ListType, b: NewCypherType) =
    if (a == ListType.ListOfUnknown || b == ?)
      ListType.ListOfUnknown
    else
      ListType(a.inner + b)

  // TODO: Remove
  private def translateOldType(in: symbols.CypherType): NewCypherType = in match {
    case symbols.CTBoolean => BoolType
    case symbols.CTString => StringType
    case symbols.CTFloat => FloatType
    case symbols.CTInteger => IntegerType
    case symbols.CTMap => MapType(?)
    case symbols.CTNode => NodeType
    case symbols.CTRelationship => RelationshipType
    case symbols.CTPoint => PointType
    case symbols.CTGeometry => GeometryType
    case symbols.CTPath => PathType
    case symbols.ListType(inner) => ListType(translateOldType(inner))
    case _ => ?
  }

}
