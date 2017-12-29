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

import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Types._
import org.neo4j.cypher.internal.util.v3_4.attribution.Attribute
import org.neo4j.cypher.internal.util.v3_4.symbols
import org.neo4j.cypher.internal.v3_4.expressions._

object Typing {

  def doIt(statement: Statement, bindings: VariableBinding): Typing = {
    val types = new Typing

    def specifyType(e: Expression, t: NewCypherType): Unit =
      types.set(e.secretId, ValidTypeConstraint(Set(t)))

    statement.treeVisitBottomUp {
      // Binary operators
      case a: Add =>
        val lhsTypes = types.get(a.lhs.secretId)
        val rhsTypes = types.get(a.rhs.secretId)

        val allPossible: Set[Either[NewCypherType, String]] = for {
          lhsTyp <- lhsTypes.possibleTypes
          rhsTyp <- rhsTypes.possibleTypes
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

        val typeConstraint = if (validTypes.isEmpty) {
          // There is no way this can succeed. To make it possible to continue typing the rest of the tree,
          // we simply make this expression be typed to any possible output for this expression
          val fallbackTypes: Set[NewCypherType] = Set(IntegerType, FloatType, StringType, ListType.ListOfUnknown)
          InvalidTypeConstraint(fallbackTypes, errors)
        } else {
          ValidTypeConstraint(validTypes, errors)
        }

        types.set(a.secretId, typeConstraint)


      // Literals
      case e: IntegerLiteral =>
        specifyType(e, IntegerType)
      case e: DoubleLiteral =>
        specifyType(e, FloatType)
      case x: StringLiteral =>
        specifyType(x, StringType)
      case x: Null =>
        specifyType(x, NullType)
      case x: BooleanLiteral =>
        specifyType(x, BoolType)
      case x: MapExpression =>
        specifyType(x, MapType(?))
      case x: Parameter =>
        // If the user is using the param explicitly, we don't have any type information. If this is the product of
        // autoparameterization, we do have the type
        if (x.parameterType == symbols.CTAny)
          types.set(x.secretId, ValidTypeConstraint(
            Set(IntegerType, FloatType, StringType, BoolType, MapType(?), NodeType, RelationshipType, PathType, PointType, GeometryType, ListType(?))))
        else
          specifyType(x, translateOldType(x.parameterType)) // TODO: Should translate the incoming type information
    }

    types
  }

  // List<L> + List<R> => List<L + R> where L and R are sets
  // unless L or R is the Set(?), in which case the resulting type is List<?>
  private def createGenericTypeFrom(a: ListType, b: ListType) =
    if (a == ListType.ListOfUnknown || b == ListType.ListOfUnknown)
      ListType.ListOfUnknown
    else
      ListType(a.possibleTypes ++ b.possibleTypes)

  private def createGenericTypeFrom(a: ListType, b: NewCypherType) =
    if (a == ListType.ListOfUnknown || b == ?)
      ListType.ListOfUnknown
    else
      ListType(a.possibleTypes + b)

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

trait TypeConstraint {
  def possibleTypes: Set[NewCypherType]

  def possibleErrors: Set[String]
}


// Set of types that this expression could have. This is used when we have at least one valid
// type - we can't fail here, and need to check types at runtime to make sure we are OK
case class ValidTypeConstraint(possibleTypes: Set[NewCypherType], possibleErrors: Set[String] = Set.empty) extends TypeConstraint {
  override def toString: String = ":" + possibleTypes.mkString(" | ")
}

// No valid types found for this expression. We want to finish typing of the rest of the tree before failing,
// so we store the errors here, but use the fallback types as the type for the expression when asked
case class InvalidTypeConstraint(fallbackTypes: Set[NewCypherType], possibleErrors: Set[String]) extends TypeConstraint {
  override def possibleTypes: Set[NewCypherType] = fallbackTypes
}

class Typing extends Attribute[TypeConstraint] {
  def getTypesFor(e: Expression): Set[NewCypherType] = get(e.secretId).possibleTypes
}

sealed trait TypingContext

object ExpressionContext extends TypingContext

object Types {

  sealed trait NewCypherType

  case class ListType(possibleTypes: Set[NewCypherType]) extends NewCypherType {
    override def toString: String = s"List[${possibleTypes.mkString(",")}]"
  }

  case class MapType(possibleTypes: Set[NewCypherType]) extends NewCypherType {
    override def toString: String = s"Map[${possibleTypes.mkString(",")}]"
  }

  case object IntegerType extends NewCypherType

  case object StringType extends NewCypherType

  case object FloatType extends NewCypherType

  case object BoolType extends NewCypherType

  case object NodeType extends NewCypherType

  case object RelationshipType extends NewCypherType

  case object PointType extends NewCypherType

  case object GeometryType extends NewCypherType

  case object PathType extends NewCypherType

  case object GraphRefType extends NewCypherType

  case object NullType extends NewCypherType

  case object ? extends NewCypherType

  object ListType {
    val ListOfUnknown = ListType(?)

    def apply(t: NewCypherType*): ListType = ListType(t.toSet)
  }

  object MapType {
    def apply(t: NewCypherType*): MapType = MapType(t.toSet)
  }

}